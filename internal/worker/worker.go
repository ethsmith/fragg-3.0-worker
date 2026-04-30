// Package worker orchestrates one cron run end-to-end:
//
//  1. Fetch every match in the configured CSC season.
//  2. Group CSC rows by match ID. CSC sometimes returns a single row per
//     match with one archive containing all maps, and sometimes one row per
//     map with separate archives sharing the same match ID; both layouts
//     are flattened into one logical "match group" here.
//  3. Skip groups we have already ingested (by GET /player-stats/match/:id
//     for each expected -mN slot).
//  4. Download each remaining group's archive(s), extract every .dem inside,
//     parse them with the eco-rating library, and POST the resulting
//     player-stats array to the stats API with ?upsert=true. Maps are
//     numbered contiguously across all URLs in the group so re-runs always
//     resolve to the same -mN keys.
//
// The worker is bounded by Config.MaxMatchesPerRun so a single pass has a
// predictable upper bound on wall-clock time. Subsequent passes pick up the
// rest — the GET-skip pre-check makes resuming free.
package worker

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"parser-worker/internal/config"
	"parser-worker/internal/csc"
	"parser-worker/internal/stats"

	"github.com/bodgit/sevenzip"
	"github.com/ethsmith/eco-rating/model"
	"github.com/ethsmith/eco-rating/parser"
)

// Result is the JSON-serializable summary the cron handler returns.
type Result struct {
	Season            int `json:"season"`
	MatchesFetched    int `json:"matches_fetched"`
	MatchesEligible   int `json:"matches_eligible"`
	MatchesTestFilter int `json:"matches_test_filtered"`
	MatchesIgnored    int `json:"matches_ignored"`
	MatchesSkipped    int `json:"matches_skipped"`
	MatchesProcessed  int `json:"matches_processed"`
	DemosParsed       int `json:"demos_parsed"`
	DemosFailed       int `json:"demos_failed"`
	StatsDocsUpserted int `json:"stats_docs_upserted"`
	// RegulationProcessed is the number of regulation match groups
	// processed from CSC data (not bucket fallback).
	RegulationProcessed int `json:"regulation_processed"`
	// CombineProcessed is the number of combine match groups processed
	// from CSC data (not bucket fallback).
	CombineProcessed int `json:"combine_processed"`
	// BucketFallbackRan is true when the pass triggered the CDN bucket scan
	// because CSC yielded no new work. When true, the fields below describe
	// what the scan discovered and what the worker did with it. (When
	// false, they are zero.)
	BucketFallbackRan      bool         `json:"bucket_fallback_ran"`
	BucketDemosTotal       int          `json:"bucket_demos_total,omitempty"`
	BucketDemosNew         int          `json:"bucket_demos_new,omitempty"`
	BucketMatchesProcessed int          `json:"bucket_matches_processed,omitempty"`
	DurationSeconds        float64      `json:"duration_seconds"`
	ProcessedMatchIDs      []string     `json:"processed_match_ids,omitempty"`
	FailedDemos            []FailedDemo `json:"failed_demos,omitempty"`
}

// permanentDownloadError flags a download failure that will never succeed on
// retry (HTTP 4xx — typically 404 because the demo was never uploaded or has
// been removed). The Run loop uses errors.As against this type to decide
// whether to add the match to the persistent ignore list.
type permanentDownloadError struct {
	StatusCode int
	inner      error
}

func (e *permanentDownloadError) Error() string { return e.inner.Error() }
func (e *permanentDownloadError) Unwrap() error { return e.inner }

// enrichedPlayerStats wraps a parsed PlayerStats with the season and match
// type metadata that the stats API expects. The embedded *model.PlayerStats
// pointer means JSON marshaling promotes every player field to the top level
// alongside "season" and "type".
type enrichedPlayerStats struct {
	*model.PlayerStats
	Season int    `json:"season"`
	Type   string `json:"type"`
}

// enrichDocs converts a parsed player slice into JSON-ready docs with
// season and type injected. The returned slice can be passed directly to
// stats.Client.UpsertMatch (which accepts interface{}).
func enrichDocs(players []*model.PlayerStats, season int, matchType string) []enrichedPlayerStats {
	docs := make([]enrichedPlayerStats, len(players))
	for i, p := range players {
		docs[i] = enrichedPlayerStats{PlayerStats: p, Season: season, Type: matchType}
	}
	return docs
}

// FailedDemo describes one parse/post failure inside the run.
type FailedDemo struct {
	MatchID string `json:"match_id"`
	Demo    string `json:"demo"`
	Error   string `json:"error"`
}

// Run is the cron entry point. It is also called directly by the local
// CLI runner. The returned Result is always non-nil; err is set when the
// run aborted before processing all eligible matches (e.g., config or
// CSC fetch failures). Per-demo errors are reported in Result.FailedDemos
// without aborting the run.
func Run(ctx context.Context, cfg *config.Config) (*Result, error) {
	start := time.Now()
	res := &Result{Season: cfg.Season}

	cscClient := csc.NewClient(cfg.CSCGraphQLURL)
	statsClient := stats.NewClient(cfg.StatsAPIURL, cfg.StatsAPIKey)

	var ignore *ignoreList
	if cfg.IgnoreFile != "" {
		il, ilErr := loadIgnoreList(cfg.IgnoreFile)
		if ilErr != nil {
			log.Printf("[worker] could not load ignore list %s: %v (continuing with empty list)", cfg.IgnoreFile, ilErr)
			il = &ignoreList{path: cfg.IgnoreFile, set: make(map[string]struct{})}
		}
		ignore = il
		log.Printf("[worker] ignore list: %d entries loaded from %s", ignore.Len(), cfg.IgnoreFile)
	}

	log.Printf("[worker] fetching matches for season %d", cfg.Season)
	matches, err := cscClient.MatchesBySeason(ctx, cfg.Season)
	if err != nil {
		return res, fmt.Errorf("fetch matches: %w", err)
	}
	res.MatchesFetched = len(matches)

	// Keep only matches that have a demo to download. Process oldest-completed
	// first so we make forward progress chronologically across cron runs.
	completed := filterCompleted(matches)

	// Drop matches where either side is a test roster (TFR franchise, "Test
	// Franchise" / "Test Team" names, TestAwayTeam / TestHomeTeam, etc.) so
	// we never spend a cron tick parsing scrim-style filler data.
	eligible := make([]csc.Match, 0, len(completed))
	for _, m := range completed {
		if reason, skip := testMatchReason(m); skip {
			log.Printf("[worker] skipping match %s (test roster: %s)", m.ID, reason)
			res.MatchesTestFilter++
			continue
		}
		eligible = append(eligible, m)
	}

	sort.SliceStable(eligible, func(i, j int) bool {
		return eligible[i].CompletedAt < eligible[j].CompletedAt
	})

	// Bucket eligible rows by CSC match ID. A BO3 may show up as one row
	// (single archive containing all maps) OR as N rows with the same ID
	// and one archive each — both must produce the same -mN keys.
	//
	// Split into regulation and combine so each type gets its own
	// processing pass and its own bucket fallback check.
	regulationMatches := filterByMatchType(eligible, "REGULATION")
	combineMatches := filterByMatchType(eligible, "COMBINE")

	regulationGroups := groupMatches(regulationMatches)
	combineGroups := groupMatches(combineMatches)

	res.MatchesEligible = len(regulationGroups) + len(combineGroups)
	log.Printf("[worker] %d total matches, %d with demoUrl, %d eligible after test-filter, %d regulation groups, %d combine groups",
		len(matches), len(completed), len(eligible), len(regulationGroups), len(combineGroups))

	for _, g := range regulationGroups {
		if res.MatchesProcessed >= cfg.MaxMatchesPerRun {
			log.Printf("[worker] reached MAX_MATCHES_PER_RUN=%d, stopping", cfg.MaxMatchesPerRun)
			break
		}
		if ctx.Err() != nil {
			log.Printf("[worker] context cancelled: %v", ctx.Err())
			break
		}

		if ignore.Has(g.ID) {
			res.MatchesIgnored++
			continue
		}

		if alreadyIngested(ctx, statsClient, g) {
			res.MatchesSkipped++
			continue
		}

		log.Printf("[worker] processing regulation match %s (%d url(s))", g.ID, len(g.URLs))
		processed, upserted, fails := processMatchGroup(ctx, g, statsClient, ignore, cfg.Season, "regulation")
		res.DemosParsed += processed
		res.StatsDocsUpserted += upserted
		res.DemosFailed += len(fails)
		res.FailedDemos = append(res.FailedDemos, fails...)

		if processed > 0 || len(fails) > 0 {
			res.MatchesProcessed++
			res.RegulationProcessed++
			res.ProcessedMatchIDs = append(res.ProcessedMatchIDs, g.ID)
		}
	}

	// When the CSC regulation pass produced zero new work, fall back to
	// scanning the bucket directly for regulation demos.
	regBucketFallback := false
	if res.RegulationProcessed == 0 && ctx.Err() == nil {
		regBucketFallback = true
		runRegulationBucketFallback(ctx, cfg, statsClient, matches, res)
	}

	for _, g := range combineGroups {
		if res.MatchesProcessed >= cfg.MaxMatchesPerRun {
			log.Printf("[worker] reached MAX_MATCHES_PER_RUN=%d, stopping", cfg.MaxMatchesPerRun)
			break
		}
		if ctx.Err() != nil {
			log.Printf("[worker] context cancelled: %v", ctx.Err())
			break
		}

		if ignore.Has(g.ID) {
			res.MatchesIgnored++
			continue
		}

		if alreadyIngested(ctx, statsClient, g) {
			res.MatchesSkipped++
			continue
		}

		log.Printf("[worker] processing combine match %s (%d url(s))", g.ID, len(g.URLs))
		processed, upserted, fails := processMatchGroup(ctx, g, statsClient, ignore, cfg.Season, "combine")
		res.DemosParsed += processed
		res.StatsDocsUpserted += upserted
		res.DemosFailed += len(fails)
		res.FailedDemos = append(res.FailedDemos, fails...)

		if processed > 0 || len(fails) > 0 {
			res.MatchesProcessed++
			res.CombineProcessed++
			res.ProcessedMatchIDs = append(res.ProcessedMatchIDs, g.ID)
		}
	}

	// Same fallback pattern for combine: if CSC gave us nothing new, scan
	// the bucket for combine demos.
	if res.CombineProcessed == 0 && ctx.Err() == nil {
		if !regBucketFallback {
			res.BucketFallbackRan = true
		}
		runCombineBucketFallback(ctx, cfg, statsClient, matches, res)
	}

	res.DurationSeconds = time.Since(start).Seconds()
	log.Printf("[worker] done: processed=%d skipped=%d ignored=%d demos=%d failed=%d in %.2fs",
		res.MatchesProcessed, res.MatchesSkipped, res.MatchesIgnored, res.DemosParsed, res.DemosFailed, res.DurationSeconds)

	// Surface CSC rows where stats[] reports more maps than there are demo
	// archives. Almost always means a BO3 only had its first map's demo
	// uploaded — those later -mN slots will never ingest until someone
	// uploads the missing demos, so we just flag it once per pass.
	partial := 0
	allGroups := make([]matchGroup, 0, len(regulationGroups)+len(combineGroups))
	allGroups = append(allGroups, regulationGroups...)
	allGroups = append(allGroups, combineGroups...)
	for _, g := range allGroups {
		if len(g.Match.Stats) > len(g.URLs) {
			partial++
		}
	}
	if partial > 0 {
		log.Printf("[worker] note: %d match group(s) have CSC stats[] longer than archive URL count "+
			"(likely partial demo uploads; missing maps will not ingest until more demos appear)",
			partial)
	}

	return res, nil
}

// runRegulationBucketFallback scans the CDN bucket for regulation-format demos that
// weren't present in the CSC API response, then ingests any new ones. It's
// the escape hatch for "CSC hasn't indexed this match yet but the demo is
// already up on the CDN" — which happens regularly early in a new match
// week.
//
// Dedup against CSC is by (matchID, mapIndex) — i.e. the same -m<N> key the
// stats DB uses. Two demos identifying the same map of the same match are
// considered the same demo regardless of any other filename component.
// Notably this excludes the trailing timestamp: when a match is
// rescheduled and re-uploaded, CSC's demoUrl and the bucket key may carry
// different timestamps for the same logical demo, and we must not let
// that cause a double-parse.
//
// Each bucket demo is routed to a specific -m<N> slot derived from the
// filename's map index ("...-mid<id>-<idx>_<mapName>..."), NOT from
// sort-position within the bucket group. That way, if CSC already has
// -m1 for match X and the bucket has only a later map (-m2, -m3, ...),
// we correctly target the new slot instead of overwriting -m1.
//
// NOTE: this path deliberately ignores the on-disk ignore list. That list
// is keyed by CSC match ID and tracks "CSC's demoUrl for match X returned
// 4xx" — which says nothing about a *different* bucket URL for another
// map of the same match. We also don't add bucket-source download failures
// to the list, for the same reason.
func runRegulationBucketFallback(
	ctx context.Context,
	cfg *config.Config,
	sc *stats.Client,
	cscMatches []csc.Match,
	res *Result,
) {
	res.BucketFallbackRan = true
	log.Printf("[worker] CSC pass produced 0 new matches; scanning bucket s%d/ for regulation demos", cfg.Season)

	// known holds the set of -m<N> keys (e.g. "8275-m1") CSC announced
	// this pass. Building it from parsed (matchID, mapIndex) pairs rather
	// than raw filenames makes the dedup tolerant to reschedules — the
	// timestamp portion of a demo filename can change without changing
	// what map of what match the file represents.
	known := make(map[string]struct{}, len(cscMatches))
	for _, m := range cscMatches {
		if m.DemoURL == "" {
			continue
		}
		mid, idx, ok := parseRegulationFilename(path.Base(m.DemoURL))
		if !ok {
			continue
		}
		known[matchIDForIndex(mid, idx+1)] = struct{}{}
	}

	demos, err := listRegulationDemos(ctx, cfg.Season)
	if err != nil {
		log.Printf("[worker] bucket scan failed: %v", err)
		return
	}
	res.BucketDemosTotal = len(demos)

	// Group candidates by match ID, dropping any whose -m<N> slot was also
	// announced by CSC this pass.
	byID := make(map[string][]bucketDemo)
	var order []string
	for _, d := range demos {
		if _, seen := known[matchIDForIndex(d.MatchID, d.MapIndex+1)]; seen {
			continue
		}
		if _, ok := byID[d.MatchID]; !ok {
			order = append(order, d.MatchID)
		}
		byID[d.MatchID] = append(byID[d.MatchID], d)
	}
	var newDemoCount int
	for _, ds := range byID {
		newDemoCount += len(ds)
	}
	res.BucketDemosNew = newDemoCount
	log.Printf("[worker] bucket scan: %d regulation demos total, %d not in CSC data (across %d unique match IDs)",
		len(demos), newDemoCount, len(byID))

	for _, matchID := range order {
		if res.MatchesProcessed >= cfg.MaxMatchesPerRun {
			log.Printf("[worker] reached MAX_MATCHES_PER_RUN=%d during bucket fallback, stopping", cfg.MaxMatchesPerRun)
			break
		}
		if ctx.Err() != nil {
			log.Printf("[worker] context cancelled during bucket fallback: %v", ctx.Err())
			break
		}
		maps := byID[matchID]
		sort.Slice(maps, func(i, j int) bool { return maps[i].MapIndex < maps[j].MapIndex })

		// Per-demo pre-skip: drop any whose target -m<N> is already in the
		// stats DB. Skipping here (instead of after download) means a match
		// whose only "new" bucket demo turned out to be a re-announce of an
		// already-ingested map costs us zero bandwidth.
		toProcess := make([]bucketDemo, 0, len(maps))
		for _, d := range maps {
			target := matchIDForIndex(d.MatchID, d.MapIndex+1)
			exists, herr := sc.HasMatch(ctx, target)
			if herr != nil {
				log.Printf("[worker] bucket HasMatch(%s) failed, will re-ingest: %v", target, herr)
				toProcess = append(toProcess, d)
				continue
			}
			if !exists {
				toProcess = append(toProcess, d)
			}
		}
		if len(toProcess) == 0 {
			res.MatchesSkipped++
			continue
		}

		log.Printf("[worker] processing bucket match %s (%d demo(s))", matchID, len(toProcess))
		anyActivity := false
		for _, d := range toProcess {
			target := matchIDForIndex(d.MatchID, d.MapIndex+1)
			parsed, upserted, fails := processSingleDemoToTarget(ctx, target, d.URL, sc, cfg.Season, "regulation")
			res.DemosParsed += parsed
			res.StatsDocsUpserted += upserted
			res.DemosFailed += len(fails)
			res.FailedDemos = append(res.FailedDemos, fails...)
			if parsed > 0 || len(fails) > 0 {
				anyActivity = true
			}
		}
		if anyActivity {
			res.MatchesProcessed++
			res.BucketMatchesProcessed++
			res.ProcessedMatchIDs = append(res.ProcessedMatchIDs, matchID)
		}
	}
}

// runCombineBucketFallback is the combine equivalent of
// runRegulationBucketFallback. It scans the bucket under
// s<season>/Combines/ for combine-format demos not present in the CSC
// API response, then ingests any new ones with type "combine".
//
// Dedup against CSC is by (matchID, mapIndex), matching the same -m<N>
// key scheme used for regulation demos.
func runCombineBucketFallback(
	ctx context.Context,
	cfg *config.Config,
	sc *stats.Client,
	cscMatches []csc.Match,
	res *Result,
) {
	log.Printf("[worker] CSC pass produced 0 new combine matches; scanning bucket s%d/Combines/ for combine demos", cfg.Season)

	known := make(map[string]struct{}, len(cscMatches))
	for _, m := range cscMatches {
		if m.DemoURL == "" {
			continue
		}
		mid, idx, ok := parseCombineFilename(path.Base(m.DemoURL))
		if !ok {
			continue
		}
		known[matchIDForIndex(mid, idx+1)] = struct{}{}
	}

	demos, err := listCombineDemos(ctx, cfg.Season)
	if err != nil {
		log.Printf("[worker] combine bucket scan failed: %v", err)
		return
	}
	res.BucketDemosTotal += len(demos)

	byID := make(map[string][]bucketDemo)
	var order []string
	for _, d := range demos {
		if _, seen := known[matchIDForIndex(d.MatchID, d.MapIndex+1)]; seen {
			continue
		}
		if _, ok := byID[d.MatchID]; !ok {
			order = append(order, d.MatchID)
		}
		byID[d.MatchID] = append(byID[d.MatchID], d)
	}
	var newDemoCount int
	for _, ds := range byID {
		newDemoCount += len(ds)
	}
	res.BucketDemosNew += newDemoCount
	log.Printf("[worker] combine bucket scan: %d demos total, %d not in CSC data (across %d unique match IDs)",
		len(demos), newDemoCount, len(byID))

	for _, matchID := range order {
		if res.MatchesProcessed >= cfg.MaxMatchesPerRun {
			log.Printf("[worker] reached MAX_MATCHES_PER_RUN=%d during combine bucket fallback, stopping", cfg.MaxMatchesPerRun)
			break
		}
		if ctx.Err() != nil {
			log.Printf("[worker] context cancelled during combine bucket fallback: %v", ctx.Err())
			break
		}
		maps := byID[matchID]
		sort.Slice(maps, func(i, j int) bool { return maps[i].MapIndex < maps[j].MapIndex })

		toProcess := make([]bucketDemo, 0, len(maps))
		for _, d := range maps {
			target := matchIDForIndex(d.MatchID, d.MapIndex+1)
			exists, herr := sc.HasMatch(ctx, target)
			if herr != nil {
				log.Printf("[worker] combine bucket HasMatch(%s) failed, will re-ingest: %v", target, herr)
				toProcess = append(toProcess, d)
				continue
			}
			if !exists {
				toProcess = append(toProcess, d)
			}
		}
		if len(toProcess) == 0 {
			res.MatchesSkipped++
			continue
		}

		log.Printf("[worker] processing combine bucket match %s (%d demo(s))", matchID, len(toProcess))
		anyActivity := false
		for _, d := range toProcess {
			target := matchIDForIndex(d.MatchID, d.MapIndex+1)
			parsed, upserted, fails := processSingleDemoToTarget(ctx, target, d.URL, sc, cfg.Season, "combine")
			res.DemosParsed += parsed
			res.StatsDocsUpserted += upserted
			res.DemosFailed += len(fails)
			res.FailedDemos = append(res.FailedDemos, fails...)
			if parsed > 0 || len(fails) > 0 {
				anyActivity = true
			}
		}
		if anyActivity {
			res.MatchesProcessed++
			res.BucketMatchesProcessed++
			res.ProcessedMatchIDs = append(res.ProcessedMatchIDs, matchID)
		}
	}
}

// processSingleDemoToTarget downloads one bucket demo, parses the single
// .dem inside, and upserts its player-stats to the given targetMatchID.
// Unlike processArchiveURL (which assigns -mN based on sort order), the
// target here is fixed by the caller — derived from the filename's map
// index — so bucket ingestion always lands in the right slot even when
// CSC already owns other maps of the same series.
//
// Intentionally does not touch the ignore list (see runBucketFallback for
// rationale): a bad bucket URL costs one failed GET per pass to retry,
// which is negligible compared to the risk of incorrectly suppressing
// other (valid) maps of the same match.
func processSingleDemoToTarget(
	ctx context.Context,
	targetMatchID, demoURL string,
	sc *stats.Client,
	season int,
	matchType string,
) (parsed, upserted int, fails []FailedDemo) {
	archivePath, size, cleanup, err := downloadToTemp(ctx, demoURL)
	if err != nil {
		log.Printf("[worker] download %s (%s) failed: %v", targetMatchID, demoURL, err)
		fails = append(fails, FailedDemo{MatchID: targetMatchID, Demo: demoURL, Error: "download: " + err.Error()})
		return
	}
	defer cleanup()
	log.Printf("[worker] downloaded %s (%d bytes) from %s", targetMatchID, size, demoURL)

	arch, err := openArchive(archivePath)
	if err != nil {
		log.Printf("[worker] open archive %s failed: %v", targetMatchID, err)
		fails = append(fails, FailedDemo{MatchID: targetMatchID, Demo: demoURL, Error: "open archive: " + err.Error()})
		return
	}
	defer arch.Close()

	entries := arch.demoEntries()
	if len(entries) == 0 {
		log.Printf("[worker] archive %s has no .dem entries", targetMatchID)
		fails = append(fails, FailedDemo{MatchID: targetMatchID, Demo: demoURL, Error: "no .dem files in archive"})
		return
	}
	if len(entries) > 1 {
		log.Printf("[worker] archive %s unexpectedly contains %d demos; bucket fallback only upserts the first (%s)",
			targetMatchID, len(entries), entries[0].name)
	}
	entry := entries[0]

	players, err := parseDemoEntry(entry)
	if err != nil {
		log.Printf("[worker] parse %s/%s: %v", targetMatchID, entry.name, err)
		fails = append(fails, FailedDemo{MatchID: targetMatchID, Demo: entry.name, Error: "parse: " + err.Error()})
		return
	}
	docs := playersToSlice(players)
	if len(docs) == 0 {
		log.Printf("[worker] %s yielded zero players, skipping post", entry.name)
		fails = append(fails, FailedDemo{MatchID: targetMatchID, Demo: entry.name, Error: "parser produced zero players"})
		return
	}
	if err := sc.UpsertMatch(ctx, targetMatchID, enrichDocs(docs, season, matchType)); err != nil {
		log.Printf("[worker] upsert %s: %v", targetMatchID, err)
		fails = append(fails, FailedDemo{MatchID: targetMatchID, Demo: entry.name, Error: "upsert: " + err.Error()})
		return
	}
	log.Printf("[worker] upserted %s (%d players)", targetMatchID, len(docs))
	parsed = 1
	upserted = len(docs)
	return
}

func filterCompleted(in []csc.Match) []csc.Match {
	out := make([]csc.Match, 0, len(in))
	for _, m := range in {
		if strings.TrimSpace(m.DemoURL) != "" {
			out = append(out, m)
		}
	}
	return out
}

// filterByMatchType returns the subset of matches whose demo URL filename
// matches the expected type pattern: "regulation" matches the regulation
// regex, "combine" matches the combine regex. The matchType argument must
// be "regulation" or "combine".
func filterByMatchType(in []csc.Match, matchType string) []csc.Match {
	out := make([]csc.Match, 0, len(in))
	for _, m := range in {
		if demosMatchType(m, matchType) {
			out = append(out, m)
		}
	}
	return out
}

// demosMatchType returns true when the match's demoURL filename matches the
// expected format for the given matchType.
func demosMatchType(m csc.Match, matchType string) bool {
	fn := path.Base(strings.TrimSpace(m.DemoURL))
	switch matchType {
	case "regulation":
		_, _, ok := parseRegulationFilename(fn)
		return ok
	case "combine":
		_, _, ok := parseCombineFilename(fn)
		return ok
	default:
		return false
	}
}

// testFranchisePrefixes are franchise short-tags that exclusively belong to
// internal/test rosters. Match check is case-insensitive exact.
var testFranchisePrefixes = []string{"TFR"}

// testMatchReason returns a non-empty reason string and true when either side
// of the match looks like a CSC test roster (test franchise, "Test Team",
// "TestHomeTeam", etc.). The check covers:
//
//   - franchise.prefix exactly matches a known test prefix (e.g. TFR)
//   - franchise.name or team.name has a "test" prefix (case-insensitive),
//     which catches "Test Franchise", "Test Team", "TestHomeTeam",
//     "TestAwayTeam", and any future "Test*" naming
func testMatchReason(m csc.Match) (string, bool) {
	if r := testTeamReason("home", m.Home); r != "" {
		return r, true
	}
	if r := testTeamReason("away", m.Away); r != "" {
		return r, true
	}
	return "", false
}

func testTeamReason(side string, t csc.Team) string {
	for _, p := range testFranchisePrefixes {
		if strings.EqualFold(strings.TrimSpace(t.Franchise.Prefix), p) {
			return fmt.Sprintf("%s.franchise.prefix=%q", side, t.Franchise.Prefix)
		}
	}
	if hasTestPrefix(t.Franchise.Name) {
		return fmt.Sprintf("%s.franchise.name=%q", side, t.Franchise.Name)
	}
	if hasTestPrefix(t.Name) {
		return fmt.Sprintf("%s.name=%q", side, t.Name)
	}
	return ""
}

// hasTestPrefix reports whether s starts with "test" (case-insensitive),
// ignoring leading whitespace. Catches both "Test Franchise" / "Test Team"
// (with separator) and "TestHomeTeam" / "TestAwayTeam" (no separator).
func hasTestPrefix(s string) bool {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) < 4 {
		return false
	}
	return strings.EqualFold(trimmed[:4], "test")
}

// matchGroup is one logical CSC match — a single ID — along with every demo
// archive URL that contributes to it. Grouping rows by ID lets the worker
// treat both BO3 layouts identically:
//
//   - one row, one archive containing every .dem (URLs has length 1, the
//     archive is unpacked into N maps)
//   - N rows sharing the same ID, one archive per map (URLs has length N,
//     each archive holds one .dem)
//
// In either case the maps are numbered contiguously — -m1, -m2, ... -mN —
// so re-runs always resolve to the same stats keys regardless of layout.
type matchGroup struct {
	ID   string
	URLs []string // sorted lexicographically; the URL filename embeds the
	// map number, so this also sorts maps in play order.
	Match csc.Match // representative metadata (Home/Away/CompletedAt). Any
	// row in the group works — they all share an ID.
}

// groupMatches buckets matches by ID, preserving first-seen order so the
// chronological pre-sort in Run still drives the iteration order. URLs
// within a bucket are sorted lexicographically because CSC's filename scheme
// (".../mid<id>-<mapNumber>_<mapName>...") puts the map number right after
// the ID, so a string sort yields play order.
func groupMatches(in []csc.Match) []matchGroup {
	index := make(map[string]int, len(in))
	out := make([]matchGroup, 0, len(in))
	for _, m := range in {
		if i, ok := index[m.ID]; ok {
			out[i].URLs = append(out[i].URLs, m.DemoURL)
			continue
		}
		index[m.ID] = len(out)
		out = append(out, matchGroup{ID: m.ID, URLs: []string{m.DemoURL}, Match: m})
	}
	for i := range out {
		sort.Strings(out[i].URLs)
	}
	return out
}

// expectedMaps is the number of -mN slots we'd expect to see in the stats DB
// for a fully-ingested match group. We trust the URL count over CSC's
// stats[] array because stats[] sometimes overcounts (it can list a planned
// third map of a 2-0 BO3 that was never played, for example), which would
// cause alreadyIngested to never return true and the worker would reprocess
// the same archive every pass forever.
func (g matchGroup) expectedMaps() int {
	if n := len(g.URLs); n > 0 {
		return n
	}
	return 1
}

// alreadyIngested returns true when every expected -mN for the group is
// already present in the stats DB. Multi-archive groups skip cleanly even
// if CSC's stats[] is wrong/empty: the URL count is the source of truth.
func alreadyIngested(ctx context.Context, sc *stats.Client, g matchGroup) bool {
	expected := g.expectedMaps()
	for i := 1; i <= expected; i++ {
		mid := matchIDForIndex(g.ID, i)
		exists, err := sc.HasMatch(ctx, mid)
		if err != nil {
			// On transient lookup errors we choose to *not* skip — better to
			// re-upsert (idempotent) than silently miss a match.
			log.Printf("[worker] HasMatch(%s) failed, will reprocess: %v", mid, err)
			return false
		}
		if !exists {
			return false
		}
	}
	return true
}

// matchIDForIndex builds the deterministic match_id used in the stats DB for
// the i-th demo (1-indexed) of a CSC match. The "-m<idx>" suffix guarantees
// re-runs of the same zip resolve to the same key.
func matchIDForIndex(cscMatchID string, idx int) string {
	return fmt.Sprintf("%s-m%d", cscMatchID, idx)
}

// processMatchGroup walks every URL in the group in sorted order, downloads
// each archive, parses every .dem inside, and upserts player-stats. The map
// index increments across URL boundaries so a BO3 split as three single-map
// archives produces -m1/-m2/-m3 deterministically.
//
// It never returns an error itself: per-URL and per-demo failures are
// recorded in FailedDemos so the run can continue with the next group.
func processMatchGroup(ctx context.Context, g matchGroup, sc *stats.Client, ignore *ignoreList, season int, matchType string) (parsed int, upserted int, fails []FailedDemo) {
	idx := 0
	for _, url := range g.URLs {
		nextIdx, p, u, f := processArchiveURL(ctx, g.ID, url, sc, ignore, idx, season, matchType)
		idx = nextIdx
		parsed += p
		upserted += u
		fails = append(fails, f...)
	}
	return parsed, upserted, fails
}

// processArchiveURL handles one archive URL inside a group. startIdx is the
// last assigned -mN (0 for the first URL in the group); the returned nextIdx
// is startIdx + (number of demos found in this archive), even if some failed
// to parse — so subsequent URLs always pick up where this one left off and
// per-map keys remain stable across re-runs.
func processArchiveURL(
	ctx context.Context,
	groupID, url string,
	sc *stats.Client,
	ignore *ignoreList,
	startIdx int,
	season int,
	matchType string,
) (nextIdx, parsed, upserted int, fails []FailedDemo) {
	nextIdx = startIdx

	archivePath, size, cleanup, err := downloadToTemp(ctx, url)
	if err != nil {
		log.Printf("[worker] download %s (%s) failed: %v", groupID, url, err)
		var perm *permanentDownloadError
		if errors.As(err, &perm) {
			reason := fmt.Sprintf("download_%d", perm.StatusCode)
			if addErr := ignore.Add(groupID, reason, url); addErr != nil {
				log.Printf("[worker] could not add %s to ignore list: %v", groupID, addErr)
			} else {
				log.Printf("[worker] added %s to ignore list (HTTP %d)", groupID, perm.StatusCode)
			}
		}
		fails = append(fails, FailedDemo{MatchID: groupID, Demo: url, Error: "download: " + err.Error()})
		return
	}
	defer cleanup()
	log.Printf("[worker] downloaded %s (%d bytes) from %s", groupID, size, url)

	arch, err := openArchive(archivePath)
	if err != nil {
		log.Printf("[worker] open archive %s failed: %v", groupID, err)
		fails = append(fails, FailedDemo{MatchID: groupID, Demo: url, Error: "open archive: " + err.Error()})
		return
	}
	defer arch.Close()

	demos := arch.demoEntries()
	if len(demos) == 0 {
		log.Printf("[worker] archive %s has no .dem entries (total entries=%d)", groupID, len(arch.entries))
		fails = append(fails, FailedDemo{MatchID: groupID, Demo: url, Error: "no .dem files in archive"})
		return
	}
	log.Printf("[worker] archive %s has %d demo(s)", groupID, len(demos))

	for _, entry := range demos {
		nextIdx++
		matchID := matchIDForIndex(groupID, nextIdx)

		players, err := parseDemoEntry(entry)
		if err != nil {
			log.Printf("[worker] parse %s/%s: %v", groupID, entry.name, err)
			fails = append(fails, FailedDemo{MatchID: matchID, Demo: entry.name, Error: "parse: " + err.Error()})
			continue
		}

		docs := playersToSlice(players)
		if len(docs) == 0 {
			log.Printf("[worker] %s yielded zero players, skipping post", entry.name)
			fails = append(fails, FailedDemo{MatchID: matchID, Demo: entry.name, Error: "parser produced zero players"})
			continue
		}

		if err := sc.UpsertMatch(ctx, matchID, enrichDocs(docs, season, matchType)); err != nil {
			log.Printf("[worker] upsert %s: %v", matchID, err)
			fails = append(fails, FailedDemo{MatchID: matchID, Demo: entry.name, Error: "upsert: " + err.Error()})
			continue
		}

		log.Printf("[worker] upserted %s (%d players)", matchID, len(docs))
		parsed++
		upserted += len(docs)
	}
	return
}

// archive is a tiny abstraction over archive/zip and bodgit/sevenzip so
// processMatch doesn't care which format the demo URL came in. Both
// libraries expose the same shape (slice of files, each with Name +
// FileInfo + Open), so this wrapper is mechanical.
type archive struct {
	closer  io.Closer
	entries []demoEntry
}

// demoEntry is one .dem file inside an archive. Open() returns a fresh
// reader each call (the underlying lib re-decompresses on each open for
// 7z; zip is essentially free).
type demoEntry struct {
	name string
	open func() (io.ReadCloser, error)
}

func (a *archive) Close() error { return a.closer.Close() }

// demoEntries returns every .dem file in the archive, sorted by base
// filename (case-insensitive). The sort makes the "-mN" suffix assignment
// in matchIDForIndex deterministic across re-runs of the same archive.
func (a *archive) demoEntries() []demoEntry {
	out := make([]demoEntry, 0, len(a.entries))
	for _, e := range a.entries {
		if strings.HasSuffix(strings.ToLower(e.name), ".dem") {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return strings.ToLower(filepath.Base(out[i].name)) <
			strings.ToLower(filepath.Base(out[j].name))
	})
	return out
}

// openArchive sniffs the file's magic bytes (more reliable than the URL
// extension) and dispatches to the right reader.
func openArchive(path string) (*archive, error) {
	format, err := detectArchiveFormat(path)
	if err != nil {
		return nil, err
	}
	switch format {
	case "zip":
		return openZipArchive(path)
	case "7z":
		return openSevenZipArchive(path)
	default:
		return nil, fmt.Errorf("unsupported archive format %q", format)
	}
}

// detectArchiveFormat reads the first 6 bytes of path and returns "zip",
// "7z", or an error. We sniff bytes rather than trust the URL extension so
// a misnamed file still parses correctly.
func detectArchiveFormat(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open for sniff: %w", err)
	}
	defer f.Close()

	var head [6]byte
	if _, err := io.ReadFull(f, head[:]); err != nil {
		return "", fmt.Errorf("read magic bytes: %w", err)
	}

	// PK\x03\x04 = standard zip; PK\x05\x06 = empty zip; PK\x07\x08 = spanned.
	if bytes.HasPrefix(head[:], []byte{'P', 'K', 0x03, 0x04}) ||
		bytes.HasPrefix(head[:], []byte{'P', 'K', 0x05, 0x06}) ||
		bytes.HasPrefix(head[:], []byte{'P', 'K', 0x07, 0x08}) {
		return "zip", nil
	}

	// 7z = "7z\xBC\xAF\x27\x1C" (six-byte signature).
	if bytes.Equal(head[:], []byte{'7', 'z', 0xBC, 0xAF, 0x27, 0x1C}) {
		return "7z", nil
	}

	return "", fmt.Errorf("unknown archive magic: % x", head)
}

func openZipArchive(path string) (*archive, error) {
	zr, err := zip.OpenReader(path)
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}
	entries := make([]demoEntry, 0, len(zr.File))
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		f := f // capture for closure
		entries = append(entries, demoEntry{
			name: f.Name,
			open: func() (io.ReadCloser, error) { return f.Open() },
		})
	}
	return &archive{closer: zr, entries: entries}, nil
}

func openSevenZipArchive(path string) (*archive, error) {
	zr, err := sevenzip.OpenReader(path)
	if err != nil {
		return nil, fmt.Errorf("open 7z: %w", err)
	}
	entries := make([]demoEntry, 0, len(zr.File))
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		f := f // capture for closure
		entries = append(entries, demoEntry{
			name: f.Name,
			open: func() (io.ReadCloser, error) { return f.Open() },
		})
	}
	return &archive{closer: zr, entries: entries}, nil
}

// parseDemoEntry streams a single .dem out of the archive and runs the
// eco-rating parser against it. Streaming avoids a second copy onto disk
// even for 7z, where bodgit/sevenzip decompresses on-the-fly.
func parseDemoEntry(e demoEntry) (map[uint64]*model.PlayerStats, error) {
	rc, err := e.open()
	if err != nil {
		return nil, fmt.Errorf("open archive entry: %w", err)
	}
	defer rc.Close()

	br := bufio.NewReaderSize(rc, 1<<20) // 1 MiB matches the eco-rating CLI

	p := parser.NewDemoParser(br)
	if err := p.Parse(); err != nil {
		return nil, fmt.Errorf("parse demo: %w", err)
	}
	return p.GetPlayers(), nil
}

// playersToSlice converts the parser's keyed map into a JSON-marshalable
// slice for the stats API. SteamID is already populated as a string field on
// each PlayerStats, so the map key (uint64) can be discarded.
func playersToSlice(in map[uint64]*model.PlayerStats) []*model.PlayerStats {
	out := make([]*model.PlayerStats, 0, len(in))
	for _, p := range in {
		if p == nil {
			continue
		}
		out = append(out, p)
	}
	// Stable order so identical inputs serialize identically.
	sort.Slice(out, func(i, j int) bool { return out[i].SteamID < out[j].SteamID })
	return out
}

// downloadToTemp streams a URL to a temp file. We use disk rather than
// memory because CSC demo archives can run several hundred MB and we want
// random-access (zip.OpenReader / sevenzip.OpenReader) without a full
// in-memory copy.
func downloadToTemp(ctx context.Context, url string) (path string, size int64, cleanup func(), err error) {
	tmp, err := os.CreateTemp("", "csc-demo-*.bin")
	if err != nil {
		return "", 0, func() {}, fmt.Errorf("create temp file: %w", err)
	}
	cleanup = func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cleanup()
		return "", 0, func() {}, fmt.Errorf("build download request: %w", err)
	}

	httpClient := &http.Client{Timeout: 5 * time.Minute}
	resp, err := httpClient.Do(req)
	if err != nil {
		cleanup()
		return "", 0, func() {}, fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		cleanup()
		base := fmt.Errorf("download returned %d (content-type=%q)", resp.StatusCode, resp.Header.Get("Content-Type"))
		// 4xx is permanent: the demo URL doesn't exist (404), is forbidden
		// (403/401 — DigitalOcean Spaces returns these for missing objects in
		// some bucket configs), or the request itself is malformed (400/410).
		// Any retry would just hit the same response, so flag it for the
		// caller to add to the persistent ignore list. 5xx and network
		// errors fall through as plain errors and remain retried each pass.
		if resp.StatusCode/100 == 4 {
			return "", 0, func() {}, &permanentDownloadError{StatusCode: resp.StatusCode, inner: base}
		}
		return "", 0, func() {}, base
	}

	n, err := io.Copy(tmp, resp.Body)
	if err != nil {
		cleanup()
		return "", 0, func() {}, fmt.Errorf("write archive to temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return "", 0, func() {}, fmt.Errorf("sync temp archive: %w", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return "", 0, func() {}, fmt.Errorf("rewind temp archive: %w", err)
	}
	return tmp.Name(), n, cleanup, nil
}
