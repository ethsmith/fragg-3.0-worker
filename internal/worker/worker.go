// Package worker orchestrates one cron run end-to-end:
//
//  1. Fetch every match in the configured CSC season.
//  2. Skip matches we have already ingested (by GET /player-stats/match/:id).
//  3. Download each remaining match's demo zip, extract every .dem inside it,
//     parse them with the eco-rating library, and POST the resulting
//     player-stats array to the stats API with ?upsert=true.
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
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
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
	Season            int          `json:"season"`
	MatchesFetched    int          `json:"matches_fetched"`
	MatchesEligible   int          `json:"matches_eligible"`
	MatchesTestFilter int          `json:"matches_test_filtered"`
	MatchesSkipped    int          `json:"matches_skipped"`
	MatchesProcessed  int          `json:"matches_processed"`
	DemosParsed       int          `json:"demos_parsed"`
	DemosFailed       int          `json:"demos_failed"`
	StatsDocsUpserted int          `json:"stats_docs_upserted"`
	DurationSeconds   float64      `json:"duration_seconds"`
	ProcessedMatchIDs []string     `json:"processed_match_ids,omitempty"`
	FailedDemos       []FailedDemo `json:"failed_demos,omitempty"`
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
	res.MatchesEligible = len(eligible)
	log.Printf("[worker] %d total matches, %d with demoUrl, %d eligible after test-filter",
		len(matches), len(completed), len(eligible))

	for _, m := range eligible {
		if res.MatchesProcessed >= cfg.MaxMatchesPerRun {
			log.Printf("[worker] reached MAX_MATCHES_PER_RUN=%d, stopping", cfg.MaxMatchesPerRun)
			break
		}
		if ctx.Err() != nil {
			log.Printf("[worker] context cancelled: %v", ctx.Err())
			break
		}

		// Cheap pre-check: if every expected map for this match is already in
		// the stats DB, skip the download entirely. Falls back to checking a
		// single key when CSC didn't populate stats[] yet.
		if alreadyIngested(ctx, statsClient, m) {
			res.MatchesSkipped++
			continue
		}

		log.Printf("[worker] processing match %s (demo=%s)", m.ID, m.DemoURL)
		processed, upserted, fails := processMatch(ctx, m, statsClient)
		res.DemosParsed += processed
		res.StatsDocsUpserted += upserted
		res.DemosFailed += len(fails)
		res.FailedDemos = append(res.FailedDemos, fails...)

		// We count a match as "processed" if we attempted at least one demo
		// from it, regardless of how many succeeded. This keeps the throttle
		// honest even when individual demos fail.
		if processed > 0 || len(fails) > 0 {
			res.MatchesProcessed++
			res.ProcessedMatchIDs = append(res.ProcessedMatchIDs, m.ID)
		}
	}

	res.DurationSeconds = time.Since(start).Seconds()
	log.Printf("[worker] done: processed=%d skipped=%d demos=%d failed=%d in %.2fs",
		res.MatchesProcessed, res.MatchesSkipped, res.DemosParsed, res.DemosFailed, res.DurationSeconds)
	return res, nil
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

// alreadyIngested returns true when every expected (match_id-mN) for a CSC
// match is already present in the stats DB.
func alreadyIngested(ctx context.Context, sc *stats.Client, m csc.Match) bool {
	expected := len(m.Stats)
	if expected <= 0 {
		expected = 1 // best-effort fallback when CSC stats[] is empty
	}
	for i := 1; i <= expected; i++ {
		mid := matchIDForIndex(m.ID, i)
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

// processMatch downloads a match's archive, parses every .dem inside, and
// upserts each demo's player-stats. Handles both .zip and .7z transparently
// (CSC is currently mid-migration from one to the other). Returns counts
// plus per-demo failures. It never returns an error itself: download
// failures are recorded as a single FailedDemo so the run can continue with
// the next match.
func processMatch(ctx context.Context, m csc.Match, sc *stats.Client) (parsed int, upserted int, fails []FailedDemo) {
	archivePath, cleanup, err := downloadToTemp(ctx, m.DemoURL)
	if err != nil {
		return 0, 0, []FailedDemo{{MatchID: m.ID, Demo: m.DemoURL, Error: "download: " + err.Error()}}
	}
	defer cleanup()

	arch, err := openArchive(archivePath)
	if err != nil {
		return 0, 0, []FailedDemo{{MatchID: m.ID, Demo: m.DemoURL, Error: "open archive: " + err.Error()}}
	}
	defer arch.Close()

	demos := arch.demoEntries()
	if len(demos) == 0 {
		return 0, 0, []FailedDemo{{MatchID: m.ID, Demo: m.DemoURL, Error: "no .dem files in archive"}}
	}

	for i, entry := range demos {
		idx := i + 1
		matchID := matchIDForIndex(m.ID, idx)

		players, err := parseDemoEntry(entry)
		if err != nil {
			log.Printf("[worker] parse %s/%s: %v", m.ID, entry.name, err)
			fails = append(fails, FailedDemo{MatchID: matchID, Demo: entry.name, Error: "parse: " + err.Error()})
			continue
		}

		docs := playersToSlice(players)
		if len(docs) == 0 {
			log.Printf("[worker] %s yielded zero players, skipping post", entry.name)
			fails = append(fails, FailedDemo{MatchID: matchID, Demo: entry.name, Error: "parser produced zero players"})
			continue
		}

		if err := sc.UpsertMatch(ctx, matchID, docs); err != nil {
			log.Printf("[worker] upsert %s: %v", matchID, err)
			fails = append(fails, FailedDemo{MatchID: matchID, Demo: entry.name, Error: "upsert: " + err.Error()})
			continue
		}

		log.Printf("[worker] upserted %s (%d players)", matchID, len(docs))
		parsed++
		upserted += len(docs)
	}
	return parsed, upserted, fails
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
func downloadToTemp(ctx context.Context, url string) (path string, cleanup func(), err error) {
	tmp, err := os.CreateTemp("", "csc-demo-*.bin")
	if err != nil {
		return "", func() {}, fmt.Errorf("create temp file: %w", err)
	}
	cleanup = func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("build download request: %w", err)
	}

	httpClient := &http.Client{Timeout: 5 * time.Minute}
	resp, err := httpClient.Do(req)
	if err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		cleanup()
		return "", func() {}, fmt.Errorf("download returned %d", resp.StatusCode)
	}

	if _, err := io.Copy(tmp, resp.Body); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("write zip to temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("sync temp zip: %w", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("rewind temp zip: %w", err)
	}
	return tmp.Name(), cleanup, nil
}
