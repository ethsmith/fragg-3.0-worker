//go:build integration

// Integration test that exercises the live CSC + fragg-3.0 stats API for a
// single CSC match ID. Used to diagnose "why is this match reprocessing
// every pass?" — it prints the row(s) CSC returns, what alreadyIngested
// sees in the stats DB, and what the archive(s) actually contain.
//
// Run with:
//
//	go test -tags=integration ./internal/worker/ -run TestDebugMatch -v
//
// Required env: SEASON, STATS_API_URL, STATS_API_KEY (loaded from
// ./.env). MATCH_ID overrides the default match (8275).
package worker

import (
	"bufio"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"parser-worker/internal/config"
	"parser-worker/internal/csc"
	"parser-worker/internal/stats"
)

func TestDebugMatch(t *testing.T) {
	loadDotEnvForTest(t, "../../.env")

	matchID := os.Getenv("MATCH_ID")
	if matchID == "" {
		matchID = "8275"
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cscClient := csc.NewClient(cfg.CSCGraphQLURL)
	statsClient := stats.NewClient(cfg.StatsAPIURL, cfg.StatsAPIKey)

	// 1. What does CSC return for this match? Multiple rows = multi-archive
	//    BO3; single row = either BO1 or one archive containing every map.
	t.Logf("step 1: fetching season %d from CSC", cfg.Season)
	all, err := cscClient.MatchesBySeason(ctx, cfg.Season)
	if err != nil {
		t.Fatalf("CSC fetch: %v", err)
	}
	t.Logf("  CSC returned %d total matches in season %d", len(all), cfg.Season)

	var rows []csc.Match
	for _, m := range all {
		if m.ID == matchID {
			rows = append(rows, m)
		}
	}
	if len(rows) == 0 {
		t.Fatalf("match %s not found in season %d", matchID, cfg.Season)
	}
	t.Logf("  found %d row(s) with ID=%s:", len(rows), matchID)
	for i, r := range rows {
		t.Logf("    row[%d] demoUrl=%s", i, r.DemoURL)
		t.Logf("           home=%q (franchise=%q/%q) away=%q (franchise=%q/%q)",
			r.Home.Name, r.Home.Franchise.Prefix, r.Home.Franchise.Name,
			r.Away.Name, r.Away.Franchise.Prefix, r.Away.Franchise.Name)
		t.Logf("           len(Stats)=%d", len(r.Stats))
		for j, s := range r.Stats {
			t.Logf("             stats[%d] mapNumber=%d mapName=%q", j, s.MapNumber, s.MapName)
		}
	}

	// 2. Group and report what the worker would compute.
	g := groupMatches(rows)[0]
	t.Logf("step 2: grouped to %d unique URL(s); expectedMaps()=%d", len(g.URLs), g.expectedMaps())
	for i, u := range g.URLs {
		t.Logf("  URLs[%d] = %s", i, u)
	}

	// 3. What does the stats API say about each -mN slot?
	t.Logf("step 3: probing stats API for existing -mN docs")
	for i := 1; i <= max(g.expectedMaps()+2, 5); i++ { // probe a couple past expected to spot stragglers
		mid := matchIDForIndex(g.ID, i)
		exists, herr := statsClient.HasMatch(ctx, mid)
		if herr != nil {
			t.Logf("  HasMatch(%s): ERROR %v", mid, herr)
			continue
		}
		t.Logf("  HasMatch(%s) = %v", mid, exists)
	}

	// 4. Run the live skip check — this is the function that's supposedly
	//    returning false on every pass.
	skip := alreadyIngested(ctx, statsClient, g)
	t.Logf("step 4: alreadyIngested() = %v (true means skip, false means reprocess)", skip)

	// 5. Download each URL and inspect what's actually inside.
	t.Logf("step 5: downloading + inspecting each archive")
	totalDemos := 0
	for i, u := range g.URLs {
		t.Logf("  url[%d]: %s", i, u)
		path, size, cleanup, derr := downloadToTemp(ctx, u)
		if derr != nil {
			t.Logf("    download FAILED: %v", derr)
			continue
		}
		func() {
			defer cleanup()
			t.Logf("    downloaded %d bytes -> %s", size, path)

			arch, oerr := openArchive(path)
			if oerr != nil {
				t.Logf("    openArchive FAILED: %v", oerr)
				return
			}
			defer arch.Close()

			t.Logf("    archive has %d total entries:", len(arch.entries))
			for _, e := range arch.entries {
				t.Logf("      - %s", e.name)
			}
			demos := arch.demoEntries()
			t.Logf("    of which %d are .dem files:", len(demos))
			for _, d := range demos {
				t.Logf("      * %s", d.name)
			}
			totalDemos += len(demos)
		}()
	}

	// 6. The smoking gun: compare expected vs actual.
	t.Logf("==================================================================")
	t.Logf("DIAGNOSIS for match %s:", matchID)
	t.Logf("  CSC rows                : %d", len(rows))
	t.Logf("  Unique URLs (post-group): %d", len(g.URLs))
	t.Logf("  expectedMaps()          : %d   (= max(len(URLs), 1))", g.expectedMaps())
	t.Logf("  Total .dem in archives  : %d", totalDemos)
	t.Logf("  alreadyIngested()       : %v", skip)
	t.Logf("==================================================================")
}

// loadDotEnvForTest is a tiny dotenv reader so the integration test works
// from `go test` without a separate `source .env`. It silently no-ops when
// the file is missing — useful for CI runs that supply env directly.
func loadDotEnvForTest(t *testing.T, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq <= 0 {
			continue
		}
		k := strings.TrimSpace(line[:eq])
		v := strings.TrimSpace(line[eq+1:])
		if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
			v = v[1 : len(v)-1]
		}
		if _, set := os.LookupEnv(k); !set {
			os.Setenv(k, v)
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
