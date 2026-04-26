// Tiny smoke-test runner. Quick "is the CSC API reachable + are franchise
// names being returned" check. Not used in production.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"parser-worker/internal/csc"
)

func main() {
	url := "https://core.playcsc.com/graphql"
	if len(os.Args) > 1 {
		url = os.Args[1]
	}
	c := csc.NewClient(url)
	matches, err := c.MatchesBySeason(context.Background(), 19)
	if err != nil {
		fmt.Println("ERROR:", err)
		os.Exit(1)
	}
	// Group by match ID to see if a single match ever has both .zip and .7z
	byID := map[string][]string{}
	for _, m := range matches {
		if m.DemoURL == "" {
			continue
		}
		byID[m.ID] = append(byID[m.ID], m.DemoURL)
	}
	dupBothExts := 0
	for _, urls := range byID {
		hasZip, has7z := false, false
		for _, u := range urls {
			lu := strings.ToLower(u)
			if strings.HasSuffix(lu, ".zip") {
				hasZip = true
			}
			if strings.HasSuffix(lu, ".7z") {
				has7z = true
			}
		}
		if hasZip && has7z {
			dupBothExts++
		}
	}
	fmt.Printf("matches with BOTH .zip and .7z URLs: %d\n", dupBothExts)

	// For up to 3 .7z URLs, see whether swapping the extension to .zip is a
	// live alternative on the CDN. HEAD only — no body downloaded.
	probed := 0
	for _, m := range matches {
		if m.DemoURL == "" || !strings.HasSuffix(strings.ToLower(m.DemoURL), ".7z") {
			continue
		}
		if isTestish(m) {
			continue
		}
		zipURL := strings.TrimSuffix(m.DemoURL, ".7z") + ".zip"
		zipURLAlt := strings.TrimSuffix(m.DemoURL, ".7z") + ".ZIP"
		statusZip := headStatus(zipURL)
		statusAlt := headStatus(zipURLAlt)
		status7z := headStatus(m.DemoURL)
		fmt.Printf("match %s\n  7z  : %d  %s\n  zip : %d  %s\n  ZIP : %d  %s\n",
			m.ID, status7z, m.DemoURL, statusZip, zipURL, statusAlt, zipURLAlt)
		probed++
		if probed >= 3 {
			break
		}
	}
}

func headStatus(u string) int {
	req, _ := http.NewRequest(http.MethodHead, u, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func isTestish(m csc.Match) bool {
	for _, t := range []csc.Team{m.Home, m.Away} {
		if strings.EqualFold(t.Franchise.Prefix, "TFR") {
			return true
		}
		if hasTestPrefix(t.Franchise.Name) || hasTestPrefix(t.Name) {
			return true
		}
	}
	return false
}

func hasTestPrefix(s string) bool {
	s = strings.TrimSpace(s)
	if len(s) < 4 {
		return false
	}
	return strings.EqualFold(s[:4], "test")
}
