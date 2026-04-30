// Package csc provides a minimal client for the CSC core GraphQL API.
//
// We only need a single query: enumerate all matches in a given season and
// return their demo URLs so the worker can download and parse them.
package csc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Match is the subset of fields the worker needs from CSC core's "matches"
// query. Anything beyond demoUrl + identifying info is ignored.
type Match struct {
	ID          string      `json:"id"`
	DemoURL     string      `json:"demoUrl"`
	MatchType   string      `json:"matchType"`
	CompletedAt string      `json:"completedAt"`
	Home        Team        `json:"home"`
	Away        Team        `json:"away"`
	Stats       []MatchStat `json:"stats"`
}

// Team captures just enough of a match side to decide whether it is a test
// roster (Test Franchise / TFR / TestHomeTeam / etc.). Roster-level details
// are intentionally omitted.
type Team struct {
	Name      string    `json:"name"`
	Franchise Franchise `json:"franchise"`
}

// Franchise is the org a team belongs to. `prefix` is the short tag (e.g.
// "TFR" for the test franchise); `name` is the long name (e.g. "Test
// Franchise").
type Franchise struct {
	Prefix string `json:"prefix"`
	Name   string `json:"name"`
}

// MatchStat is the per-map record CSC returns under each match.
type MatchStat struct {
	MapName   string `json:"mapName"`
	MapNumber int    `json:"mapNumber"`
}

// Client talks to the CSC core GraphQL endpoint.
type Client struct {
	URL  string
	HTTP *http.Client
}

// NewClient returns a Client configured for the given GraphQL URL.
//
// The underlying http.Client re-posts the original method and body when the
// upstream issues a 301/302/307/308. Go's default CheckRedirect follows
// redirects but demotes POST to GET on 301/302, which silently drops the
// GraphQL body and yields "No GraphQL query found in the request" errors —
// exactly what CSC's core host did when it started redirecting to playcsc.com.
func NewClient(url string) *Client {
	return &Client{
		URL: url,
		HTTP: &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 5 {
					return http.ErrUseLastResponse
				}
				if len(via) > 0 {
					// Preserve method + body on redirect by copying them from
					// the original request. net/http already rewinds the body
					// via Request.GetBody when it's set (bytes.Reader supplies
					// one automatically through http.NewRequest).
					orig := via[0]
					req.Method = orig.Method
					if req.Header.Get("Content-Type") == "" {
						req.Header.Set("Content-Type", orig.Header.Get("Content-Type"))
					}
				}
				return nil
			},
		},
	}
}

const matchesQuery = `query Matches($season: Int!) {
  matches(season: $season) {
    id
    completedAt
    demoUrl
    matchType
    home {
      name
      franchise {
        prefix
        name
      }
    }
    away {
      name
      franchise {
        prefix
        name
      }
    }
    stats {
      mapName
      mapNumber
    }
  }
}`

type graphRequest struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables"`
}

type graphResponse struct {
	Data struct {
		Matches []Match `json:"matches"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// MatchesBySeason returns every match in the given season. Matches that have
// not been played yet (no demoUrl) are still returned; callers should filter.
func (c *Client) MatchesBySeason(ctx context.Context, season int) ([]Match, error) {
	body, err := json.Marshal(graphRequest{
		Query:     matchesQuery,
		Variables: map[string]interface{}{"season": season},
	})
	if err != nil {
		return nil, fmt.Errorf("encode graphql body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call csc graphql: %w", err)
	}
	defer resp.Body.Close()

	rawBody, _ := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("csc graphql returned %d: %s", resp.StatusCode, truncate(rawBody, 512))
	}

	var parsed graphResponse
	if err := json.Unmarshal(rawBody, &parsed); err != nil {
		return nil, fmt.Errorf("decode graphql response: %w (body=%s)", err, truncate(rawBody, 512))
	}
	if len(parsed.Errors) > 0 {
		return nil, fmt.Errorf("csc graphql error: %s", parsed.Errors[0].Message)
	}
	return parsed.Data.Matches, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "...(truncated)"
}
