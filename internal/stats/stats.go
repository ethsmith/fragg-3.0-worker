// Package stats wraps the fragg-3.0 stats API endpoints the worker needs:
//
//   GET  /player-stats/match/:matchId   - check whether the match is already ingested
//   POST /player-stats/match/:matchId   - upsert player-stats docs for that match
//
// Auth uses the shared Bearer secret accepted by requireApiKey middleware.
package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Client posts player-stats payloads to the stats API.
type Client struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client
}

// NewClient builds a Client for a base URL like https://fragg-3-0-api.example.com
// (no trailing slash; if present, it is tolerated).
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		BaseURL: baseURL,
		APIKey:  apiKey,
		HTTP: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// HasMatch reports whether the stats API already contains at least one
// player-stats document for the given match_id. Used to avoid re-downloading
// and re-parsing a demo we already ingested.
func (c *Client) HasMatch(ctx context.Context, matchID string) (bool, error) {
	endpoint := fmt.Sprintf("%s/player-stats/match/%s", c.BaseURL, url.PathEscape(matchID))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, fmt.Errorf("build has-match request: %w", err)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return false, fmt.Errorf("call has-match: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1*1024*1024))
	if resp.StatusCode/100 != 2 {
		return false, fmt.Errorf("has-match returned %d: %s", resp.StatusCode, truncate(body, 256))
	}

	var payload struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return false, fmt.Errorf("decode has-match: %w", err)
	}
	return payload.Count > 0, nil
}

// UpsertMatch sends the player-stats array for a single match (one demo's
// players) to the stats API with ?upsert=true so reprocessing the same demo
// is idempotent.
//
// docs MUST be a JSON-marshalable slice (the parser's []*model.PlayerStats
// works directly because every relevant field is tagged for JSON).
func (c *Client) UpsertMatch(ctx context.Context, matchID string, docs interface{}) error {
	body, err := json.Marshal(docs)
	if err != nil {
		return fmt.Errorf("encode player stats: %w", err)
	}

	endpoint := fmt.Sprintf(
		"%s/player-stats/match/%s?upsert=true",
		c.BaseURL,
		url.PathEscape(matchID),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build upsert request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.APIKey)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("call upsert: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1*1024*1024))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("upsert returned %d: %s", resp.StatusCode, truncate(respBody, 512))
	}
	return nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "...(truncated)"
}
