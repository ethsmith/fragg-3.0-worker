// Package stats wraps the fragg-3.0 stats API endpoints the worker needs:
//
//	HEAD   /matches/:type/:matchId   is the map already stored?
//	PUT    /matches/:type/:matchId   store a map's payload, replacing any existing one
//	DELETE /matches/:type/:matchId   delete a map (the reparse clears old records first)
//
// Writes send the shared secret as Authorization: Bearer <key>.
package stats

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Client talks to the stats API.
type Client struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client
}

// NewClient builds a Client for a base URL like https://fragg-3-0-api.example.com
// (no trailing slash).
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		BaseURL: baseURL,
		APIKey:  apiKey,
		HTTP: &http.Client{
			// A full payload is a few MB before gzip; leave room for a slow link.
			Timeout: 2 * time.Minute,
		},
	}
}

func (c *Client) matchURL(matchType, matchID string) string {
	return fmt.Sprintf("%s/matches/%s/%s", c.BaseURL, url.PathEscape(matchType), url.PathEscape(matchID))
}

// HasMatch reports whether the API already stores the map (matchType, matchID).
// Used to skip re-downloading and re-parsing a demo already ingested.
func (c *Client) HasMatch(ctx context.Context, matchType, matchID string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.matchURL(matchType, matchID), nil)
	if err != nil {
		return false, fmt.Errorf("build has-match request: %w", err)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return false, fmt.Errorf("call has-match: %w", err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusOK:
		return true, nil
	case resp.StatusCode == http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("has-match returned %d", resp.StatusCode)
	}
}

// PutMatch stores doc (the engine payload plus the worker's metadata) as the
// map (matchType, matchID), replacing any existing document. The body is
// gzipped JSON.
func (c *Client) PutMatch(ctx context.Context, matchType, matchID string, doc any) error {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if err := json.NewEncoder(zw).Encode(doc); err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("gzip payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.matchURL(matchType, matchID), &buf)
	if err != nil {
		return fmt.Errorf("build put request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	return c.do(req, "put")
}

// DeleteMatch deletes the map (matchType, matchID). Deleting a map that isn't
// stored is not an error.
func (c *Client) DeleteMatch(ctx context.Context, matchType, matchID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.matchURL(matchType, matchID), nil)
	if err != nil {
		return fmt.Errorf("build delete request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	return c.do(req, "delete")
}

func (c *Client) do(req *http.Request, what string) error {
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("call %s: %w", what, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s returned %d: %s", what, resp.StatusCode, truncate(body, 512))
	}
	return nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "...(truncated)"
}
