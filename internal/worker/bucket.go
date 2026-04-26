package worker

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"time"
)

// Bucket hosts. The listing side talks to the origin (CDN edges don't
// consistently cache the XML LIST responses), while downloads go through the
// CDN — the same hostname CSC itself embeds in the demoUrls we get back
// from GraphQL.
const (
	bucketListURL     = "https://cscdemos.nyc3.digitaloceanspaces.com/"
	bucketDownloadURL = "https://cscdemos.nyc3.cdn.digitaloceanspaces.com/"
)

// regulationDemoRegex matches the CSC regulation-match demo filename format:
//
//	s<season>-M<match>-<team1>-vs-<team2>-mid<id>-<mapIdx>_<mapName>-<YYYY-MM-DD>_<HH-MM-SS>.dem.zip
//
// Example:
//
//	s19-M15-TheWaveWranglers-vs-PrettyPenne-mid8317-1_de_dust2-2026-04-01_01-55-05.dem.zip
//
// Team names are not allowed to contain hyphens (the CSC naming scheme uses
// CamelCase for multi-word names), which keeps the -vs- / -mid boundaries
// unambiguous. Captured groups: season, matchID, mapIndex (0-based).
var regulationDemoRegex = regexp.MustCompile(
	`^s(\d+)-M\d+-[^-]+-vs-[^-]+-mid(\d+)-(\d+)_[a-z0-9_]+-\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.dem\.zip$`,
)

// bucketDemo is one regulation-format .dem.zip object in the bucket.
type bucketDemo struct {
	Key      string // full S3 key, e.g. "s19/M13/s19-M13-...-mid8275-0_de_nuke-...dem.zip"
	Filename string // path.Base(Key)
	URL      string // CDN download URL (matches CSC's demoUrl host)
	MatchID  string // digits captured from "-mid<N>-"
	MapIndex int    // 0-based map index captured from "-mid<N>-<idx>_"
}

// parseRegulationFilename returns a populated bucketDemo (without Key/URL,
// which the caller supplies) when the filename matches the regulation
// format, and ok=false otherwise.
func parseRegulationFilename(filename string) (matchID string, mapIndex int, ok bool) {
	m := regulationDemoRegex.FindStringSubmatch(filename)
	if m == nil {
		return "", 0, false
	}
	idx, err := strconv.Atoi(m[3])
	if err != nil {
		return "", 0, false
	}
	return m[2], idx, true
}

// listBucketResult mirrors the subset of S3 ListBucket XML we need. Kept
// intentionally small — we don't care about ETags, StorageClass, etc.
type listBucketResult struct {
	XMLName  xml.Name `xml:"ListBucketResult"`
	Contents []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	} `xml:"Contents"`
	IsTruncated bool   `xml:"IsTruncated"`
	Marker      string `xml:"Marker"`
}

// listRegulationDemos scans the CSC demos bucket under s<season>/ and
// returns every object whose filename matches the regulation format. It
// paginates through all pages via the S3 "marker" field (the bucket has
// ~1000 objects per page and a season typically produces several thousand
// demos, so pagination is required).
//
// The returned list is in bucket-native order; callers should sort as
// needed. Non-regulation files (combines, scrims, old formats) are silently
// filtered out.
func listRegulationDemos(ctx context.Context, season int) ([]bucketDemo, error) {
	prefix := fmt.Sprintf("s%d/", season)
	httpClient := &http.Client{Timeout: 60 * time.Second}

	var out []bucketDemo
	marker := ""
	for pageNum := 1; ; pageNum++ {
		reqURL, err := url.Parse(bucketListURL)
		if err != nil {
			return nil, fmt.Errorf("parse bucket URL: %w", err)
		}
		q := reqURL.Query()
		q.Set("prefix", prefix)
		if marker != "" {
			q.Set("marker", marker)
		}
		reqURL.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("build list request: %w", err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list bucket page %d: %w", pageNum, err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 32*1024*1024))
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read list page %d: %w", pageNum, readErr)
		}
		if resp.StatusCode/100 != 2 {
			snip := body
			if len(snip) > 512 {
				snip = snip[:512]
			}
			return nil, fmt.Errorf("bucket list page %d returned %d: %s", pageNum, resp.StatusCode, string(snip))
		}

		var parsed listBucketResult
		if err := xml.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("parse list page %d: %w", pageNum, err)
		}

		for _, c := range parsed.Contents {
			filename := path.Base(c.Key)
			mid, idx, ok := parseRegulationFilename(filename)
			if !ok {
				continue
			}
			out = append(out, bucketDemo{
				Key:      c.Key,
				Filename: filename,
				URL:      bucketDownloadURL + c.Key,
				MatchID:  mid,
				MapIndex: idx,
			})
		}

		if !parsed.IsTruncated || len(parsed.Contents) == 0 {
			break
		}
		marker = parsed.Contents[len(parsed.Contents)-1].Key
	}
	return out, nil
}
