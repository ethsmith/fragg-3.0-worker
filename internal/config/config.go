// Package config loads the worker's runtime configuration from environment
// variables. All callers (the daemon entrypoint, the probe tools, etc.)
// share the same Load() entrypoint so config errors fail loudly and
// uniformly at startup rather than mid-run.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds resolved worker configuration.
type Config struct {
	// Season is the CSC season number to poll for completed matches.
	Season int

	// MatchType filters CSC matches (default "Regulation").
	MatchType string

	// CSCGraphQLURL is the CSC core GraphQL endpoint.
	CSCGraphQLURL string

	// StatsAPIURL is the base URL of the fragg-3.0 stats API. No trailing
	// slash; e.g. https://fragg-3-0-api.example.com.
	StatsAPIURL string

	// StatsAPIKey is the shared secret accepted by the stats API
	// (sent as Authorization: Bearer <key>).
	StatsAPIKey string

	// MaxMatchesPerRun caps how many CSC matches the worker will attempt to
	// process in one pass. Each match may contain multiple demos. Set this
	// to bound a single pass's wall-clock when running under tight resource
	// limits; on a dedicated server you can raise it freely.
	MaxMatchesPerRun int

	// CheckIntervalMinutes controls the worker's run mode in cmd/worker:
	//
	//   - 0 (or unset): run a single pass and exit. Use this when an
	//     external scheduler (system cron, k8s CronJob, etc.) is driving
	//     the cadence.
	//   - >0:           run as a daemon, sleeping this many minutes
	//     between passes.
	CheckIntervalMinutes int

	// IgnoreFile is the path to a persistent skiplist of CSC match IDs the
	// worker should never retry — populated automatically when a demo URL
	// returns HTTP 4xx (typically 404). Operators recover a match by
	// deleting its line from this file (or wiping the whole file). Set to
	// "" to disable the feature entirely. Default "ignore.txt" lives next
	// to the binary, which under Pterodactyl is /home/container/ignore.txt
	// and is therefore visible/editable from the panel's Files tab.
	IgnoreFile string
}

// Load resolves a Config from os.Getenv. Returns an error if any required
// value is missing or malformed.
func Load() (*Config, error) {
	cfg := &Config{
		MatchType:            getEnvDefault("MATCH_TYPE", "Regulation"),
		CSCGraphQLURL:        getEnvDefault("CSC_GRAPHQL_URL", "https://core.playcsc.com/graphql"),
		StatsAPIURL:          strings.TrimRight(os.Getenv("STATS_API_URL"), "/"),
		StatsAPIKey:          os.Getenv("STATS_API_KEY"),
		MaxMatchesPerRun:     getEnvInt("MAX_MATCHES_PER_RUN", 50),
		CheckIntervalMinutes: getEnvInt("CHECK_INTERVAL_MINUTES", 0),
		IgnoreFile:           getEnvDefault("IGNORE_FILE", "ignore.txt"),
	}

	season, err := strconv.Atoi(strings.TrimSpace(os.Getenv("SEASON")))
	if err != nil || season <= 0 {
		return nil, errors.New("SEASON env var is required and must be a positive integer")
	}
	cfg.Season = season

	if cfg.StatsAPIURL == "" {
		return nil, errors.New("STATS_API_URL env var is required")
	}
	if cfg.StatsAPIKey == "" {
		return nil, errors.New("STATS_API_KEY env var is required")
	}
	if cfg.MaxMatchesPerRun <= 0 {
		return nil, fmt.Errorf("MAX_MATCHES_PER_RUN must be > 0 (got %d)", cfg.MaxMatchesPerRun)
	}
	return cfg, nil
}

func getEnvDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
