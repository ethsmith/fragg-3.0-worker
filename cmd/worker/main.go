// parser-worker daemon entrypoint.
//
// Two modes, picked from CHECK_INTERVAL_MINUTES:
//
//   - 0 or unset: run one worker pass, print a JSON summary, exit. Use this
//     when you're driving the schedule externally (system cron, k8s
//     CronJob, etc.).
//   - >0:         run forever, sleeping CHECK_INTERVAL_MINUTES between
//     passes. SIGINT/SIGTERM stop the loop cleanly between passes (or
//     interrupt the in-flight pass and finish whatever demo it was on).
//
// Usage:
//
//	cp .env.example .env   # fill in real values
//	go run ./cmd/worker
//
// Or build a static binary and run it under systemd / docker:
//
//	go build -o parser-worker ./cmd/worker
//	./parser-worker
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"parser-worker/internal/config"
	"parser-worker/internal/worker"
)

func main() {
	loadDotEnv(".env")
	ensureTempDir()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.CheckIntervalMinutes <= 0 {
		runOnce(ctx, cfg)
		return
	}
	runDaemon(ctx, cfg)
}

// runOnce executes a single worker pass and exits. Process exit code is 0 on
// success, 1 on a hard run failure (per-demo failures are non-fatal).
func runOnce(ctx context.Context, cfg *config.Config) {
	res, runErr := worker.Run(ctx, cfg)
	printSummary(res, runErr)
	if runErr != nil {
		os.Exit(1)
	}
}

// runDaemon loops forever: pass, sleep, pass, sleep. The sleep interrupts on
// SIGINT/SIGTERM so `systemctl stop` returns promptly. A failure in one pass
// is logged and the loop continues — this is a worker, not a one-shot.
func runDaemon(ctx context.Context, cfg *config.Config) {
	interval := time.Duration(cfg.CheckIntervalMinutes) * time.Minute
	log.Printf("[main] daemon mode, interval=%s", interval)

	for {
		res, runErr := worker.Run(ctx, cfg)
		printSummary(res, runErr)
		if runErr != nil {
			log.Printf("[main] pass error (continuing): %v", runErr)
		}

		select {
		case <-ctx.Done():
			log.Printf("[main] shutdown signal received, exiting")
			return
		case <-time.After(interval):
		}
	}
}

func printSummary(res *worker.Result, runErr error) {
	out, _ := json.MarshalIndent(map[string]interface{}{
		"ok":     runErr == nil,
		"error":  errString(runErr),
		"result": res,
	}, "", "  ")
	fmt.Println(string(out))
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// ensureTempDir points the OS temp directory at <cwd>/tmp so downloaded
// demo archives (50–500 MB each) land on the same volume the binary runs
// from, not the system /tmp. This is critical under Pterodactyl/Docker
// where /tmp is a small tmpfs (often 64 MB) — a single demo download
// would fill it. On a normal box this just creates a tmp/ next to the
// binary, which is harmless and easy to inspect/clean.
//
// Honors an explicit TMPDIR if the operator set one (e.g. pointing at a
// dedicated scratch disk); only falls back to <cwd>/tmp when TMPDIR is
// unset or the default "/tmp".
func ensureTempDir() {
	if t := os.Getenv("TMPDIR"); t != "" && t != "/tmp" {
		return
	}
	wd, err := os.Getwd()
	if err != nil {
		log.Printf("[main] warning: getwd failed (%v); leaving TMPDIR alone", err)
		return
	}
	dir := filepath.Join(wd, "tmp")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("[main] warning: mkdir %s failed (%v); leaving TMPDIR alone", dir, err)
		return
	}
	if err := os.Setenv("TMPDIR", dir); err != nil {
		log.Printf("[main] warning: setenv TMPDIR=%s failed (%v)", dir, err)
		return
	}
	log.Printf("[main] TMPDIR=%s (avoid small /tmp tmpfs in containers)", dir)
}

// loadDotEnv is a tiny dotenv loader so the worker doesn't pull a dependency
// just for env-file parsing. It silently does nothing when the file is
// missing — useful when env vars are provided by docker/systemd/etc.
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	const maxLine = 1 << 16
	buf := make([]byte, 0, maxLine)
	tmp := make([]byte, 4096)
	for {
		n, err := f.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			break
		}
	}
	for _, raw := range splitLines(buf) {
		line := stripComment(trim(raw))
		if line == "" {
			continue
		}
		eq := indexByte(line, '=')
		if eq <= 0 {
			continue
		}
		k := trim(line[:eq])
		v := trim(line[eq+1:])
		v = stripQuotes(v)
		if _, set := os.LookupEnv(k); set {
			continue
		}
		_ = os.Setenv(k, v)
	}
}

func splitLines(b []byte) []string {
	var out []string
	start := 0
	for i, c := range b {
		if c == '\n' {
			out = append(out, string(b[start:i]))
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, string(b[start:]))
	}
	return out
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func trim(s string) string {
	i, j := 0, len(s)
	for i < j && (s[i] == ' ' || s[i] == '\t' || s[i] == '\r') {
		i++
	}
	for j > i && (s[j-1] == ' ' || s[j-1] == '\t' || s[j-1] == '\r') {
		j--
	}
	return s[i:j]
}

func stripComment(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '#' {
			return trim(s[:i])
		}
	}
	return s
}

func stripQuotes(s string) string {
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1]
	}
	return s
}
