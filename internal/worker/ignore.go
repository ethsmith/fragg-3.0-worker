// Persistent skiplist of CSC match IDs the worker should never retry.
//
// The on-disk format is human-readable, tab-separated, one record per line:
//
//	<match_id>\t<reason>\t<RFC3339 timestamp>\t<demo_url>
//
// Lines starting with '#' or that are blank are ignored. Operators recover a
// match (e.g. after the demo gets re-uploaded) by deleting its line — or by
// wiping the file entirely. The list is reloaded from disk at the top of
// every Run, so changes take effect on the next pass with no restart.
package worker

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type ignoreList struct {
	path string
	mu   sync.Mutex
	set  map[string]struct{}
}

// loadIgnoreList reads path into memory. A missing file is not an error —
// it returns an empty list that future Add calls will create on first write.
func loadIgnoreList(path string) (*ignoreList, error) {
	il := &ignoreList{path: path, set: make(map[string]struct{})}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return il, nil
		}
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	// Match IDs are short, but demo URLs in the trailing column can push a
	// line past the 64 KiB default. 1 MiB is plenty and keeps the loader
	// resilient to anyone hand-editing comments into the file.
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		il.set[fields[0]] = struct{}{}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}
	return il, nil
}

// Has reports whether id is in the skiplist. Safe to call on a nil receiver
// (always false) so callers don't need to gate on whether the feature is on.
func (il *ignoreList) Has(id string) bool {
	if il == nil {
		return false
	}
	il.mu.Lock()
	defer il.mu.Unlock()
	_, ok := il.set[id]
	return ok
}

// Len returns the number of skiplisted IDs. nil-safe (returns 0).
func (il *ignoreList) Len() int {
	if il == nil {
		return 0
	}
	il.mu.Lock()
	defer il.mu.Unlock()
	return len(il.set)
}

// Add records id in memory and appends a line to the file. Idempotent within
// a process: re-adding an id is a no-op (no extra disk write). nil-safe.
func (il *ignoreList) Add(id, reason, demoURL string) error {
	if il == nil {
		return nil
	}
	il.mu.Lock()
	defer il.mu.Unlock()
	if _, ok := il.set[id]; ok {
		return nil
	}
	il.set[id] = struct{}{}

	needHeader := false
	if _, err := os.Stat(il.path); os.IsNotExist(err) {
		needHeader = true
	}
	f, err := os.OpenFile(il.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open ignore file: %w", err)
	}
	defer f.Close()

	if needHeader {
		fmt.Fprintln(f, "# parser-worker ignore list.")
		fmt.Fprintln(f, "# Format: <match_id>\\t<reason>\\t<RFC3339 timestamp>\\t<demo_url>")
		fmt.Fprintln(f, "# Delete a line (or wipe the whole file) to retry that match.")
	}
	if _, err := fmt.Fprintf(f, "%s\t%s\t%s\t%s\n", id, reason, time.Now().UTC().Format(time.RFC3339), demoURL); err != nil {
		return fmt.Errorf("append ignore record: %w", err)
	}
	return nil
}
