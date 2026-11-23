// Package reparse reads and resets the worker's reparse flag file
// (reparse.yml by default).
//
// The file holds one setting:
//
//	reparse: false
//
// Set it to true and restart the worker: on startup it re-parses every demo,
// deletes each map's old record from the stats DB and stores the new one.
// When the reparse pass gets through every demo, the worker sets the flag
// back to false so the next restart is a normal one.
package reparse

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"

	"go.yaml.in/yaml/v3"
)

// DefaultContent is written when the file doesn't exist yet.
const DefaultContent = `# Reparse every demo on the next worker start.
#
# Set to true and restart the worker. On startup it re-parses every demo it
# can find for the configured SEASON (CSC matches and every bucket scan),
# deletes each map's old record from the stats DB, and stores the new one.
# MAX_MATCHES_PER_RUN does not apply to the reparse pass.
#
# Once every demo has been processed, the worker sets this back to false.
# If the pass is interrupted (stop/restart) or a CSC fetch or bucket scan
# fails, it stays true and the reparse runs again (the daemon's next pass,
# or the next start).
reparse: false
`

type file struct {
	Reparse bool `yaml:"reparse"`
}

// Load reports whether path asks for a reparse. A missing file is created
// with DefaultContent (reparse: false), so operators find it next to the
// binary.
func Load(path string) (bool, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		if werr := os.WriteFile(path, []byte(DefaultContent), 0o644); werr != nil {
			return false, fmt.Errorf("create %s: %w", path, werr)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read %s: %w", path, err)
	}
	var f file
	if err := yaml.Unmarshal(b, &f); err != nil {
		return false, fmt.Errorf("parse %s: %w", path, err)
	}
	return f.Reparse, nil
}

var reparseLine = regexp.MustCompile(`(?m)^(\s*reparse\s*:\s*)[^#\r\n]*?(\s*(?:#.*)?)(\r?)$`)

// Reset sets the flag in path back to false, keeping the rest of the file
// (comments included) as the operator wrote it.
func Reset(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	out := b
	if reparseLine.Match(b) {
		out = reparseLine.ReplaceAll(b, []byte("${1}false${2}${3}"))
	} else {
		out = []byte(DefaultContent)
	}
	if err := os.WriteFile(path, out, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if on, err := Load(path); err != nil || on {
		return fmt.Errorf("reset %s: flag still set after rewrite (err=%v)", path, err)
	}
	return nil
}
