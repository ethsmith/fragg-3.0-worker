package worker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIgnoreList_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ignore.txt")

	// Empty / missing file: load returns empty list, no error.
	il, err := loadIgnoreList(path)
	if err != nil {
		t.Fatalf("load missing: %v", err)
	}
	if il.Len() != 0 {
		t.Fatalf("missing file should yield 0 entries, got %d", il.Len())
	}

	// Add three, verify in-memory state.
	for _, id := range []string{"7705", "7706", "7707"} {
		if err := il.Add(id, "download_404", "https://example/"+id); err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
	}
	if il.Len() != 3 {
		t.Fatalf("expected 3 after Add, got %d", il.Len())
	}
	if !il.Has("7706") {
		t.Fatal("Has(7706) should be true")
	}
	if il.Has("9999") {
		t.Fatal("Has(9999) should be false")
	}

	// Re-Add is idempotent and shouldn't double-write.
	if err := il.Add("7705", "download_404", "https://example/7705"); err != nil {
		t.Fatalf("re-add: %v", err)
	}
	if il.Len() != 3 {
		t.Fatalf("re-add changed Len to %d", il.Len())
	}

	// Reload from disk; ignore-set must round-trip.
	il2, err := loadIgnoreList(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	for _, id := range []string{"7705", "7706", "7707"} {
		if !il2.Has(id) {
			t.Errorf("reload missing id %s", id)
		}
	}

	// Comments and blank lines must be ignored on parse.
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "# parser-worker ignore list.") {
		t.Errorf("expected header comment, got:\n%s", bodyStr)
	}
}

func TestIgnoreList_NilSafe(t *testing.T) {
	var il *ignoreList
	if il.Has("anything") {
		t.Fatal("nil Has should be false")
	}
	if il.Len() != 0 {
		t.Fatal("nil Len should be 0")
	}
	if err := il.Add("1", "r", "u"); err != nil {
		t.Fatalf("nil Add should be no-op, got %v", err)
	}
}

func TestIgnoreList_HandWrittenFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ignore.txt")
	// Operator-edited file: comments, blank lines, just-an-id, and full
	// tab-separated records all parse.
	hand := strings.Join([]string{
		"# header",
		"",
		"7705",
		"7706\tdownload_404\t2026-04-26T12:42:58Z\thttps://example/7706",
		"   # leading-space comment",
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(hand), 0o644); err != nil {
		t.Fatal(err)
	}
	il, err := loadIgnoreList(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if il.Len() != 2 {
		t.Fatalf("expected 2 entries, got %d", il.Len())
	}
	if !il.Has("7705") || !il.Has("7706") {
		t.Errorf("missing expected ids: %+v", il.set)
	}
}
