package reparse

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadCreatesDefault(t *testing.T) {
	p := filepath.Join(t.TempDir(), "reparse.yml")
	on, err := Load(p)
	if err != nil || on {
		t.Fatalf("Load(missing) = %v, %v; want false, nil", on, err)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("default file not created: %v", err)
	}
	if string(b) != DefaultContent {
		t.Fatalf("default file content = %q", b)
	}
}

func TestLoadAndReset(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"plain", "reparse: true\n", "reparse: false\n"},
		{"comments kept", "# keep me\nreparse: true # inline\nother: 1\n", "# keep me\nreparse: false # inline\nother: 1\n"},
		{"crlf", "# c\r\nreparse: true\r\n", "# c\r\nreparse: false\r\n"},
		{"yes spelled True", "reparse: True\n", "reparse: false\n"},
		{"default content", strings.Replace(DefaultContent, "reparse: false", "reparse: true", 1), DefaultContent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "reparse.yml")
			if err := os.WriteFile(p, []byte(tc.in), 0o644); err != nil {
				t.Fatal(err)
			}
			on, err := Load(p)
			if err != nil || !on {
				t.Fatalf("Load = %v, %v; want true, nil", on, err)
			}
			if err := Reset(p); err != nil {
				t.Fatalf("Reset: %v", err)
			}
			b, _ := os.ReadFile(p)
			if string(b) != tc.want {
				t.Fatalf("after Reset:\n%q\nwant\n%q", b, tc.want)
			}
		})
	}
}

func TestLoadFalseAndBad(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "off.yml")
	os.WriteFile(p, []byte("reparse: false\n"), 0o644)
	if on, err := Load(p); err != nil || on {
		t.Fatalf("Load(false) = %v, %v", on, err)
	}
	bad := filepath.Join(dir, "bad.yml")
	os.WriteFile(bad, []byte("reparse: [\n"), 0o644)
	if _, err := Load(bad); err == nil {
		t.Fatal("Load(bad yaml) = nil error")
	}
}
