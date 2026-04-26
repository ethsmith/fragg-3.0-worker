// End-to-end smoke test: download a CSC archive (.zip OR .7z), pull every
// .dem out of it, and run the eco-rating parser against each one. Prints a
// per-demo summary. Does NOT post anywhere — purely local validation.
//
// Usage:
//
//	go run ./cmd/parsetest <demo-url>
//
// Default URL is one of the season 19 .7z matches we know exists.
package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/bodgit/sevenzip"
	"github.com/ethsmith/eco-rating/model"
	"github.com/ethsmith/eco-rating/parser"
)

func main() {
	url := "https://cscdemos.nyc3.cdn.digitaloceanspaces.com/s19/M01/s19-M01-Drakes-vs-TheMasters-mid7704.7z"
	if len(os.Args) > 1 {
		url = os.Args[1]
	}

	fmt.Printf("downloading %s\n", url)
	path, cleanup, err := downloadToTemp(url)
	if err != nil {
		die("download:", err)
	}
	defer cleanup()
	fi, _ := os.Stat(path)
	fmt.Printf("downloaded %s (%.2f MB)\n", filepath.Base(path), float64(fi.Size())/(1024*1024))

	format, err := detectFormat(path)
	if err != nil {
		die("sniff:", err)
	}
	fmt.Printf("detected format: %s\n", format)

	demos, closer, err := listDemos(path, format)
	if err != nil {
		die("list:", err)
	}
	defer closer.Close()
	fmt.Printf("found %d .dem entries\n", len(demos))

	for i, d := range demos {
		t0 := time.Now()
		players, mapName, err := parseDemo(d)
		dur := time.Since(t0)
		if err != nil {
			fmt.Printf("  [%d] %s: ERROR %v (%.2fs)\n", i+1, d.name, err, dur.Seconds())
			continue
		}
		fmt.Printf("  [%d] %s: map=%s players=%d (%.2fs)\n",
			i+1, d.name, mapName, len(players), dur.Seconds())
	}
}

type entry struct {
	name string
	open func() (io.ReadCloser, error)
}

func listDemos(path, format string) ([]entry, io.Closer, error) {
	switch format {
	case "zip":
		zr, err := zip.OpenReader(path)
		if err != nil {
			return nil, nil, err
		}
		var es []entry
		for _, f := range zr.File {
			if f.FileInfo().IsDir() || !strings.HasSuffix(strings.ToLower(f.Name), ".dem") {
				continue
			}
			f := f
			es = append(es, entry{name: f.Name, open: func() (io.ReadCloser, error) { return f.Open() }})
		}
		sortEntries(es)
		return es, zr, nil
	case "7z":
		zr, err := sevenzip.OpenReader(path)
		if err != nil {
			return nil, nil, err
		}
		var es []entry
		for _, f := range zr.File {
			if f.FileInfo().IsDir() || !strings.HasSuffix(strings.ToLower(f.Name), ".dem") {
				continue
			}
			f := f
			es = append(es, entry{name: f.Name, open: func() (io.ReadCloser, error) { return f.Open() }})
		}
		sortEntries(es)
		return es, zr, nil
	}
	return nil, nil, fmt.Errorf("unsupported %q", format)
}

func sortEntries(es []entry) {
	sort.SliceStable(es, func(i, j int) bool {
		return strings.ToLower(filepath.Base(es[i].name)) <
			strings.ToLower(filepath.Base(es[j].name))
	})
}

func parseDemo(e entry) (map[uint64]*model.PlayerStats, string, error) {
	rc, err := e.open()
	if err != nil {
		return nil, "", err
	}
	defer rc.Close()
	br := bufio.NewReaderSize(rc, 1<<20)
	p := parser.NewDemoParser(br)
	if err := p.Parse(); err != nil {
		return nil, "", err
	}
	return p.GetPlayers(), p.GetMapName(), nil
}

func detectFormat(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	var head [6]byte
	if _, err := io.ReadFull(f, head[:]); err != nil {
		return "", err
	}
	switch {
	case bytes.HasPrefix(head[:], []byte{'P', 'K', 0x03, 0x04}),
		bytes.HasPrefix(head[:], []byte{'P', 'K', 0x05, 0x06}),
		bytes.HasPrefix(head[:], []byte{'P', 'K', 0x07, 0x08}):
		return "zip", nil
	case bytes.Equal(head[:], []byte{'7', 'z', 0xBC, 0xAF, 0x27, 0x1C}):
		return "7z", nil
	}
	return "", fmt.Errorf("unknown magic: % x", head)
}

func downloadToTemp(url string) (string, func(), error) {
	tmp, err := os.CreateTemp("", "demo-*.bin")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	c := &http.Client{Timeout: 5 * time.Minute}
	resp, err := c.Do(req)
	if err != nil {
		cleanup()
		return "", func() {}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		cleanup()
		return "", func() {}, fmt.Errorf("status %d", resp.StatusCode)
	}
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		cleanup()
		return "", func() {}, err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return tmp.Name(), cleanup, nil
}

func die(prefix string, err error) {
	fmt.Fprintln(os.Stderr, prefix, err)
	os.Exit(1)
}
