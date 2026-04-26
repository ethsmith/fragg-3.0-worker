package worker

import (
	"reflect"
	"testing"

	"parser-worker/internal/csc"
)

func TestGroupMatches_SingleArchivePerMatch(t *testing.T) {
	in := []csc.Match{
		{ID: "8275", DemoURL: "https://example/mid8275-0_de_nuke.dem.zip"},
		{ID: "8276", DemoURL: "https://example/mid8276-0_de_inferno.dem.zip"},
	}
	got := groupMatches(in)
	if len(got) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(got))
	}
	if got[0].ID != "8275" || len(got[0].URLs) != 1 {
		t.Errorf("group 0: %+v", got[0])
	}
	if got[1].ID != "8276" || len(got[1].URLs) != 1 {
		t.Errorf("group 1: %+v", got[1])
	}
}

func TestGroupMatches_MultiURLBO3(t *testing.T) {
	// CSC sometimes returns the BO3 maps as three separate rows with the
	// same match ID. Out-of-order on purpose so we verify the URL sort.
	in := []csc.Match{
		{ID: "9001", DemoURL: "https://example/mid9001-2_de_mirage.dem.zip"},
		{ID: "9001", DemoURL: "https://example/mid9001-0_de_nuke.dem.zip"},
		{ID: "9001", DemoURL: "https://example/mid9001-1_de_inferno.dem.zip"},
	}
	got := groupMatches(in)
	if len(got) != 1 {
		t.Fatalf("expected 1 group, got %d", len(got))
	}
	want := []string{
		"https://example/mid9001-0_de_nuke.dem.zip",
		"https://example/mid9001-1_de_inferno.dem.zip",
		"https://example/mid9001-2_de_mirage.dem.zip",
	}
	if !reflect.DeepEqual(got[0].URLs, want) {
		t.Errorf("URLs not sorted in play order:\n got=%v\nwant=%v", got[0].URLs, want)
	}
	if got[0].expectedMaps() != 3 {
		t.Errorf("expectedMaps()=%d, want 3", got[0].expectedMaps())
	}
}

func TestGroupMatches_PreservesFirstSeenOrder(t *testing.T) {
	// The chronological pre-sort in Run must survive grouping, so we
	// preserve first-seen order across groups.
	in := []csc.Match{
		{ID: "B", DemoURL: "u-B-1"},
		{ID: "A", DemoURL: "u-A-1"},
		{ID: "B", DemoURL: "u-B-2"},
		{ID: "C", DemoURL: "u-C-1"},
	}
	got := groupMatches(in)
	ids := []string{got[0].ID, got[1].ID, got[2].ID}
	want := []string{"B", "A", "C"}
	if !reflect.DeepEqual(ids, want) {
		t.Errorf("group order = %v, want %v", ids, want)
	}
	if len(got[0].URLs) != 2 {
		t.Errorf("group B should have 2 urls, got %d", len(got[0].URLs))
	}
}

func TestExpectedMaps_Fallback(t *testing.T) {
	// A group with no URLs (shouldn't happen in practice, but be defensive)
	// still reports >= 1 so alreadyIngested loops at least once.
	g := matchGroup{ID: "x"}
	if g.expectedMaps() != 1 {
		t.Errorf("expectedMaps() with no URLs = %d, want 1", g.expectedMaps())
	}
}
