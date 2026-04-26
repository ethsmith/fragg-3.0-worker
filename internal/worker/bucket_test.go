package worker

import "testing"

func TestParseRegulationFilename(t *testing.T) {
	cases := []struct {
		name       string
		filename   string
		wantOK     bool
		wantID     string
		wantMapIdx int
	}{
		{
			name:       "canonical example from spec",
			filename:   "s19-M15-TheWaveWranglers-vs-PrettyPenne-mid8317-1_de_dust2-2026-04-01_01-55-05.dem.zip",
			wantOK:     true,
			wantID:     "8317",
			wantMapIdx: 1,
		},
		{
			name:       "first map (index 0)",
			filename:   "s19-M13-Nightshades-vs-Wyverns-mid8275-0_de_nuke-2026-03-25_01-07-36.dem.zip",
			wantOK:     true,
			wantID:     "8275",
			wantMapIdx: 0,
		},
		{
			name:       "third map of a BO5",
			filename:   "s20-M01-Alpha-vs-Bravo-mid10000-4_de_ancient-2026-05-01_20-00-00.dem.zip",
			wantOK:     true,
			wantID:     "10000",
			wantMapIdx: 4,
		},
		{
			name:     "combine (old format) must be rejected",
			filename: "combine-contender-mid7272-0_de_mirage-2026-01-01_12-00-00.dem.zip",
			wantOK:   false,
		},
		{
			name:     "plain .dem (no zip) must be rejected",
			filename: "s19-M13-Foo-vs-Bar-mid8275-0_de_nuke-2026-03-25_01-07-36.dem",
			wantOK:   false,
		},
		{
			name:     "missing timestamp must be rejected",
			filename: "s19-M13-Foo-vs-Bar-mid8275-0_de_nuke.dem.zip",
			wantOK:   false,
		},
		{
			name:     "team name with hyphen must be rejected (would ambiguate -vs-/-mid)",
			filename: "s19-M13-Foo-Bar-vs-Baz-mid8275-0_de_nuke-2026-03-25_01-07-36.dem.zip",
			wantOK:   false,
		},
		{
			name:     "random file must be rejected",
			filename: "README.txt",
			wantOK:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, idx, ok := parseRegulationFilename(tc.filename)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if id != tc.wantID {
				t.Errorf("matchID=%q want %q", id, tc.wantID)
			}
			if idx != tc.wantMapIdx {
				t.Errorf("mapIndex=%d want %d", idx, tc.wantMapIdx)
			}
		})
	}
}
