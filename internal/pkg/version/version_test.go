package version

import (
	"runtime"
	"testing"
)

// TestGet mutates the package-level build vars, so it deliberately does NOT call
// t.Parallel(): the vars are process-global and a parallel reader would race
// (the -race detector flags it). It restores the originals on cleanup.
func TestGet(t *testing.T) {
	prevV, prevC, prevD := Version, Commit, BuildDate
	t.Cleanup(func() { Version, Commit, BuildDate = prevV, prevC, prevD })

	Version, Commit, BuildDate = "v3.1.0", "abc1234", "2026-06-19T00:00:00Z"

	got := Get()
	if got.Version != "v3.1.0" || got.Commit != "abc1234" || got.BuildDate != "2026-06-19T00:00:00Z" {
		t.Fatalf("Get() did not reflect overridden vars: %+v", got)
	}
	if got.GoVersion != runtime.Version() {
		t.Fatalf("GoVersion = %q, want %q", got.GoVersion, runtime.Version())
	}
}

func TestServiceVersion(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		info Info
		want string
	}{
		{name: "release", info: Info{Version: "3.0.0", Commit: "abc1234"}, want: "3.0.0+abc1234"},
		{name: "snapshot embeds the commit", info: Info{Version: "3.0.0-SNAPSHOT-abc1234", Commit: "abc1234"}, want: "3.0.0-SNAPSHOT-abc1234"},
		{name: "dev build", info: Info{Version: "dev", Commit: "unknown"}, want: "dev"},
		{name: "empty commit", info: Info{Version: "3.0.0"}, want: "3.0.0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := test.info.ServiceVersion(); got != test.want {
				t.Fatalf("ServiceVersion() = %q, want %q", got, test.want)
			}
		})
	}
}
