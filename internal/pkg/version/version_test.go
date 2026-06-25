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
