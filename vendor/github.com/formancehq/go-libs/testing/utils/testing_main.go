package utils

import (
	"flag"
	"fmt"
	"os"
	"slices"
)

// TestingTForMain implements require.TestingT and potentially other alternative T interface
// This allow to use the same tooling on func TestMain(t *testing.M) and TestXXX(t *testing.T)
type TestingTForMain struct {
	cleanup []func()
}

func (t *TestingTForMain) Helper() {}

func (t *TestingTForMain) Errorf(format string, args ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, format, args...)
}

func (t *TestingTForMain) FailNow() {
	t.callCleanup()
	os.Exit(1)
}

func (t *TestingTForMain) Cleanup(f func()) {
	t.cleanup = append(t.cleanup, f)
}

func (t *TestingTForMain) callCleanup() {
	cleanup := t.cleanup
	slices.Reverse(cleanup)
	for _, cleanup := range cleanup {
		cleanup()
	}
}

func (t *TestingTForMain) close() {
	t.callCleanup()
}

func WithTestMain(fn func(main *TestingTForMain) int) {
	flag.Parse()

	t := &TestingTForMain{}
	code := fn(t)
	t.close()

	os.Exit(code)
}
