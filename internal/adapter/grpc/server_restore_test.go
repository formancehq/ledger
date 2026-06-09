package grpc

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSafeStagingPath_Valid(t *testing.T) {
	t.Parallel()

	staging := filepath.Join(t.TempDir(), "staging")

	cases := []struct {
		name string
		in   string
	}{
		{"plain filename", "000123.sst"},
		{"nested forward-slash path", "subdir/000123.sst"},
		{"deep nesting", "a/b/c/d/file.sst"},
		{"dot-prefixed file", ".hidden"},
		{"double dot in middle (not traversal)", "a..b/c.sst"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := safeStagingPath(staging, tc.in)
			require.NoError(t, err)

			rel, err := filepath.Rel(staging, got)
			require.NoError(t, err)
			require.False(t, strings.HasPrefix(rel, ".."),
				"resolved path %q must stay under %q (rel=%q)", got, staging, rel)
		})
	}
}

func TestSafeStagingPath_Rejected(t *testing.T) {
	t.Parallel()

	staging := filepath.Join(t.TempDir(), "staging")

	cases := []struct {
		name       string
		in         string
		wantSubstr string
	}{
		{"empty", "", "empty filename"},
		{"single dot-dot", "..", "escapes"},
		{"parent traversal", "../etc/passwd", "escapes"},
		{"deep traversal", "a/../../etc/passwd", "escapes"},
		{"unix absolute path", "/etc/passwd", "absolute path"},
		{"absolute via leading slash + traversal", "/../etc/passwd", "absolute path"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := safeStagingPath(staging, tc.in)
			require.Error(t, err, "expected rejection for %q", tc.in)
			require.Contains(t, err.Error(), tc.wantSubstr,
				"error %q should mention %q", err.Error(), tc.wantSubstr)
		})
	}
}

// TestSafeStagingPath_DefenseInDepthRel ensures the filepath.Rel guard
// would still reject even if the prefix check missed an edge case. We
// can't easily construct an input that bypasses the prefix check but
// fails Rel, so this test asserts the contract: every accepted name
// has a Rel that stays under the staging root.
func TestSafeStagingPath_DefenseInDepthRel(t *testing.T) {
	t.Parallel()

	staging := filepath.Join(t.TempDir(), "staging")

	// A normal nested file.
	dest, err := safeStagingPath(staging, "checkpoints/0/000123.sst")
	require.NoError(t, err)

	rel, err := filepath.Rel(staging, dest)
	require.NoError(t, err)
	require.Equal(t, filepath.Join("checkpoints", "0", "000123.sst"), rel)
}
