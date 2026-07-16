package semver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		want    Version
		wantErr bool
	}{
		{"1.0.0", Version{1, 0, 0}, false},
		{"0.0.0", Version{0, 0, 0}, false},
		{"10.20.30", Version{10, 20, 30}, false},
		{"1.0", Version{}, true},
		{"1", Version{}, true},
		{"", Version{}, true},
		{"1.0.0.0", Version{}, true},
		{"a.b.c", Version{}, true},
		{"1..0", Version{}, true},
		// Non-canonical (leading zeros) must be rejected: the raw string is a
		// storage key, so "01.0.0" and "1.0.0" would diverge the projection.
		{"01.0.0", Version{}, true},
		{"1.00.0", Version{}, true},
		{"1.0.007", Version{}, true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()

			got, err := Parse(tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
				require.Equal(t, tc.input, got.String())
			}
		})
	}
}

func TestParsePartial(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     string
		wantMajor uint32
		wantMinor uint32
		wantPatch uint32
		wantDepth int
		wantErr   bool
	}{
		{"1", 1, 0, 0, 1, false},
		{"1.2", 1, 2, 0, 2, false},
		{"1.2.3", 1, 2, 3, 3, false},
		{"", 0, 0, 0, 0, true},
		{"1.2.3.4", 0, 0, 0, 0, true},
		{"a", 0, 0, 0, 0, true},
		{"1.b", 0, 0, 0, 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()

			major, minor, patch, depth, err := ParsePartial(tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantMajor, major)
				require.Equal(t, tc.wantMinor, minor)
				require.Equal(t, tc.wantPatch, patch)
				require.Equal(t, tc.wantDepth, depth)
			}
		})
	}
}
