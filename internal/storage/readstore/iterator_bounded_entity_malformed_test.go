package readstore

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestBoundedEntityIterator_RejectsInvalidWidths(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	for _, tc := range []struct {
		name         string
		entityLen    int
		lower, upper []byte
	}{
		{name: "zero entity width", entityLen: 0},
		{name: "negative entity width", entityLen: -1},
		{name: "empty lower", entityLen: 2, lower: []byte{}},
		{name: "short lower", entityLen: 2, lower: []byte{1}},
		{name: "long lower", entityLen: 2, lower: []byte{1, 2, 3}},
		{name: "empty upper", entityLen: 2, upper: []byte{}},
		{name: "short upper", entityLen: 2, upper: []byte{1}},
		{name: "long upper", entityLen: 2, upper: []byte{1, 2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			it, err := NewBoundedEntityIterator(s.DB(), []byte{0x70}, tc.lower, tc.upper, tc.entityLen)
			if it != nil {
				t.Cleanup(it.Close)
			}
			require.ErrorContains(t, err, "invariant: BoundedEntityIterator")
			switch {
			case tc.entityLen <= 0:
				require.ErrorContains(t, err, "entityLen")
			case tc.lower != nil:
				require.ErrorContains(t, err, "lower bound length")
			default:
				require.ErrorContains(t, err, "upper bound length")
			}
			require.Nil(t, it)
		})
	}
}

// A malformed row must fail the query instead of being skipped, truncated to
// another entity, or cached as proof of clean exhaustion. Keep independent
// triggers for initial positioning, sequential advancement and absolute seek.
func TestBoundedEntityIterator_MalformedSuffixLatchesError(t *testing.T) {
	t.Parallel()

	for _, malformed := range []struct {
		name   string
		suffix []byte
	}{
		{name: "short", suffix: []byte{1}},
		{name: "long", suffix: []byte{1, 0, 1}},
	} {
		for _, trigger := range []string{"first Next", "subsequent Next", "Seek"} {
			t.Run(malformed.name+"/"+trigger, func(t *testing.T) {
				t.Parallel()

				s := newTestStore(t)
				prefix := []byte{0x70}
				seed := func(suffix []byte) {
					t.Helper()
					key := append(append([]byte(nil), prefix...), suffix...)
					require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
				}
				if trigger != "first Next" {
					seed([]byte{0, 1})
				}
				seed(malformed.suffix)
				seed([]byte{2, 0}) // Valid later row must not hide corruption.

				it, err := NewBoundedEntityIterator(s.DB(), prefix, nil, nil, 2)
				require.NoError(t, err)
				t.Cleanup(it.Close)

				switch trigger {
				case "first Next":
					require.False(t, it.Next(), "malformed first row must not be skipped or truncated")
				case "subsequent Next":
					require.True(t, it.Next())
					require.Equal(t, []byte{0, 1}, it.Current())
					require.False(t, it.Next(), "malformed following row must not be skipped or truncated")
				case "Seek":
					require.False(t, it.Seek([]byte{3, 0}))
					require.NoError(t, it.Err())
					require.True(t, it.floor.set, "clean seek past the final entity establishes a floor")
					require.False(t, it.Seek([]byte{0, 2}), "seek must reject malformed row before valid entity")
				}

				failure := it.Err()
				require.ErrorContains(t, failure, "invariant: BoundedEntityIterator")
				require.ErrorContains(t, failure, "key suffix length")
				require.False(t, it.floor.set, "corruption is not proof of clean exhaustion")
				require.False(t, it.Next())
				require.ErrorIs(t, it.Err(), failure)
				require.False(t, it.Seek([]byte{0, 0}), "backward seek must retain corruption failure")
				require.ErrorIs(t, it.Err(), failure)
				require.False(t, it.Seek([]byte{2, 0}), "seek must not recover by skipping malformed row")
				require.ErrorIs(t, it.Err(), failure)
				require.False(t, it.floor.set, "failed operations must not manufacture an exhaustion floor")
			})
		}
	}
}
