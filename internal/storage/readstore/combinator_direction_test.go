package readstore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Direction-parity suite for the shared combinators (EN-1966).
//
// And, Or, Not and Slice are single implementations parameterized by
// Direction, so the thing worth testing is that each one actually USES the
// direction it was given: a combinator that ignored D, or mirrored its
// comparison wrongly, would still pass an ascending-only suite. Every case
// below builds the same composition in both directions over the same fixture
// and asserts the descending traversal is the exact reverse of the ascending
// one.
//
// The leaves are slice-backed so the suite is hermetic and says nothing about
// Pebble; the physical leaves keep their own tests.

func entities(vals ...string) [][]byte {
	out := make([][]byte, len(vals))
	for i, v := range vals {
		out[i] = []byte(v)
	}

	return out
}

func drain(t *testing.T, it nextable) string {
	t.Helper()

	var got []string
	for it.Next() {
		got = append(got, string(it.Current()))
	}

	return strings.Join(got, ",")
}

func reversed(csv string) string {
	if csv == "" {
		return ""
	}

	parts := strings.Split(csv, ",")
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}

	return strings.Join(parts, ",")
}

// directionCase builds one composition in both directions over the same
// ascending fixtures.
type directionCase struct {
	name string
	asc  func() EntityIterator
	desc func() ReverseIterator
	want string // ascending traversal; descending must be its reverse
}

func directionCases() []directionCase {
	return []directionCase{
		{
			name: "Slice",
			asc:  func() EntityIterator { return NewSliceIterator(entities("a", "b", "c", "d")) },
			desc: func() ReverseIterator { return NewReverseSliceIterator(entities("a", "b", "c", "d")) },
			want: "a,b,c,d",
		},
		{
			name: "And",
			asc: func() EntityIterator {
				return NewAndIterator(
					NewSliceIterator(entities("a", "b", "c", "e")),
					NewSliceIterator(entities("b", "c", "d", "e")),
				)
			},
			desc: func() ReverseIterator {
				return NewReverseAndIterator(
					NewReverseSliceIterator(entities("a", "b", "c", "e")),
					NewReverseSliceIterator(entities("b", "c", "d", "e")),
				)
			},
			want: "b,c,e",
		},
		{
			name: "Or",
			asc: func() EntityIterator {
				return NewOrIterator(
					NewSliceIterator(entities("a", "c", "e")),
					NewSliceIterator(entities("b", "c", "d")),
				)
			},
			desc: func() ReverseIterator {
				return NewReverseOrIterator(
					NewReverseSliceIterator(entities("a", "c", "e")),
					NewReverseSliceIterator(entities("b", "c", "d")),
				)
			},
			want: "a,b,c,d,e",
		},
		{
			name: "Not",
			asc: func() EntityIterator {
				return NewNotIterator(
					NewSliceIterator(entities("a", "b", "c", "d", "e")),
					NewSliceIterator(entities("b", "d")),
				)
			},
			desc: func() ReverseIterator {
				return NewReverseNotIterator(
					NewReverseSliceIterator(entities("a", "b", "c", "d", "e")),
					NewReverseSliceIterator(entities("b", "d")),
				)
			},
			want: "a,c,e",
		},
		{
			name: "AndOverNot",
			asc: func() EntityIterator {
				return NewAndIterator(
					NewSliceIterator(entities("a", "b", "c", "d", "e")),
					NewNotIterator(
						NewSliceIterator(entities("a", "b", "c", "d", "e")),
						NewSliceIterator(entities("a", "e")),
					),
				)
			},
			desc: func() ReverseIterator {
				return NewReverseAndIterator(
					NewReverseSliceIterator(entities("a", "b", "c", "d", "e")),
					NewReverseNotIterator(
						NewReverseSliceIterator(entities("a", "b", "c", "d", "e")),
						NewReverseSliceIterator(entities("a", "e")),
					),
				)
			},
			want: "b,c,d",
		},
	}
}

func TestCombinators_DescendingIsReversedAscending(t *testing.T) {
	t.Parallel()

	for _, tc := range directionCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			asc := tc.asc()
			t.Cleanup(asc.Close)
			require.Equal(t, tc.want, drain(t, asc), "ascending traversal")
			require.NoError(t, asc.Err())

			desc := tc.desc()
			t.Cleanup(desc.Close)
			require.Equal(t, reversed(tc.want), drain(t, desc), "descending traversal")
			require.NoError(t, desc.Err())
		})
	}
}

// TestCombinators_SeekIsAbsoluteInBothDirections pins the absolute-seek
// contract on the shared combinators: a seek is computed from target alone,
// so repeating it yields the same entity without consuming it, and it still
// repositions after the iterator has been exhausted.
func TestCombinators_SeekIsAbsoluteInBothDirections(t *testing.T) {
	t.Parallel()

	for _, tc := range directionCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Ascending: seek "c" lands on the first emitted entity >= "c".
			asc := tc.asc()
			t.Cleanup(asc.Close)
			ascWant := firstAtOrAfter(strings.Split(tc.want, ","), "c", true)

			require.Equal(t, ascWant != "", asc.Seek([]byte("c")))

			if ascWant != "" {
				require.Equal(t, ascWant, string(asc.Current()))
				// Idempotent and non-consuming.
				require.True(t, asc.Seek([]byte("c")))
				require.Equal(t, ascWant, string(asc.Current()))
			}

			// Well-defined after exhaustion: drain, then re-seek.
			for asc.Next() {
			}

			require.NoError(t, asc.Err())
			require.Equal(t, ascWant != "", asc.Seek([]byte("c")))

			// Descending: seek "c" lands on the last emitted entity <= "c".
			desc := tc.desc()
			t.Cleanup(desc.Close)
			descWant := firstAtOrAfter(strings.Split(tc.want, ","), "c", false)

			require.Equal(t, descWant != "", desc.Seek([]byte("c")))

			if descWant != "" {
				require.Equal(t, descWant, string(desc.Current()))
				require.True(t, desc.Seek([]byte("c")))
				require.Equal(t, descWant, string(desc.Current()))
			}

			for desc.Next() {
			}

			require.NoError(t, desc.Err())
			require.Equal(t, descWant != "", desc.Seek([]byte("c")))
		})
	}
}

// firstAtOrAfter returns the entity a seek to target must land on: the
// smallest result >= target ascending, the largest result <= target
// descending. Empty means the seek must fail.
func firstAtOrAfter(want []string, target string, ascending bool) string {
	best := ""

	for _, v := range want {
		if v == "" {
			continue
		}

		if ascending && v >= target {
			return v
		}

		if !ascending && v <= target {
			best = v
		}
	}

	if ascending {
		return ""
	}

	return best
}
