package readstore

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func collectReverse(it ReverseIterator) []string {
	var out []string
	for it.Next() {
		out = append(out, string(it.Current()))
	}

	return out
}

func collectReverseEvent(t *testing.T, s *Store, prefix []byte, pin uint64) []string {
	t.Helper()

	it, err := NewReverseEventResolveIterator(s.DB(), prefix, pin)
	require.NoError(t, err)
	defer it.Close()

	out := collectReverse(it)

	require.NoError(t, it.Err())

	return out
}

// --- ReverseSliceIterator ---

func TestReverseSliceIterator_WalksSortedSliceBackwards(t *testing.T) {
	t.Parallel()

	it := NewReverseSliceIterator([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	defer it.Close()

	require.Equal(t, []string{"c", "b", "a"}, collectReverse(it))
	require.NoError(t, it.Err())
}

func TestReverseSliceIterator_SeekLE(t *testing.T) {
	t.Parallel()

	it := NewReverseSliceIterator([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	defer it.Close()

	require.True(t, it.SeekLE([]byte("b")))
	require.Equal(t, "b", string(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, "a", string(it.Current()))

	// Seek above the max lands on the max.
	require.True(t, it.SeekLE([]byte("z")))
	require.Equal(t, "c", string(it.Current()))

	// Seek below the min is exhaustion.
	require.False(t, it.SeekLE([]byte("0")))
	require.NoError(t, it.Err())
}

// --- ReverseAndIterator ---

func TestReverseAndIterator_IntersectsDescending(t *testing.T) {
	t.Parallel()

	left := newReverseAliasingIter("a", "b", "c", "z")
	right := newReverseAliasingIter("a", "b", "c")

	it := NewReverseAndIterator(left, right)
	defer it.Close()

	require.Equal(t, []string{"c", "b", "a"}, collectReverse(it))
	require.NoError(t, it.Err())
}

func TestReverseAndIterator_SeekLERepositionsAllChildren(t *testing.T) {
	t.Parallel()

	left := newReverseAliasingIter("a", "b", "c", "z")
	right := newReverseAliasingIter("a", "b", "c")

	it := NewReverseAndIterator(left, right)
	defer it.Close()

	require.Equal(t, []string{"c", "b", "a"}, collectReverse(it))

	// Absolute re-seek to "b" must yield "b" after the descending pass.
	require.True(t, it.SeekLE([]byte("b")))
	require.Equal(t, "b", string(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, "a", string(it.Current()))
	require.False(t, it.Next())
	require.NoError(t, it.Err())
}

// --- ReverseNotIterator ---

func TestReverseNotIterator_DifferenceDescending(t *testing.T) {
	t.Parallel()

	it := NewReverseNotIterator(
		newReverseAliasingIter("a", "b", "c", "d", "e"),
		newReverseAliasingIter("b", "d"),
	)
	defer it.Close()

	require.Equal(t, []string{"e", "c", "a"}, collectReverse(it))
	require.NoError(t, it.Err())
}

func TestReverseNotIterator_SeekLERepositionsAfterExhaustion(t *testing.T) {
	t.Parallel()

	it := NewReverseNotIterator(
		newReverseAliasingIter("a", "b", "c", "d", "e"),
		newReverseAliasingIter("b", "d"),
	)
	defer it.Close()

	require.Equal(t, []string{"e", "c", "a"}, collectReverse(it))

	require.True(t, it.SeekLE([]byte("c")))
	require.Equal(t, "c", string(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, "a", string(it.Current()))
	require.False(t, it.Next())
	require.NoError(t, it.Err())
}

// --- ReverseEventResolveIterator (point form) ---

func TestReverseEventResolveIterator_Descending(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v",
		ev{"a:1", 10, MetadataEventAdd},
		ev{"a:2", 10, MetadataEventAdd},
		ev{"a:2", 20, MetadataEventDel},
		ev{"a:3", 10, MetadataEventAdd},
	)

	require.Equal(t, []string{"a:3", "a:1"}, collectReverseEvent(t, s, prefix, 25),
		"descending live groups, a:2 resolved dead")
}

func TestReverseEventResolveIterator_SeekLEContract(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v",
		ev{"a:1", 10, MetadataEventAdd},
		ev{"a:2", 10, MetadataEventAdd},
		ev{"a:2", 20, MetadataEventDel},
		ev{"a:3", 10, MetadataEventAdd},
	)

	it, err := NewReverseEventResolveIterator(s.DB(), prefix, 25)
	require.NoError(t, err)
	defer it.Close()

	require.True(t, it.SeekLE([]byte("a:2")), "lands on the largest live entity <= target")
	require.Equal(t, "a:1", string(it.Current()), "a:2 is dead at pin 25")
	require.False(t, it.Next(), "nothing below a:1")

	require.True(t, it.SeekLE([]byte("a:3")), "re-seek upward after exhaustion")
	require.Equal(t, "a:3", string(it.Current()))

	require.True(t, it.SeekLE([]byte("a:1")), "repeatable seek onto a live entity")
	require.Equal(t, "a:1", string(it.Current()))

	require.NoError(t, it.Err())
}

func TestReverseEventResolveIterator_MultipleEventsPerGroup(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v",
		ev{"a:1", 10, MetadataEventAdd},
		ev{"a:1", 20, MetadataEventDel},
		ev{"a:1", 30, MetadataEventAdd},
		ev{"a:2", 10, MetadataEventAdd},
	)

	require.Equal(t, []string{"a:2", "a:1"}, collectReverseEvent(t, s, prefix, 35))
	require.Equal(t, []string{"a:2"}, collectReverseEvent(t, s, prefix, 25), "a:1 dead between its DEL and re-ADD")
}

// --- ReversePebbleAccountPrefixIterator ---

func TestPebbleReverseAccountPrefixIterator_Bounded(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	prefix := make([]byte, 2+dal.LedgerNameFixedSize)
	prefix[0] = dal.ZoneAttributes
	prefix[1] = dal.SubAttrVolume
	copy(prefix[2:], "l")

	for _, addr := range []string{"a:1", "a:2", "b:3"} {
		key := append(append(append([]byte{}, prefix...), addr...), dal.CanonicalKeySepVolume)
		require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
	}

	it, err := NewPebbleReverseAccountPrefixIterator(s.DB(), "l", "a:")
	require.NoError(t, err)
	defer it.Close()

	require.Equal(t, []string{"a:2", "a:1"}, collectReverse(it), "b:3 excluded by the address prefix bound")

	require.True(t, it.SeekLE([]byte("a:2")))
	require.Equal(t, "a:2", string(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, "a:1", string(it.Current()))

	require.True(t, it.SeekLE([]byte("b:9")), "clamps to the max address within the prefix")
	require.Equal(t, "a:2", string(it.Current()))
	require.False(t, it.SeekLE([]byte("a:0")), "below the prefix bound")
	require.NoError(t, it.Err())
}

// --- ReverseBitsetIterator ---

func u64be(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)

	return b
}

func collectReverseBitsetIDs(it *ReverseBitsetIterator) []uint64 {
	var out []uint64
	for it.Next() {
		out = append(out, binary.BigEndian.Uint64(it.Current()))
	}

	return out
}

func TestReverseBitsetIterator_EmitsSetBitsDescending(t *testing.T) {
	t.Parallel()

	bs := &bitset.Bitset{}
	want := []uint64{0, 3, 63, 64, 65, 200, 4095}
	for _, id := range want {
		bs.Set(id)
	}

	var reversed []uint64
	for _, w := range slices.Backward(want) {
		reversed = append(reversed, w)
	}

	require.Equal(t, reversed, collectReverseBitsetIDs(NewReverseBitsetIterator(bs)))
}

func TestReverseBitsetIterator_SeekLE(t *testing.T) {
	t.Parallel()

	bs := &bitset.Bitset{}
	for _, id := range []uint64{1, 5, 64, 130} {
		bs.Set(id)
	}

	it := NewReverseBitsetIterator(bs)
	require.True(t, it.SeekLE(u64be(130)))
	require.Equal(t, uint64(130), binary.BigEndian.Uint64(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, uint64(64), binary.BigEndian.Uint64(it.Current()))

	// Seek into a gap lands on the next lower bit (across a word boundary).
	gap := NewReverseBitsetIterator(bs)
	require.True(t, gap.SeekLE(u64be(70)))
	require.Equal(t, uint64(64), binary.BigEndian.Uint64(gap.Current()))

	// Seek past the last word clamps to the whole last word.
	pastEnd := NewReverseBitsetIterator(bs)
	require.True(t, pastEnd.SeekLE(u64be(500)))
	require.Equal(t, uint64(130), binary.BigEndian.Uint64(pastEnd.Current()))

	// Seek below the lowest set bit is exhausted.
	past := NewReverseBitsetIterator(bs)
	require.False(t, past.SeekLE(u64be(0)))
}

func TestReverseBitsetIterator_Empty(t *testing.T) {
	t.Parallel()

	require.Empty(t, collectReverseBitsetIDs(NewReverseBitsetIterator(&bitset.Bitset{})))
	require.Empty(t, collectReverseBitsetIDs(NewReverseBitsetIterator(nil)))
}
