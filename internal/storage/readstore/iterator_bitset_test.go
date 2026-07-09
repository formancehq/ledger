package readstore_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func be8(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)

	return b
}

func collectBitsetIDs(it *readstore.BitsetIterator) []uint64 {
	var out []uint64
	for it.Next() {
		out = append(out, binary.BigEndian.Uint64(it.Current()))
	}

	return out
}

func TestBitsetIterator_EmitsSetBitsAscending(t *testing.T) {
	t.Parallel()

	bs := &bitset.Bitset{}
	want := []uint64{0, 3, 63, 64, 65, 200, 4095}
	for _, id := range want {
		bs.Set(id)
	}

	require.Equal(t, want, collectBitsetIDs(readstore.NewBitsetIterator(bs)))
}

func TestBitsetIterator_Empty(t *testing.T) {
	t.Parallel()

	require.Empty(t, collectBitsetIDs(readstore.NewBitsetIterator(&bitset.Bitset{})))
	require.Empty(t, collectBitsetIDs(readstore.NewBitsetIterator(nil)))
}

func TestBitsetIterator_SeekGE(t *testing.T) {
	t.Parallel()

	bs := &bitset.Bitset{}
	for _, id := range []uint64{1, 5, 64, 130} {
		bs.Set(id)
	}

	// Seek onto an exact set bit, then keep iterating.
	it := readstore.NewBitsetIterator(bs)
	require.True(t, it.SeekGE(be8(5)))
	require.Equal(t, uint64(5), binary.BigEndian.Uint64(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, uint64(64), binary.BigEndian.Uint64(it.Current()))

	// Seek into a gap lands on the next set bit (across a word boundary).
	gap := readstore.NewBitsetIterator(bs)
	require.True(t, gap.SeekGE(be8(6)))
	require.Equal(t, uint64(64), binary.BigEndian.Uint64(gap.Current()))

	// Seek beyond the last set bit is exhausted.
	past := readstore.NewBitsetIterator(bs)
	require.False(t, past.SeekGE(be8(131)))
}
