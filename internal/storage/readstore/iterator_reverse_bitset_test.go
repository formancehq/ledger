package readstore_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestReverseBitsetIterator_BoundariesAndAbsoluteSeek(t *testing.T) {
	t.Parallel()
	bs := &bitset.Bitset{}
	for _, id := range []uint64{0, 3, 63, 64, 65, 200, 4095} {
		bs.Set(id)
	}
	it := readstore.NewReverseBitsetIterator(bs)
	defer it.Close()
	var got []uint64
	for it.Next() {
		got = append(got, binary.BigEndian.Uint64(it.Current()))
	}
	require.Equal(t, []uint64{4095, 200, 65, 64, 63, 3, 0}, got)
	require.False(t, it.Next())
	for _, tc := range []struct{ target, want uint64 }{
		{math.MaxUint64, 4095}, {64, 64}, {64, 64}, {199, 65}, {63, 63}, {62, 3}, {4095, 4095}, {0, 0},
	} {
		require.True(t, it.Seek(be8(tc.target)), "target %d", tc.target)
		require.Equal(t, tc.want, binary.BigEndian.Uint64(it.Current()))
	}
	require.False(t, it.Next())
	require.True(t, it.Seek(be8(65)))
	require.True(t, it.Next())
	require.Equal(t, uint64(64), binary.BigEndian.Uint64(it.Current()))
	require.True(t, it.Seek(nil))
	require.Equal(t, uint64(0), binary.BigEndian.Uint64(it.Current()))
	require.NoError(t, it.Err())
}

func TestReverseBitsetIterator_EmptyAndFailedSeek(t *testing.T) {
	t.Parallel()
	for _, bs := range []*bitset.Bitset{nil, {}} {
		it := readstore.NewReverseBitsetIterator(bs)
		require.False(t, it.Next())
		require.False(t, it.Seek(be8(math.MaxUint64)))
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		it.Close()
	}
	bs := &bitset.Bitset{}
	bs.Set(64)
	it := readstore.NewReverseBitsetIterator(bs)
	defer it.Close()
	require.False(t, it.Seek(be8(63)))
	require.False(t, it.Next())
	require.True(t, it.Seek(be8(64)))
	require.Equal(t, uint64(64), binary.BigEndian.Uint64(it.Current()))
	require.False(t, it.Next())
	require.NoError(t, it.Err())
}
