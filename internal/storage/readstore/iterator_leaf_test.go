package readstore

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// RangeIterator drains [lower, upper) forward; SeekGE is unimplementable on
// the raw scan (rows surface in (value, entity) order across buckets) and
// must fail the query loudly instead of mis-seeking.
func TestRangeIterator_DrainsForwardAndRefusesSeek(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	for _, id := range []uint64{1, 2, 3} {
		require.NoError(t, s.DB().Set(AccountTxKey(kb, PrefixAccountTx, "l", "acc:1", id), nil, pebble.NoSync))
	}

	prefix := AccountTxPrefix(dal.NewKeyBuilder(), PrefixAccountTx, "l", "acc:1")
	lower := append(append([]byte{}, prefix...), txIDBytes(1)...)
	upper := append(append([]byte{}, prefix...), txIDBytes(3)...)

	it, err := NewRangeIterator(s.DB(), lower, upper, len(prefix), 8)
	require.NoError(t, err)
	defer it.Close()

	var got []uint64
	for it.Next() {
		got = append(got, binary.BigEndian.Uint64(it.Current()))
	}
	require.Equal(t, []uint64{1, 2}, got, "upper bound is exclusive")
	require.NoError(t, it.Err())

	require.False(t, it.SeekGE(txIDBytes(1)))
	require.ErrorIs(t, it.Err(), errInvariantRangeIteratorSeek)
}
