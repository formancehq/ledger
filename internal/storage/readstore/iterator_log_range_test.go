package readstore

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func drainLogRange(t *testing.T, it EntityIterator) []uint64 {
	t.Helper()

	var out []uint64
	for it.Next() {
		out = append(out, binary.BigEndian.Uint64(it.Current()))
	}
	require.NoError(t, it.Err())

	return out
}

func seedLogRange(t *testing.T, s *Store, ledger string, ids ...uint64) {
	t.Helper()

	kb := dal.NewKeyBuilder()
	for _, id := range ids {
		require.NoError(t, s.DB().Set(LedgerLogKey(kb, ledger, id), nil, pebble.NoSync))
	}
}

// The ledger-log range leaf must honour the half-open [min, max) bounds, and —
// critically — an absent upper bound must be closed by the successor of the
// prefix (not prefix + eight 0xff bytes), so a MaxUint64 log ID stays included.
func TestLedgerLogRangeIterator_HalfOpenBoundsAndMaxUint64(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	const ledger = "l"

	seedLogRange(t, s, ledger, 0, 1, math.MaxUint64-1, math.MaxUint64)

	kb := dal.NewKeyBuilder()

	t.Run("unbounded includes MaxUint64", func(t *testing.T) {
		it, err := NewLedgerLogRangeIterator(s.DB(), kb, ledger, nil, nil)
		require.NoError(t, err)
		defer it.Close()

		require.Equal(t, []uint64{0, 1, math.MaxUint64 - 1, math.MaxUint64}, drainLogRange(t, it))
	})

	t.Run("exclusive MaxUint64 upper excludes it", func(t *testing.T) {
		it, err := NewLedgerLogRangeIterator(s.DB(), kb, ledger, txIDBytes(0), txIDBytes(math.MaxUint64))
		require.NoError(t, err)
		defer it.Close()

		require.Equal(t, []uint64{0, 1, math.MaxUint64 - 1}, drainLogRange(t, it))
	})

	t.Run("bounded half-open range", func(t *testing.T) {
		it, err := NewLedgerLogRangeIterator(s.DB(), kb, ledger, txIDBytes(1), txIDBytes(math.MaxUint64-1))
		require.NoError(t, err)
		defer it.Close()

		require.Equal(t, []uint64{1}, drainLogRange(t, it))
	})

	t.Run("singleton MaxUint64 lower bound", func(t *testing.T) {
		it, err := NewLedgerLogRangeIterator(s.DB(), kb, ledger, txIDBytes(math.MaxUint64), nil)
		require.NoError(t, err)
		defer it.Close()

		require.Equal(t, []uint64{math.MaxUint64}, drainLogRange(t, it))
	})
}

// The leaf must be a true absolute-seek citizen: a failed seek at or above the
// floor returns false without a Pebble seek, a backward seek repositions, a
// seek after exhaustion works, and reaching MaxUint64 must not wrap around to
// the smallest log ID.
func TestLedgerLogRangeIterator_SeekContractAndNoWrapAfterMax(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()
	const ledger = "l"

	seedLogRange(t, s, ledger, 3, 5, math.MaxUint64)

	t.Run("bounded floor and backward seek", func(t *testing.T) {
		it, err := NewLedgerLogRangeIterator(s.DB(), kb, ledger, txIDBytes(1), txIDBytes(10))
		require.NoError(t, err)
		defer it.Close()

		require.False(t, it.SeekGE(txIDBytes(7)), "no entity >= 7 within [1,10)")
		require.False(t, it.SeekGE(txIDBytes(8)), "covered by the floor")

		require.True(t, it.SeekGE(txIDBytes(4)), "backward seek repositions")
		require.Equal(t, uint64(5), binary.BigEndian.Uint64(it.Current()))
		require.False(t, it.Next())
		require.NoError(t, it.Err())

		require.True(t, it.SeekGE(txIDBytes(3)), "reposition after exhaustion")
		require.Equal(t, uint64(3), binary.BigEndian.Uint64(it.Current()))
	})

	t.Run("no wraparound after MaxUint64", func(t *testing.T) {
		it, err := NewLedgerLogRangeIterator(s.DB(), kb, ledger, nil, nil)
		require.NoError(t, err)
		defer it.Close()

		require.True(t, it.SeekGE(txIDBytes(5)))
		require.Equal(t, uint64(5), binary.BigEndian.Uint64(it.Current()))
		require.True(t, it.Next())
		require.Equal(t, uint64(math.MaxUint64), binary.BigEndian.Uint64(it.Current()))
		require.False(t, it.Next(), "must not wrap to the smallest log ID")
		require.False(t, it.Next())
		require.NoError(t, it.Err())

		require.True(t, it.SeekGE(txIDBytes(4)), "re-seek after exhaustion past the maximum")
		require.Equal(t, uint64(5), binary.BigEndian.Uint64(it.Current()))
	})
}
