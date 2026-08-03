package readstore

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestSeekFloor(t *testing.T) {
	t.Parallel()

	var f seekFloor

	require.False(t, f.covers([]byte("a")), "unset floor covers nothing")

	f.fail([]byte("x"), errors.New("checksum mismatch"))
	require.False(t, f.covers([]byte("x")), "an I/O-failed seek proves nothing")

	f.fail([]byte("m"), nil)
	require.True(t, f.covers([]byte("m")), "failure bound is inclusive")
	require.True(t, f.covers([]byte("z")))
	require.False(t, f.covers([]byte("a")), "targets below the bound must re-seek")

	f.fail([]byte("d"), nil)
	require.True(t, f.covers([]byte("d")), "a lower failure tightens the bound")
	require.False(t, f.covers([]byte("c")))
}

func TestSeekCeil(t *testing.T) {
	t.Parallel()

	var c seekCeil

	require.False(t, c.covers([]byte("a")), "unset ceil covers nothing")

	c.fail([]byte("b"), errors.New("checksum mismatch"))
	require.False(t, c.covers([]byte("b")), "an I/O-failed seek proves nothing")

	c.fail([]byte("m"), nil)
	require.True(t, c.covers([]byte("m")), "failure bound is inclusive")
	require.True(t, c.covers([]byte("a")))
	require.False(t, c.covers([]byte("z")), "targets above the bound must re-seek")

	c.fail([]byte("t"), nil)
	require.True(t, c.covers([]byte("t")), "a higher failure tightens the bound")
	require.False(t, c.covers([]byte("u")))
}

// A floored leaf must still honor the absolute-seek contract: failed seeks at
// or above the proven bound return false (without re-seeking Pebble), while a
// seek below it repositions normally — including after exhaustion.
func TestPrefixIterator_SeekFloorKeepsRepositioning(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	for _, id := range []uint64{1, 2, 3} {
		require.NoError(t, s.DB().Set(AccountTxKey(kb, PrefixAccountTx, "l", "acc:1", id), nil, pebble.NoSync))
	}

	prefix := AccountTxPrefix(dal.NewKeyBuilder(), PrefixAccountTx, "l", "acc:1")

	it, err := NewPrefixIterator(s.DB(), prefix, len(prefix), 8)
	require.NoError(t, err)
	defer it.Close()

	require.False(t, it.SeekGE(txIDBytes(5)), "no entity >= 5")
	require.False(t, it.Next(), "failed seek leaves the iterator exhausted")
	require.False(t, it.SeekGE(txIDBytes(7)), "covered by the floor")

	require.True(t, it.SeekGE(txIDBytes(2)), "below the floor: real reposition")
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(it.Current()))
	require.False(t, it.Next())

	require.True(t, it.SeekGE(txIDBytes(0)), "reposition after Next-exhaustion")
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(it.Current()))
	require.NoError(t, it.Err())
}

// The descending mirror: failed SeekLEs at or below the proven ceil return
// false without a Pebble seek, while a seek above it repositions normally —
// including after exhaustion.
func TestReversePrefixIterator_SeekCeilKeepsRepositioning(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	for _, id := range []uint64{2, 3, 4} {
		require.NoError(t, s.DB().Set(AccountTxKey(kb, PrefixAccountTx, "l", "acc:1", id), nil, pebble.NoSync))
	}

	prefix := AccountTxPrefix(dal.NewKeyBuilder(), PrefixAccountTx, "l", "acc:1")

	it, err := NewReversePrefixIterator(s.DB(), prefix, len(prefix), 8)
	require.NoError(t, err)
	defer it.Close()

	require.False(t, it.SeekLE(txIDBytes(1)), "no entity <= 1")
	require.False(t, it.Next(), "failed seek leaves the iterator exhausted")
	require.False(t, it.SeekLE(txIDBytes(0)), "covered by the ceil")

	require.True(t, it.SeekLE(txIDBytes(3)), "above the ceil: real reposition")
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(it.Current()))
	require.False(t, it.Next())

	require.True(t, it.SeekLE(txIDBytes(9)), "reposition after Next-exhaustion")
	require.Equal(t, uint64(4), binary.BigEndian.Uint64(it.Current()))
	require.NoError(t, it.Err())
}

// PebbleReverseTxIterator.SeekLE has three exhaustion branches (Prev fails
// after a positioned SeekGE, Last fails on an empty view, and the scan-back
// loop running out); the first two must record the ceil and stay re-seekable.
func TestPebbleReverseTxIterator_SeekLERepositioning(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	for _, id := range []uint64{5, 7} {
		require.NoError(t, s.DB().Set(append(txAttributeCode("l"), txIDBytes(id)...), nil, pebble.NoSync))
	}

	it, err := NewPebbleReverseTxIterator(s.DB(), "l")
	require.NoError(t, err)
	defer it.Close()

	// Prev()-fails branch: SeekGE lands on tx 5's key, stepping back leaves the
	// bounded range.
	require.False(t, it.SeekLE(txIDBytes(4)), "no entity <= 4")
	require.False(t, it.SeekLE(txIDBytes(4)), "covered by the ceil")

	require.True(t, it.SeekLE(txIDBytes(6)), "above the ceil: real reposition")
	require.Equal(t, uint64(5), binary.BigEndian.Uint64(it.Current()))
	require.False(t, it.Next())

	require.True(t, it.SeekLE(txIDBytes(7)), "reposition after Next-exhaustion")
	require.Equal(t, uint64(7), binary.BigEndian.Uint64(it.Current()))
	require.NoError(t, it.Err())

	// Last()-fails branch: an empty view records the ceil on the first seek,
	// covering every later target below it.
	empty, err := NewPebbleReverseTxIterator(s.DB(), "empty")
	require.NoError(t, err)
	defer empty.Close()

	require.False(t, empty.SeekLE(txIDBytes(9)), "empty view")
	require.False(t, empty.SeekLE(txIDBytes(3)), "covered by the ceil")
	require.NoError(t, empty.Err())
}

// The floor short-circuit and reposition-after-exhaustion behavior is
// mechanically identical across the forward Pebble leaves; the three tests
// below mirror TestPrefixIterator_SeekFloorKeepsRepositioning for the
// remaining leaves.

func TestPebbleTxIterator_SeekFloorKeepsRepositioning(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	for _, id := range []uint64{1, 2, 3} {
		require.NoError(t, s.DB().Set(append(txAttributeCode("l"), txIDBytes(id)...), nil, pebble.NoSync))
	}

	it, err := NewPebbleTxIterator(s.DB(), "l")
	require.NoError(t, err)
	defer it.Close()

	require.False(t, it.SeekGE(txIDBytes(5)), "no entity >= 5")
	require.False(t, it.Next(), "failed seek leaves the iterator exhausted")
	require.False(t, it.SeekGE(txIDBytes(7)), "covered by the floor")

	require.True(t, it.SeekGE(txIDBytes(2)), "below the floor: real reposition")
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(it.Current()))
	require.False(t, it.Next())

	require.True(t, it.SeekGE(txIDBytes(1)), "reposition after Next-exhaustion")
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(it.Current()))
	require.NoError(t, it.Err())
}

func TestPebbleTxRangeIterator_SeekFloorKeepsRepositioning(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	for _, id := range []uint64{1, 2, 3} {
		require.NoError(t, s.DB().Set(append(txAttributeCode("l"), txIDBytes(id)...), nil, pebble.NoSync))
	}

	it, err := NewPebbleTxRangeIterator(s.DB(), "l", txIDBytes(1), txIDBytes(4))
	require.NoError(t, err)
	defer it.Close()

	require.False(t, it.SeekGE(txIDBytes(5)), "past the upper bound")
	require.False(t, it.Next(), "failed seek leaves the iterator exhausted")
	require.False(t, it.SeekGE(txIDBytes(9)), "covered by the floor")

	require.True(t, it.SeekGE(txIDBytes(2)), "below the floor: real reposition")
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(it.Current()))
	require.False(t, it.Next())

	require.True(t, it.SeekGE(txIDBytes(1)), "reposition after Next-exhaustion")
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(it.Current()))
	require.NoError(t, it.Err())
}

func TestPebbleAccountIterator_SeekFloorKeepsRepositioning(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	prefix := make([]byte, 2+dal.LedgerNameFixedSize)
	prefix[0] = dal.ZoneAttributes
	prefix[1] = dal.SubAttrVolume
	copy(prefix[2:], "l")

	for _, addr := range []string{"a:1", "a:2", "a:3"} {
		key := append(append(append([]byte{}, prefix...), addr...), dal.CanonicalKeySepVolume)
		require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
	}

	it, err := newSingleTypeAccountIterator(s.DB(), dal.SubAttrVolume, "l", "")
	require.NoError(t, err)
	defer it.Close()

	require.False(t, it.SeekGE([]byte("b")), "no address >= b")
	require.False(t, it.Next(), "failed seek leaves the iterator exhausted")
	require.False(t, it.SeekGE([]byte("c")), "covered by the floor")

	require.True(t, it.SeekGE([]byte("a:2")), "below the floor: real reposition")
	require.Equal(t, "a:2", string(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, "a:3", string(it.Current()))
	require.False(t, it.Next())

	require.True(t, it.SeekGE([]byte("a:1")), "reposition after Next-exhaustion")
	require.Equal(t, "a:1", string(it.Current()))
	require.NoError(t, it.Err())
}
