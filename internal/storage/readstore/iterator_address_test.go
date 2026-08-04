package readstore

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func txIDBytes(id uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)

	return b
}

// newAddressTxFixture writes account→tx rows and returns an AddressTxIterator
// over the given addresses.
func newAddressTxFixture(t *testing.T, txsByAccount map[string][]uint64, addrs ...string) *AddressTxIterator {
	t.Helper()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	for account, txs := range txsByAccount {
		for _, id := range txs {
			require.NoError(t, s.DB().Set(AccountTxKey(kb, PrefixAccountTx, "l", account, id), nil, pebble.NoSync))
		}
	}

	return NewAddressTxIterator(s.DB(), dal.NewKeyBuilder(), "l", newAliasingIter(addrs...), PrefixAccountTx)
}

// SeekGE on AddressTxIterator must be an absolute reposition over the
// materialized union: repeatable at the same target, seekable backwards, and
// well-defined after exhaustion (EN-1597, paul-nicolas review of PR #1635).
// The prior implementation consumed the matched entry (`pendingTxns[idx+1:]`)
// and latched on exhaustion, so a repeated or backward seek dropped rows.
func TestAddressTxIterator_SeekGEIsAbsolute(t *testing.T) {
	t.Parallel()

	it := newAddressTxFixture(t, map[string][]uint64{
		"acc:1": {1, 3},
		"acc:2": {2, 3}, // tx 3 shared — union must deduplicate
	}, "acc:1", "acc:2")
	defer it.Close()

	// Full forward pass: the deduplicated union in order.
	var got []uint64
	for it.Next() {
		got = append(got, binary.BigEndian.Uint64(it.Current()))
	}
	require.Equal(t, []uint64{1, 2, 3}, got)
	require.NoError(t, it.Err())

	// Reposition after exhaustion.
	require.True(t, it.SeekGE(txIDBytes(2)))
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(it.Current()))

	// Same target again: same row (a conforming seek does not consume).
	require.True(t, it.SeekGE(txIDBytes(2)))
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(it.Current()))

	// Next continues from the seeked position.
	require.True(t, it.Next())
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(it.Current()))

	// Backward seek below everything.
	require.True(t, it.SeekGE(txIDBytes(0)))
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(it.Current()))

	// Seek past the end fails, then a lower seek succeeds again.
	require.False(t, it.SeekGE(txIDBytes(99)))
	require.False(t, it.Next())
	require.True(t, it.SeekGE(txIDBytes(3)))
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(it.Current()))
	require.NoError(t, it.Err())
}

// An AND over an AddressTxIterator must not drop intersections when converge
// re-seeks the child onto its own current position (the destructive-consume
// regression from the unconditional all-children re-seek in AndIterator.SeekGE).
func TestAndIterator_AddressTxChildKeepsIntersection(t *testing.T) {
	t.Parallel()

	addrTx := newAddressTxFixture(t, map[string][]uint64{
		"acc:1": {1, 2, 3},
	}, "acc:1")

	other := newAliasingIter(
		string(txIDBytes(1)), string(txIDBytes(2)), string(txIDBytes(3)),
	)

	it := NewAndIterator(addrTx, other)
	defer it.Close()

	require.True(t, it.SeekGE(txIDBytes(1)))
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(it.Current()))

	// Re-seek to the same target: the intersection must still start at 1.
	require.True(t, it.SeekGE(txIDBytes(1)))
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(it.Current()))

	var rest []uint64
	for it.Next() {
		rest = append(rest, binary.BigEndian.Uint64(it.Current()))
	}
	require.Equal(t, []uint64{2, 3}, rest)
	require.NoError(t, it.Err())
}

// A NOT whose child is an AddressTxIterator must keep excluding after the
// child was driven to exhaustion by a forward pass — the child's SeekGE must
// reposition, not latch. Pre-fix, the exhausted (and consumed) child stopped
// excluding, leaking every excluded row into the difference.
func TestNotIterator_AddressTxChildExcludesAfterExhaustion(t *testing.T) {
	t.Parallel()

	addrTx := newAddressTxFixture(t, map[string][]uint64{
		"acc:1": {1, 2, 3},
	}, "acc:1")

	universe := newAliasingIter(
		string(txIDBytes(1)), string(txIDBytes(2)), string(txIDBytes(3)), string(txIDBytes(4)),
	)

	it := NewNotIterator(universe, addrTx)
	defer it.Close()

	// Forward pass: 1-3 are excluded; reaching 4 drives the child past its
	// last entry, exhausting it.
	require.True(t, it.Next())
	require.Equal(t, uint64(4), binary.BigEndian.Uint64(it.Current()))
	require.False(t, it.Next())

	// The absolute re-seek back to 1 must still exclude 1-3 and land on 4.
	require.True(t, it.SeekGE(txIDBytes(1)))
	require.Equal(t, uint64(4), binary.BigEndian.Uint64(it.Current()))
	require.NoError(t, it.Err())
}

func TestAddressTxIterator_EmptyUnion(t *testing.T) {
	t.Parallel()

	it := newAddressTxFixture(t, nil, "acc:1")
	defer it.Close()

	require.False(t, it.Next())
	require.False(t, it.SeekGE(txIDBytes(0)))
	require.False(t, it.Next())
	require.NoError(t, it.Err())
}
