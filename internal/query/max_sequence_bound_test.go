package query_test

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// sequenceKey builds a [zone][sub][sequence BE 8] key.
func sequenceKey(zone, sub byte, sequence uint64) []byte {
	out := make([]byte, 10)
	out[0], out[1] = zone, sub
	binary.BigEndian.PutUint64(out[2:], sequence)

	return out
}

// TestReadersIncludeMaxUint64Sequence covers every sequence-keyed reader that
// used to bound itself with dal.MaxUint64Bytes.
//
// Pebble's iterator upper bound is EXCLUSIVE, so appending an eight-byte 0xFF
// run to a [zone][sub] prefix produced a bound byte-identical to the key at
// math.MaxUint64 and dropped exactly that row. Every reader below was blind to
// it: the checker could not report it, recovery could not see it, the index
// builder and event emitter skipped it, and an incremental export left it out.
// The bound is now the prefix successor, which has no such hole.
//
// The FSM cannot allocate this sequence — the counters are seeded at 1 and
// advanced by one — so the row can only arrive through a tampered restore
// stream or storage corruption. That is precisely why the readers must see it.
func TestReadersIncludeMaxUint64Sequence(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	logValue, err := (&commonpb.Log{Sequence: math.MaxUint64}).MarshalVT()
	require.NoError(t, err)

	auditValue, err := (&auditpb.AuditEntry{Sequence: math.MaxUint64}).MarshalVT()
	require.NoError(t, err)

	proposalValue, err := (&proposalpb.AppliedProposal{}).MarshalVT()
	require.NoError(t, err)

	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes(sequenceKey(dal.ZoneHistory, dal.SubHistoryLog, math.MaxUint64), logValue))
	require.NoError(t, batch.SetBytes(sequenceKey(dal.ZoneHistory, dal.SubHistoryAudit, math.MaxUint64), auditValue))
	require.NoError(t, batch.SetBytes(sequenceKey(dal.ZoneHistory, dal.SubHistoryAppliedProposal, math.MaxUint64), proposalValue))
	require.NoError(t, batch.Commit())

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)

	// Not parallel below: the subtests share this handle, which is closed when
	// this function returns.
	defer func() { _ = handle.Close() }()

	ctx := context.Background()

	t.Run("ReadLastSequence", func(t *testing.T) {
		last, err := query.ReadLastSequence(handle)
		require.NoError(t, err)
		require.EqualValues(t, uint64(math.MaxUint64), last)
	})

	t.Run("ReadLogsSince", func(t *testing.T) {
		c, err := query.ReadLogsSince(ctx, handle, 0)
		require.NoError(t, err)

		logs, err := cursor.Collect[*commonpb.Log](c)
		require.NoError(t, err)
		require.Len(t, logs, 1)
		require.EqualValues(t, uint64(math.MaxUint64), logs[0].GetSequence())
	})

	t.Run("ReadLogsSinceRaw", func(t *testing.T) {
		iter, err := query.ReadLogsSinceRaw(ctx, handle, 0)
		require.NoError(t, err)

		defer func() { _ = iter.Close() }()

		require.True(t, iter.First(), "the raw iterator must reach the row")
		require.EqualValues(t, uint64(math.MaxUint64), binary.BigEndian.Uint64(iter.Key()[2:10]))
	})

	t.Run("ReadLastAuditEntry", func(t *testing.T) {
		entry, err := query.ReadLastAuditEntry(handle)
		require.NoError(t, err)
		require.NotNil(t, entry)
		require.EqualValues(t, uint64(math.MaxUint64), entry.GetSequence())
	})

	t.Run("ReadAuditEntries", func(t *testing.T) {
		c, err := query.ReadAuditEntries(ctx, handle, nil)
		require.NoError(t, err)

		entries, err := cursor.Collect[*auditpb.AuditEntry](c)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.EqualValues(t, uint64(math.MaxUint64), entries[0].GetSequence())
	})

	t.Run("ReadAppliedProposals", func(t *testing.T) {
		c, err := query.ReadAppliedProposals(ctx, handle, nil)
		require.NoError(t, err)

		proposals, err := cursor.Collect[*proposalpb.AppliedProposal](c)
		require.NoError(t, err)
		require.Len(t, proposals, 1)
	})
}

// TestReadAuditItemsAtMaxUint64Sequence covers the other half of the wrap: the
// item range is built as [seq, seq+1), which at math.MaxUint64 wraps to a bound
// BELOW the lower one. Pebble reads that as an empty range, so the entry's items
// vanished silently — on the audit path an entry with no items reads as an
// entry with no orders, which is a shape no writer produces.
func TestReadAuditItemsAtMaxUint64Sequence(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	itemValue, err := (&auditpb.AuditItem{OrderIndex: 0, LogSequence: 7}).MarshalVT()
	require.NoError(t, err)

	key := make([]byte, 14)
	key[0], key[1] = dal.ZoneHistory, dal.SubHistoryAuditItem
	binary.BigEndian.PutUint64(key[2:], math.MaxUint64)
	binary.BigEndian.PutUint32(key[10:], 0)

	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes(key, itemValue))
	require.NoError(t, batch.Commit())

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	items, err := query.ReadAuditItems(context.Background(), handle, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, items, 1, "a wrapped upper bound would report the entry as having no items")
	require.EqualValues(t, 7, items[0].GetLogSequence())
}
