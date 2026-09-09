package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// writeQueryCheckpointRow persists a query-checkpoint row (max_sequence = id*10,
// created_at = id*100, applied_index = id*1000) directly at its ZoneGlobal/SubGlobQueryCheckpoint key,
// matching the on-disk layout the FSM writes.
func writeQueryCheckpointRow(t *testing.T, store *dal.Store, id uint64) {
	t.Helper()
	writeQueryCheckpointRowKeyed(t, store, id, id, id*10, id*100, id*1000)
}

// writeQueryCheckpointRowKeyed persists a row at key keyID carrying a payload
// with checkpoint_id=payloadID, the given max_sequence and created_at, so tests
// can build the corrupted key≠payload and content-mismatch cases.
func writeQueryCheckpointRowKeyed(t *testing.T, store *dal.Store, keyID, payloadID, maxSeq, createdAt, appliedIndex uint64) {
	t.Helper()

	batch := store.OpenWriteSession()
	batch.KeyBuilder.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobQueryCheckpoint).PutUint64(keyID)
	require.NoError(t, batch.SetProto(batch.KeyBuilder.Consume(), &raftcmdpb.QueryCheckpointState{
		CheckpointId: payloadID,
		MaxSequence:  maxSeq,
		CreatedAt:    &commonpb.Timestamp{Data: createdAt},
		AppliedIndex: appliedIndex,
	}))
	require.NoError(t, batch.Commit())
}

// derivedLog is the audit-derived value for one checkpoint id (max_sequence =
// id*10, created_at = id*100, applied_index = id*1000), matching writeQueryCheckpointRow.
func derivedLog(id uint64) *commonpb.CreatedQueryCheckpointLog {
	return &commonpb.CreatedQueryCheckpointLog{
		CheckpointId: id,
		MaxSequence:  id * 10,
		CreatedAt:    &commonpb.Timestamp{Data: id * 100},
		AppliedIndex: id * 1000,
	}
}

// collectQueryCheckpointEvents runs compareQueryCheckpoints against the store's
// live checkpoint rows with the given audit-derived set and returns only the
// QUERY_CHECKPOINT_MISMATCH errors.
func collectQueryCheckpointEvents(t *testing.T, store *dal.Store, derived map[uint64]*commonpb.CreatedQueryCheckpointLog) []*servicepb.CheckStoreError {
	t.Helper()

	checker := NewChecker(store, attributes.New(), "query-checkpoint-cluster", nil, logging.Testing())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	var got []*servicepb.CheckStoreError

	require.NoError(t, checker.compareQueryCheckpoints(handle, derived, func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok &&
			e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_QUERY_CHECKPOINT_MISMATCH {
			got = append(got, e.Error)
		}
	}))

	return got
}

// TestCompareQueryCheckpoints_PhantomFlagged: a stored row the audit chain never
// created (or later deleted) is corruption and must emit exactly one mismatch.
func TestCompareQueryCheckpoints_PhantomFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeQueryCheckpointRow(t, store, 5)

	events := collectQueryCheckpointEvents(t, store, map[uint64]*commonpb.CreatedQueryCheckpointLog{})
	require.Len(t, events, 1)
	require.Contains(t, events[0].GetMessage(), "5")
}

// TestCompareQueryCheckpoints_ConsistentPasses: every stored row justified by
// the derived set, with matching contents, emits nothing.
func TestCompareQueryCheckpoints_ConsistentPasses(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeQueryCheckpointRow(t, store, 5)
	writeQueryCheckpointRow(t, store, 6)

	events := collectQueryCheckpointEvents(t, store, map[uint64]*commonpb.CreatedQueryCheckpointLog{5: derivedLog(5), 6: derivedLog(6)})
	require.Empty(t, events)
}

// TestCompareQueryCheckpoints_KeyAuthoritative: a row whose Pebble key id (99)
// differs from its payload checkpoint_id (5) must be judged by the KEY — the
// check sees a phantom key 99 and a missing row for the audit-live 5.
func TestCompareQueryCheckpoints_KeyAuthoritative(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeQueryCheckpointRowKeyed(t, store, 99, 5, 50, 500, 5000)

	events := collectQueryCheckpointEvents(t, store, map[uint64]*commonpb.CreatedQueryCheckpointLog{5: derivedLog(5)})
	require.Len(t, events, 2)

	msgs := events[0].GetMessage() + "\n" + events[1].GetMessage()
	require.Contains(t, msgs, "stored query checkpoint 99", "the phantom key 99 must be flagged, not laundered by payload id 5")
	require.Contains(t, msgs, "live query checkpoint 5", "the audit-live id 5 has no stored row")
}

// TestCompareQueryCheckpoints_MissingRowFlagged: an audit-live checkpoint with
// no stored row is a lost/dropped projection and must be flagged.
func TestCompareQueryCheckpoints_MissingRowFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)

	events := collectQueryCheckpointEvents(t, store, map[uint64]*commonpb.CreatedQueryCheckpointLog{7: derivedLog(7)})
	require.Len(t, events, 1)
	require.Contains(t, events[0].GetMessage(), "7")
}

// TestCompareQueryCheckpoints_MaxSequenceMismatch: a present row whose stored
// max_sequence diverges from the audit-derived value is content corruption.
func TestCompareQueryCheckpoints_MaxSequenceMismatch(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeQueryCheckpointRowKeyed(t, store, 5, 5, 50, 500, 5000)

	// Audit says max_sequence 99, store holds 50 (created_at matches).
	events := collectQueryCheckpointEvents(t, store, map[uint64]*commonpb.CreatedQueryCheckpointLog{
		5: {CheckpointId: 5, MaxSequence: 99, CreatedAt: &commonpb.Timestamp{Data: 500}, AppliedIndex: 5000},
	})
	require.Len(t, events, 1)
	require.Contains(t, events[0].GetMessage(), "max_sequence")
}

// TestCompareQueryCheckpoints_CreatedAtMismatch: a present row whose stored
// created_at diverges from the audit-derived value is content corruption.
func TestCompareQueryCheckpoints_CreatedAtMismatch(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeQueryCheckpointRowKeyed(t, store, 5, 5, 50, 500, 5000)

	// Audit says created_at 999, store holds 500 (max_sequence matches).
	events := collectQueryCheckpointEvents(t, store, map[uint64]*commonpb.CreatedQueryCheckpointLog{
		5: {CheckpointId: 5, MaxSequence: 50, CreatedAt: &commonpb.Timestamp{Data: 999}, AppliedIndex: 5000},
	})
	require.Len(t, events, 1)
	require.Contains(t, events[0].GetMessage(), "created_at")
}

func TestCompareQueryCheckpoints_AppliedIndexMismatch(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeQueryCheckpointRowKeyed(t, store, 5, 5, 50, 500, 4999)

	events := collectQueryCheckpointEvents(t, store, map[uint64]*commonpb.CreatedQueryCheckpointLog{
		5: {CheckpointId: 5, MaxSequence: 50, CreatedAt: &commonpb.Timestamp{Data: 500}, AppliedIndex: 5000},
	})
	require.Len(t, events, 1)
	require.Contains(t, events[0].GetMessage(), "applied_index")
}

// TestCompareQueryCheckpoints_PayloadIDMismatch: a present row whose payload
// checkpoint_id disagrees with its key is a corrupted row.
func TestCompareQueryCheckpoints_PayloadIDMismatch(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	// Keyed 5, payload says 6, matching max_sequence and created_at.
	writeQueryCheckpointRowKeyed(t, store, 5, 6, 50, 500, 5000)

	events := collectQueryCheckpointEvents(t, store, map[uint64]*commonpb.CreatedQueryCheckpointLog{5: derivedLog(5)})
	require.Len(t, events, 1)
	require.Contains(t, events[0].GetMessage(), "mismatched payload checkpoint_id")
}

// TestCheck_QueryCheckpointProjection_EmptyAuditWiring pins that the checkpoint
// comparison runs on the lastSequence == 0 fast path, which returns before the
// replay (and thus before the compare phase every other projection pass lives
// in). A zero-log store proves no CreateQueryCheckpoint order was audited, so a
// stored checkpoint row is unaudited by construction; reporting it clean would
// let it be loaded into LiveQueryCheckpointIDs unchecked.
func TestCheck_QueryCheckpointProjection_EmptyAuditWiring(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeQueryCheckpointRow(t, store, 1)

	checker := NewChecker(store, attributes.New(), "test-cluster", nil, logging.Testing())

	var got []*servicepb.CheckStoreError
	require.NoError(t, checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
		if errEvent, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			got = append(got, errEvent.Error)
		}
	}))

	require.Len(t, got, 1, "a stored query checkpoint with no audited creation must be reported")
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_QUERY_CHECKPOINT_MISMATCH, got[0].GetErrorType())
	require.Contains(t, got[0].GetMessage(), "not justified by the audit chain")
}
