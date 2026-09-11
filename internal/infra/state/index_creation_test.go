package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func indexCreationOrder(ledger string, id *commonpb.IndexID) *raftcmdpb.Order {
	return indexApplyOrder(ledger, &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateIndex{
		CreateIndex: &raftcmdpb.CreateIndexOrder{Id: id},
	}})
}

func indexDropOrder(ledger string, id *commonpb.IndexID) *raftcmdpb.Order {
	return indexApplyOrder(ledger, &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_DropIndex{
		DropIndex: &raftcmdpb.DropIndexOrder{Id: id},
	}})
}

func indexApplyOrder(ledger string, apply *raftcmdpb.LedgerApplyOrder) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: ledger, Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: apply},
	}}}
}

// Index coverage is explicit here because the generic machine fixture only
// declares ledger, transaction and metadata keys. Values remain cache-backed.
func indexProposal(sequence uint64, ledger string, id *commonpb.IndexID, orders ...*raftcmdpb.Order) *raftcmdpb.Proposal {
	proposal := makeProposal(sequence, orders...)
	key, _ := attributes.MakeKey(indexes.KeyFor(ledger, id).Bytes())
	proposal.ExecutionPlan.Attributes = append(proposal.ExecutionPlan.Attributes, declareTestPlan(key, dal.SubAttrIndex))

	return proposal
}

func TestCreateIndex_DuplicateBatchRollsBack(t *testing.T) {
	t.Parallel()

	machine, store, attrs := newTestMachine(t)
	const ledger = "index-atomic"
	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledger))))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, handle.Close()) })
	lastBefore, err := query.ReadLastLog(handle)
	require.NoError(t, err)

	result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, indexProposal(2, ledger, id,
		indexCreationOrder(ledger, id), indexCreationOrder(ledger, id))))
	require.NoError(t, err)
	var duplicate *domain.ErrIndexAlreadyExists
	require.ErrorAs(t, result.Results[0].Error, &duplicate)
	require.Equal(t, indexes.Canonical(id), duplicate.Index)
	require.Empty(t, result.Results[0].Logs)
	row, err := attrs.Index.Get(handle, indexes.KeyFor(ledger, id).Bytes())
	require.NoError(t, err)
	require.Nil(t, row, "first creation must roll back with the duplicate")
	_, _, err = machine.Registry.Indexes.KeyStore().GetKey(indexes.KeyFor(ledger, id))
	require.ErrorIs(t, err, domain.ErrNotFound, "rollback must also discard the cache entry")
	lastAfter, err := query.ReadLastLog(handle)
	require.NoError(t, err)
	require.Equal(t, lastBefore.GetSequence(), lastAfter.GetSequence(), "failed batch must not commit a CreatedIndexLog")
	audit := listAuditEntries(t, store, 0)
	failure := audit[len(audit)-1].GetFailure()
	require.Equal(t, domain.ErrReasonIndexAlreadyExists, domain.ReasonString(failure.GetReason()))
	require.Equal(t, map[string]string{"index": indexes.Canonical(id)}, failure.GetContext())

	// A new creation proves the aborted write did not survive in the FSM.
	result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 3, indexProposal(3, ledger, id, indexCreationOrder(ledger, id))))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	require.Len(t, result.Results[0].Logs, 1)
}

func TestCreateIndex_IdempotencyReplayAndFreshDuplicate(t *testing.T) {
	t.Parallel()

	machine, store, _ := newTestMachine(t)
	const ledger = "index-idem"
	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	apply := func(sequence uint64, key string, order *raftcmdpb.Order) ApplyResult {
		t.Helper()
		proposal := indexProposal(sequence, ledger, id, order)
		if key != "" {
			proposal.Idempotency = &commonpb.Idempotency{Key: key}
		}
		result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, sequence, proposal))
		require.NoError(t, err)

		return result.Results[0]
	}
	require.NoError(t, apply(1, "", createLedgerOrder(ledger)).Error)
	first := apply(2, "create", indexCreationOrder(ledger, id))
	require.NoError(t, first.Error)
	require.Len(t, first.Logs, 1)
	sequence := first.Logs[0].GetCreatedLog().GetSequence()
	auditSequence := machine.State.NextAuditSequenceID
	auditHash := append([]byte(nil), machine.State.LastAuditHash...)

	replay := apply(3, "create", indexCreationOrder(ledger, id))
	require.NoError(t, replay.Error)
	require.Len(t, replay.Logs, 1)
	require.Nil(t, replay.Logs[0].GetCreatedLog())
	require.Equal(t, sequence, replay.Logs[0].GetReferenceSequence())
	require.False(t, replay.AuditEntryWritten)
	require.Equal(t, auditSequence, machine.State.NextAuditSequenceID)
	require.Equal(t, auditHash, machine.State.LastAuditHash)

	for offset, key := range []string{"", "fresh-key"} {
		duplicate := apply(uint64(4+offset), key, indexCreationOrder(ledger, id))
		var alreadyExists *domain.ErrIndexAlreadyExists
		require.ErrorAs(t, duplicate.Error, &alreadyExists)
		require.Empty(t, duplicate.Logs)
	}

	// Definitive duplicate failure stays frozen under its retained key even
	// after a drop makes a fresh creation possible.
	require.NoError(t, apply(6, "", indexDropOrder(ledger, id)).Error)
	frozen := apply(7, "fresh-key", indexCreationOrder(ledger, id))
	var replayed *domain.ReplayedFailure
	require.ErrorAs(t, frozen.Error, &replayed)
	require.Equal(t, domain.ErrReasonIndexAlreadyExists, replayed.Reason())
	require.False(t, frozen.AuditEntryWritten)
	require.NoError(t, apply(8, "another-key", indexCreationOrder(ledger, id)).Error)
}

func TestCreateIndex_DuplicateAfterRetypePreservesRegistry(t *testing.T) {
	t.Parallel()

	machine, store, attrs := newTestMachine(t)
	const ledger = "index-retype"
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score")
	apply := func(sequence uint64, orders ...*raftcmdpb.Order) ApplyResult {
		t.Helper()
		result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, sequence, indexProposal(sequence, ledger, id, orders...)))
		require.NoError(t, err)

		return result.Results[0]
	}
	setType := func(typ commonpb.MetadataType) *raftcmdpb.Order {
		return indexApplyOrder(ledger, &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
			SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: "score", Type: typ},
		}})
	}
	require.NoError(t, apply(1, createLedgerOrder(ledger)).Error)
	require.NoError(t, apply(2, setType(commonpb.MetadataType_METADATA_TYPE_STRING), indexCreationOrder(ledger, id)).Error)
	require.NoError(t, apply(3, setType(commonpb.MetadataType_METADATA_TYPE_INT64)).Error)

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, handle.Close()) })
	before, err := attrs.Index.Get(handle, indexes.KeyFor(ledger, id).Bytes())
	require.NoError(t, err)
	require.Equal(t, uint32(2), before.GetForwardEncodingVersion())
	lastBefore, err := query.ReadLastLog(handle)
	require.NoError(t, err)

	duplicate := apply(4, indexCreationOrder(ledger, id))
	var alreadyExists *domain.ErrIndexAlreadyExists
	require.ErrorAs(t, duplicate.Error, &alreadyExists)
	require.Empty(t, duplicate.Logs)
	after, err := attrs.Index.Get(handle, indexes.KeyFor(ledger, id).Bytes())
	require.NoError(t, err)
	require.True(t, before.EqualVT(after), "duplicate must preserve the complete persisted registry row")
	cached, _, err := machine.Registry.Indexes.KeyStore().GetKey(indexes.KeyFor(ledger, id))
	require.NoError(t, err)
	require.True(t, before.EqualVT(cached), "cache and durable row must agree after the rejected duplicate")
	lastAfter, err := query.ReadLastLog(handle)
	require.NoError(t, err)
	require.Equal(t, lastBefore.GetSequence(), lastAfter.GetSequence())

	// Drop/recreate in one batch observes the tombstone and creates version 1
	// bound to the current schema, including a fresh creation timestamp.
	recreated := apply(5, indexDropOrder(ledger, id), indexCreationOrder(ledger, id))
	require.NoError(t, recreated.Error)
	require.Len(t, recreated.Logs, 2)
	rebuilt, err := attrs.Index.Get(handle, indexes.KeyFor(ledger, id).Bytes())
	require.NoError(t, err)
	require.Equal(t, uint32(1), rebuilt.GetForwardEncodingVersion())
	require.NotEqual(t, before.GetCreatedAt().GetData(), rebuilt.GetCreatedAt().GetData())
	created := recreated.Logs[1].GetCreatedLog().GetPayload().GetApply().GetLog().GetData().GetCreateIndex()
	require.True(t, created.GetBoundTypeDeclared())
	require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, created.GetBoundType())
}

func TestCreateIndex_MissingIndexCoverageIsNotAbsence(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	const ledger = "index-coverage"
	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledger))))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	// Deliberately omit the index plan: the real gate must reject the probe.
	result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, makeProposal(2, indexCreationOrder(ledger, id))))
	require.NoError(t, err)
	var miss *ErrCoverageMiss
	require.ErrorAs(t, result.Results[0].Error, &miss)
	require.Equal(t, domain.ErrReasonCoverageMiss, miss.Reason())
	require.Empty(t, result.Results[0].Logs)
}
