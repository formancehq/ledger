package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestProcessCreateIndex_WritesRegistryNotLedgerInfo pins down the contract
// after the bucket-scoped index registry refactor: a CreateIndexOrder must
// (a) PUT a fresh entry keyed by (LedgerID, Canonical), and (b) NEVER call
// PutLedger — the LedgerInfo proto no longer carries indexes.
func TestProcessCreateIndex_WritesRegistryNotLedgerInfo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger", Id: 7}
	indexID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	now := &commonpb.Timestamp{Data: 1}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(now.AsReader())

	// Shared Indexes stub: Put captures the entry written by
	// processCreateIndex.
	var seenKey domain.IndexKey
	var seenIdx *commonpb.Index
	idxStub := setupIndexesStub(mockStore)
	idxStub.putHook = func(key domain.IndexKey, idx *commonpb.Index) {
		seenKey = key
		seenIdx = idx
	}

	order := &raftcmdpb.CreateIndexOrder{Id: indexID}
	payload, derr := processCreateIndex("test-ledger", order, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)

	require.Equal(t, "test-ledger", seenKey.LedgerName)
	require.Equal(t, indexes.Canonical(indexID), seenKey.Canonical)
	require.Equal(t, "test-ledger", seenIdx.GetLedger())
	require.Equal(t, uint32(1), seenIdx.GetForwardEncodingVersion())
	require.True(t, indexes.Equal(indexID, seenIdx.GetId()))
}

// TestProcessDropIndex_DeletesByRegistryKey verifies the drop path routes
// through the registry: no PutLedger, just a DeleteIndex(IndexKey).
func TestProcessDropIndex_DeletesByRegistryKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger", Id: 3}
	indexID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "color")

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil)
	expectDeleteIndex(t, mockStore, domain.IndexKey{LedgerName: "test-ledger", Canonical: indexes.Canonical(indexID)})

	payload, derr := processDropIndex("test-ledger", &raftcmdpb.DropIndexOrder{Id: indexID}, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)
}

// TestProcessDeleteLedger_DoesNotTouchIndexRegistry pins the design choice:
// the per-ledger Index registry purge is NOT done in-batch — it is delegated
// to the deferred batch.deleteLedgerData pass (Pebble range delete on the
// SubAttrIndex zone) and to the processApply DeletedAt guard that blocks
// any same-batch reader. An in-batch cache-iteration drop would bypass the
// coverage gate (no preload exists for the ledger's index set), so we
// deliberately keep the loop out of the FSM hot path.
//
// On this branch the cleanup signal is no longer requested explicitly by
// the processor: the WriteSet sink absorbs the DeletedLedgerLog payload
// and queues the Pebble range-delete at Merge time. The test therefore
// only pins what the processor itself touches (load + PutLedger with
// DeletedAt set).
func TestProcessDeleteLedger_DoesNotTouchIndexRegistry(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger", Id: 4}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 1}).AsReader())
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	// The Boundary cascade is now gated: processDeleteLedger deletes it
	// through the Scope with the envelope key (EN-1522).
	expectDeleteBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"})
	// No DeleteIndex / RangeIndexes — the deferred Pebble range delete is
	// derived from the DeletedLedgerLog by the WriteSet sink via Absorb at
	// commit time, not requested directly by the processor.

	payload, derr := processDeleteLedger("test-ledger", &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)
}

// TestProcessCreateIndex_StampsInitialWhenBornEmpty verifies an index declared
// while its ledger is still born-empty is stamped initial=true on the log.
func TestProcessCreateIndex_StampsInitialWhenBornEmpty(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger", Id: 7}
	indexID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	now := &commonpb.Timestamp{Data: 1}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(now.AsReader())

	idxStub := setupIndexesStub(mockStore)
	idxStub.putHook = func(domain.IndexKey, *commonpb.Index) {}

	ctx := &Context{Scope: mockStore}
	ctx.markBornEmpty("test-ledger")

	payload, derr := processCreateIndex("test-ledger", &raftcmdpb.CreateIndexOrder{Id: indexID}, ctx)
	require.Nil(t, derr)
	require.True(t, payload.GetCreateIndex().GetInitial())
}

// TestProcessCreateIndex_NotInitialWithoutBornEmpty verifies the default: an
// index on a ledger not tracked as born-empty is stamped initial=false.
func TestProcessCreateIndex_NotInitialWithoutBornEmpty(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger", Id: 7}
	indexID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	now := &commonpb.Timestamp{Data: 1}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(now.AsReader())

	idxStub := setupIndexesStub(mockStore)
	idxStub.putHook = func(domain.IndexKey, *commonpb.Index) {}

	payload, derr := processCreateIndex("test-ledger", &raftcmdpb.CreateIndexOrder{Id: indexID}, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.False(t, payload.GetCreateIndex().GetInitial())
}
