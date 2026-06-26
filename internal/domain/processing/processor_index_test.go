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
// (a) PROBE the registry to detect an existing READY duplicate, (b) PUT a
// fresh BUILDING entry keyed by (LedgerID, Canonical), and (c) NEVER call
// PutLedger — the LedgerInfo proto no longer carries indexes.
func TestProcessCreateIndex_WritesRegistryNotLedgerInfo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger", Id: 7}
	indexID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	now := &commonpb.Timestamp{Data: 1}

	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo.AsReader(), nil)
	mockStore.EXPECT().GetIndex(domain.IndexKey{LedgerName: "test-ledger", Canonical: indexes.Canonical(indexID)}).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().GetDate().Return(now.AsReader())

	var seenKey domain.IndexKey
	var seenIdx *commonpb.Index

	mockStore.EXPECT().PutIndex(gomock.Any(), gomock.Any()).Do(func(key domain.IndexKey, idx *commonpb.Index) {
		seenKey = key
		seenIdx = idx
	})

	order := &raftcmdpb.CreateIndexOrder{Id: indexID}
	payload, derr := processCreateIndex("test-ledger", order, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)

	require.Equal(t, "test-ledger", seenKey.LedgerName)
	require.Equal(t, indexes.Canonical(indexID), seenKey.Canonical)
	require.Equal(t, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING, seenIdx.GetBuildStatus())
	require.Equal(t, "test-ledger", seenIdx.GetLedger())
	require.True(t, indexes.Equal(indexID, seenIdx.GetId()))
}

// TestProcessCreateIndex_ShortCircuitOnReady verifies that an idempotent
// re-issue against a READY entry does NOT call PutIndex again — the
// short-circuit branch is exercised by the registry probe alone.
func TestProcessCreateIndex_ShortCircuitOnReady(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger", Id: 7}
	indexID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)

	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo.AsReader(), nil)
	mockStore.EXPECT().GetIndex(domain.IndexKey{LedgerName: "test-ledger", Canonical: indexes.Canonical(indexID)}).Return(
		(&commonpb.Index{Id: indexID, BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY}).AsReader(),
		nil,
	)

	// No PutIndex / GetDate expected on the short-circuit path.
	payload, derr := processCreateIndex("test-ledger", &raftcmdpb.CreateIndexOrder{Id: indexID}, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)
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

	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo.AsReader(), nil)
	mockStore.EXPECT().DeleteIndex(domain.IndexKey{LedgerName: "test-ledger", Canonical: indexes.Canonical(indexID)})

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

	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 4}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 1}).AsReader())
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any())
	// No DeleteIndex / RangeIndexes — the deferred Pebble range delete is
	// derived from the DeletedLedgerLog by the WriteSet sink via Absorb at
	// commit time, not requested directly by the processor.

	payload, derr := processDeleteLedger("test-ledger", &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)
}
