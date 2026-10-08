package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
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

	ledgerInfo := &ledgerpb.LedgerInfo{Name: "test-ledger", Id: 7}
	indexID := indexes.TxBuiltinID(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	now := &ledgerpb.Timestamp{Data: 1}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(now.AsReader())

	// Shared Indexes stub: Put captures the entry written by
	// processCreateIndex.
	var seenKey domain.IndexKey
	var seenIdx *ledgerpb.Index
	idxStub := setupIndexesStub(mockStore)
	idxStub.putHook = func(key domain.IndexKey, idx *ledgerpb.Index) {
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

	ledgerInfo := &ledgerpb.LedgerInfo{Name: "test-ledger", Id: 3}
	indexID := indexes.MetadataID(ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, "color")

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

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&ledgerpb.LedgerInfo{Name: "test-ledger", Id: 4}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return((&ledgerpb.Timestamp{Data: 1}).AsReader())
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

// TestProcessCreateIndex_StampsBoundTypeAtSequence pins the EN-1724 binding
// contract: the minted CreatedIndexLog carries the metadata field's declared
// type as it stands when the order applies. Replicas fold the log at
// arbitrary replay distance — a backfill or a rebuild sees a schema that may
// be several retypes ahead — so the log itself is the only place the
// at-sequence binding can live.
func TestProcessCreateIndex_StampsBoundTypeAtSequence(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	ledgerInfo := &ledgerpb.LedgerInfo{
		Name: "test-ledger",
		Id:   7,
		MetadataSchema: &ledgerpb.MetadataSchema{
			AccountFields: map[string]*ledgerpb.MetadataFieldSchema{
				"score": {Type: ledgerpb.MetadataType_METADATA_TYPE_INT64},
			},
		},
	}
	indexID := indexes.MetadataID(ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, "score")
	now := &ledgerpb.Timestamp{Data: 1}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(now.AsReader())

	idxStub := setupIndexesStub(mockStore)
	idxStub.expectGet(domain.IndexKey{LedgerName: "test-ledger", Canonical: indexes.Canonical(indexID)}, nil, domain.ErrNotFound)
	idxStub.putHook = func(domain.IndexKey, *ledgerpb.Index) {}

	payload, derr := processCreateIndex("test-ledger", &raftcmdpb.CreateIndexOrder{Id: indexID}, &Context{Scope: mockStore})
	require.Nil(t, derr)

	log := payload.GetCreateIndex()
	require.NotNil(t, log)
	require.True(t, log.GetBoundTypeDeclared())
	require.Equal(t, ledgerpb.MetadataType_METADATA_TYPE_INT64, log.GetBoundType())
}

// TestProcessCreateIndex_BuiltinCarriesNoBinding verifies a builtin index's
// CreatedIndexLog is stamped with no type binding: there is no metadata
// field, and its values keep the natural encoding.
func TestProcessCreateIndex_BuiltinCarriesNoBinding(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	ledgerInfo := &ledgerpb.LedgerInfo{Name: "test-ledger", Id: 7}
	indexID := indexes.TxBuiltinID(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	now := &ledgerpb.Timestamp{Data: 1}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(now.AsReader())

	idxStub := setupIndexesStub(mockStore)
	idxStub.expectGet(domain.IndexKey{LedgerName: "test-ledger", Canonical: indexes.Canonical(indexID)}, nil, domain.ErrNotFound)
	idxStub.putHook = func(domain.IndexKey, *ledgerpb.Index) {}

	payload, derr := processCreateIndex("test-ledger", &raftcmdpb.CreateIndexOrder{Id: indexID}, &Context{Scope: mockStore})
	require.Nil(t, derr)

	log := payload.GetCreateIndex()
	require.NotNil(t, log)
	require.False(t, log.GetBoundTypeDeclared())
}
