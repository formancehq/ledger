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

// EN-1522 gap A — command-envelope key identity.
//
// Each of these handlers loads the ledger by the command-envelope name
// (correct) but historically derived the downstream write/delete/log key
// from the LOADED projection's mutable Name field. If a tampered/desynced
// LedgerInfo.name ever diverged from the envelope, the write silently
// targeted another ledger's keys. The fix keys every write off the
// envelope; these tests pin that by feeding a deliberately divergent
// projection and asserting the envelope wins.

const (
	envelopeLedger  = "envelope-ledger"
	divergentLedger = "divergent-projection"
)

// TestProcessDeleteLedger_KeysOffEnvelopeNotProjection covers A1 + B: the
// DeletedLedgerLog name and the gated Boundary delete both key off the
// envelope, never the loaded projection's Name.
func TestProcessDeleteLedger_KeysOffEnvelopeNotProjection(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	// Loaded projection reports a DIFFERENT name than the envelope.
	expectGetLedger(mockStore, domain.LedgerKey{Name: envelopeLedger},
		(&commonpb.LedgerInfo{Name: divergentLedger, Id: 7}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 1}).AsReader())
	// PutLedger and the gated Boundary delete MUST both use the envelope key.
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: envelopeLedger}, nil)
	expectDeleteBoundaries(t, mockStore, domain.LedgerKey{Name: envelopeLedger})

	payload, derr := processDeleteLedger(envelopeLedger, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)

	// The emitted log names the envelope ledger, not the divergent projection.
	require.Equal(t, envelopeLedger, payload.GetDeleteLedger().GetName())
}

// TestProcessAddLedgerMetadata_KeysOffEnvelope covers A3 (add path): the
// LedgerMetadataKey written for each entry keys off the envelope.
func TestProcessAddLedgerMetadata_KeysOffEnvelope(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	expectGetLedger(mockStore, domain.LedgerKey{Name: envelopeLedger},
		(&commonpb.LedgerInfo{Name: divergentLedger, Id: 7}).AsReader(), nil)

	lm := &kindStub[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]{}
	mockStore.EXPECT().LedgerMetadata().Return(lm).AnyTimes()

	var wroteKeys []domain.LedgerMetadataKey
	lm.onPut(func(k domain.LedgerMetadataKey, _ *commonpb.MetadataValue) {
		wroteKeys = append(wroteKeys, k)
	})

	order := &raftcmdpb.SaveLedgerMetadataOrder{
		Metadata: map[string]*commonpb.MetadataValue{
			"color": commonpb.NewStringValue("blue"),
		},
	}

	payload, derr := processAddLedgerMetadata(envelopeLedger, order, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)

	require.Len(t, wroteKeys, 1)
	require.Equal(t, envelopeLedger, wroteKeys[0].LedgerName)
	require.Equal(t, "color", wroteKeys[0].Key)
}

// TestProcessDeleteLedgerMetadata_KeysOffEnvelope covers A3 (delete path):
// the existence Get and the Delete both key off the envelope.
func TestProcessDeleteLedgerMetadata_KeysOffEnvelope(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	expectGetLedger(mockStore, domain.LedgerKey{Name: envelopeLedger},
		(&commonpb.LedgerInfo{Name: divergentLedger, Id: 7}).AsReader(), nil)

	lm := &kindStub[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]{}
	mockStore.EXPECT().LedgerMetadata().Return(lm).AnyTimes()

	// Existence check must resolve for the envelope key.
	envKey := domain.LedgerMetadataKey{LedgerName: envelopeLedger, Key: "color"}
	lm.expectGet(envKey, commonpb.NewStringValue("blue").AsReader(), nil)

	var deletedKey domain.LedgerMetadataKey
	lm.onDelete(func(k domain.LedgerMetadataKey) { deletedKey = k })

	order := &raftcmdpb.DeleteLedgerMetadataOrder{Key: "color"}

	payload, derr := processDeleteLedgerMetadata(envelopeLedger, order, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)

	require.Equal(t, envelopeLedger, deletedKey.LedgerName)
	require.Equal(t, "color", deletedKey.Key)
}

// TestProcessCreateIndex_KeysOffEnvelope covers the index-registry half of
// gap A: both the duplicate probe and the BUILDING write must key off the
// envelope. This one also pins an internal-consistency property — the row's
// Ledger field already carried the envelope while its KEY came from the
// projection, so a divergence produced a registry entry whose key and payload
// disagreed about which ledger it belonged to.
func TestProcessCreateIndex_KeysOffEnvelope(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	indexID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)

	expectGetLedger(mockStore, domain.LedgerKey{Name: envelopeLedger},
		(&commonpb.LedgerInfo{Name: divergentLedger, Id: 7}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 1}).AsReader())

	var wroteKey domain.IndexKey
	var wroteIdx *commonpb.Index

	idxStub := setupIndexesStub(mockStore)
	idxStub.onPut(func(k domain.IndexKey, idx *commonpb.Index) {
		wroteKey = k
		wroteIdx = idx
	})

	payload, derr := processCreateIndex(envelopeLedger, &raftcmdpb.CreateIndexOrder{Id: indexID}, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)

	require.Equal(t, envelopeLedger, wroteKey.LedgerName, "the registry write must key off the envelope")
	require.Equal(t, indexes.Canonical(indexID), wroteKey.Canonical)
	// Key and payload must agree on the owning ledger.
	require.Equal(t, wroteKey.LedgerName, wroteIdx.GetLedger(),
		"registry row key and its Ledger field must name the same ledger")
}

// TestProcessDropIndex_KeysOffEnvelope covers the drop path: the registry
// Delete keys off the envelope, so a divergent projection cannot drop another
// ledger's index.
func TestProcessDropIndex_KeysOffEnvelope(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	indexID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)

	expectGetLedger(mockStore, domain.LedgerKey{Name: envelopeLedger},
		(&commonpb.LedgerInfo{Name: divergentLedger, Id: 7}).AsReader(), nil)

	var deletedKey domain.IndexKey
	idxStub := setupIndexesStub(mockStore)
	idxStub.onDelete(func(k domain.IndexKey) { deletedKey = k })

	payload, derr := processDropIndex(envelopeLedger, &raftcmdpb.DropIndexOrder{Id: indexID}, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)

	require.Equal(t, envelopeLedger, deletedKey.LedgerName, "the registry delete must key off the envelope")
	require.Equal(t, indexes.Canonical(indexID), deletedKey.Canonical)
}

// TestProcessSetMetadataFieldType_IndexCascadeKeysOffEnvelope covers the
// schema-change index cascade: the LedgerInfo is Put under the envelope key,
// so the index cascade must use the same key or the two projections split.
func TestProcessSetMetadataFieldType_IndexCascadeKeysOffEnvelope(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	const field = "color"
	indexID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, field)

	expectGetLedger(mockStore, domain.LedgerKey{Name: envelopeLedger},
		(&commonpb.LedgerInfo{Name: divergentLedger, Id: 7}).AsReader(), nil)
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: envelopeLedger}, nil)

	// An index already covers the field, so the BUILDING re-flip cascade runs.
	var probedKeys []domain.IndexKey
	var wroteKey domain.IndexKey

	idxStub := setupIndexesStub(mockStore)
	idxStub.onGet(func(k domain.IndexKey) (commonpb.IndexReader, error) {
		probedKeys = append(probedKeys, k)

		return (&commonpb.Index{
			Id:                     indexID,
			Ledger:                 envelopeLedger,
			ForwardEncodingVersion: 1,
		}).AsReader(), nil
	})
	idxStub.onPut(func(k domain.IndexKey, _ *commonpb.Index) { wroteKey = k })

	order := &raftcmdpb.SetMetadataFieldTypeOrder{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        field,
		Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
	}

	payload, derr := processSetMetadataFieldType(envelopeLedger, order, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)

	require.Len(t, probedKeys, 1)
	require.Equal(t, envelopeLedger, probedKeys[0].LedgerName, "the cascade probe must key off the envelope")
	require.Equal(t, envelopeLedger, wroteKey.LedgerName, "the cascade write must key off the envelope")
}

// TestProcessRemoveMetadataFieldType_IndexCascadeKeysOffEnvelope covers the
// schema-removal index cascade: the dropped index is resolved and deleted
// under the envelope key.
func TestProcessRemoveMetadataFieldType_IndexCascadeKeysOffEnvelope(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	const field = "color"
	indexID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, field)

	expectGetLedger(mockStore, domain.LedgerKey{Name: envelopeLedger},
		(&commonpb.LedgerInfo{Name: divergentLedger, Id: 7}).AsReader(), nil)
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: envelopeLedger}, nil)

	var probedKeys []domain.IndexKey
	var deletedKey domain.IndexKey

	idxStub := setupIndexesStub(mockStore)
	idxStub.onGet(func(k domain.IndexKey) (commonpb.IndexReader, error) {
		probedKeys = append(probedKeys, k)

		return (&commonpb.Index{Id: indexID, Ledger: envelopeLedger}).AsReader(), nil
	})
	idxStub.onDelete(func(k domain.IndexKey) { deletedKey = k })

	order := &raftcmdpb.RemoveMetadataFieldTypeOrder{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        field,
	}

	payload, derr := processRemoveMetadataFieldType(envelopeLedger, order, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)

	require.Len(t, probedKeys, 1)
	require.Equal(t, envelopeLedger, probedKeys[0].LedgerName, "the removal probe must key off the envelope")
	require.Equal(t, envelopeLedger, deletedKey.LedgerName, "the cascade delete must key off the envelope")
}

// TestProcessPromoteLedger_KeysOffEnvelope covers the promotion log: its Name
// must be the envelope. backup/rebuild.go replays PromoteLedger by that name
// to resolve the LedgerInfo it promotes, so a projection-derived name would
// either abort the rebuild on its "invariant: PromoteLedger …" guard or
// promote a different ledger.
func TestProcessPromoteLedger_KeysOffEnvelope(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	expectGetLedger(mockStore, domain.LedgerKey{Name: envelopeLedger},
		(&commonpb.LedgerInfo{
			Name: divergentLedger,
			Id:   7,
			Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
		}).AsReader(), nil)
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: envelopeLedger}, nil)

	payload, derr := processPromoteLedger(envelopeLedger, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)

	require.Equal(t, envelopeLedger, payload.GetPromoteLedger().GetName(),
		"the promotion log must name the envelope ledger, not the divergent projection")
}
