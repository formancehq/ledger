package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
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
