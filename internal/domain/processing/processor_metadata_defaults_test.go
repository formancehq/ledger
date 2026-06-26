package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// saveAccountMetadataRequest builds a SaveMetadata request targeting an account.
func saveAccountMetadataRequest(account string, metadata map[string]*commonpb.MetadataValue) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: account},
							},
						},
						Metadata: metadata,
					},
				}},
			},
		},
	}
}

// addMetadataCommonMocks wires the processApply-level calls every AddMetadata
// order makes regardless of the default-metadata behaviour under test.
func addMetadataCommonMocks(mockStore *MockScope, info *commonpb.LedgerInfo) {
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetLedger("test-ledger").Return(info.AsReader(), nil).AnyTimes()
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
}

// TestProcessAddMetadata_NewAccount_MergesDefaults verifies a metadata-set that
// first creates an account writes the existence marker and merges the matching
// account type's default_metadata for keys the caller did not set explicitly.
func TestProcessAddMetadata_NewAccount_MergesDefaults(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	addMetadataCommonMocks(mockStore, ledgerWithDefaults())

	// New account: marker written exactly once.
	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound).Times(1)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any()).Times(1)

	// Both the explicit key and the merged default are applied.
	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: acctKey, Key: "note"}, commonpb.NewStringValue("hello"))
	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: acctKey, Key: "tier"}, commonpb.NewStringValue("standard"))

	result, err := processor.ProcessOrder(requestToOrder(saveAccountMetadataRequest("users:alice",
		map[string]*commonpb.MetadataValue{"note": commonpb.NewStringValue("hello")})), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	// The logged metadata carries both keys so replay/rebuild reconstruct them.
	logged := result.GetApply().GetLog().GetData().GetSavedMetadata().GetMetadata()
	require.Equal(t, commonpb.NewStringValue("hello"), logged["note"])
	require.Equal(t, commonpb.NewStringValue("standard"), logged["tier"])
}

// TestProcessAddMetadata_NewAccount_ExplicitOverridesDefault verifies an explicit
// value for a default key wins (the default is not merged over it).
func TestProcessAddMetadata_NewAccount_ExplicitOverridesDefault(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	addMetadataCommonMocks(mockStore, ledgerWithDefaults())

	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound).Times(1)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any()).Times(1)

	// Only the explicit tier=gold is applied; the default tier=standard never wins.
	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: acctKey, Key: "tier"}, commonpb.NewStringValue("gold"))

	result, err := processor.ProcessOrder(requestToOrder(saveAccountMetadataRequest("users:alice",
		map[string]*commonpb.MetadataValue{"tier": commonpb.NewStringValue("gold")})), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	logged := result.GetApply().GetLog().GetData().GetSavedMetadata().GetMetadata()
	require.Equal(t, commonpb.NewStringValue("gold"), logged["tier"])
	require.Len(t, logged, 1)
}

// TestProcessAddMetadata_ExistingAccount_NoDefaults verifies that setting
// metadata on an already-existing account writes no marker and merges no
// defaults — defaults apply only on first creation.
func TestProcessAddMetadata_ExistingAccount_NoDefaults(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	addMetadataCommonMocks(mockStore, ledgerWithDefaults())

	// Account already exists: marker present, so no PutAccount and no defaults.
	mockStore.EXPECT().GetAccount(acctKey).Return((&commonpb.AccountState{}).AsReader(), nil).Times(1)
	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: acctKey, Key: "note"}, commonpb.NewStringValue("hi"))

	result, err := processor.ProcessOrder(requestToOrder(saveAccountMetadataRequest("users:alice",
		map[string]*commonpb.MetadataValue{"note": commonpb.NewStringValue("hi")})), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	logged := result.GetApply().GetLog().GetData().GetSavedMetadata().GetMetadata()
	require.Equal(t, commonpb.NewStringValue("hi"), logged["note"])
	require.NotContains(t, logged, "tier")
}

// TestProcessAddMetadata_GateOff_NoMarker verifies that on a ledger with no
// default-bearing account types the marker path is skipped entirely — GetAccount
// and PutAccount are never called (the strict mock fails if they are).
func TestProcessAddMetadata_GateOff_NoMarker(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	// Ledger declares no defaults → gate off.
	addMetadataCommonMocks(mockStore, ledgerInfoWithID("test-ledger", 1))

	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: acctKey, Key: "note"}, commonpb.NewStringValue("hi"))

	result, err := processor.ProcessOrder(requestToOrder(saveAccountMetadataRequest("users:alice",
		map[string]*commonpb.MetadataValue{"note": commonpb.NewStringValue("hi")})), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	logged := result.GetApply().GetLog().GetData().GetSavedMetadata().GetMetadata()
	require.Equal(t, commonpb.NewStringValue("hi"), logged["note"])
	require.Len(t, logged, 1)
}
