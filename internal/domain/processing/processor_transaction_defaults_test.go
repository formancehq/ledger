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

// ledgerWithDefaults returns a LedgerInfo whose "users:{id}" account type
// carries default_metadata, so the EN-1276 apply-path gate is active.
func ledgerWithDefaults() *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{
		Name: "test-ledger",
		Id:   1,
		AccountTypes: map[string]*commonpb.AccountType{
			"user": {
				Name:    "user",
				Pattern: "users:{id}",
				DefaultMetadata: map[string]*commonpb.MetadataValue{
					"tier": commonpb.NewStringValue("standard"),
				},
			},
		},
	}
}

// expectWorldToAccountPostingMocks wires the volume reads/writes for a
// world -> account posting of `amount` USD. world is the source so the balance
// check is skipped.
func expectWorldToAccountPostingMocks(mockStore *MockScope, account string) {
	worldKey := domain.NewVolumeKey("test-ledger", "world", "USD")
	destKey := domain.NewVolumeKey("test-ledger", account, "USD")
	zero := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetVolume(worldKey).Return(zero.AsReader(), nil)
	mockStore.EXPECT().PutVolume(worldKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(zero.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())
}

// commonTxMocks wires the calls every processCreateTransaction makes that are
// not specific to the default-metadata behaviour under test.
func commonTxMocks(mockStore *MockScope, info *commonpb.LedgerInfo) {
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(info, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
	mockStore.EXPECT().PutTransactionState(gomock.Any(), gomock.Any())
}

func worldToAccountRequest(account string, metadata map[string]*commonpb.MetadataMap) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{
								Source:      "world",
								Destination: account,
								Amount:      commonpb.NewUint256FromUint64(100),
								Asset:       "USD",
							},
						},
						AccountMetadata: metadata,
					},
				}},
			},
		},
	}
}

func TestProcessCreateTransaction_AppliesDefaultMetadataToNewAccount(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	commonTxMocks(mockStore, ledgerWithDefaults())
	expectWorldToAccountPostingMocks(mockStore, "users:alice")

	// New account: marker absent.
	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any())

	// The default key is written.
	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: acctKey, Key: "tier"},
		commonpb.NewStringValue("standard"),
	)

	result, err := processor.ProcessOrder(requestToOrder(worldToAccountRequest("users:alice", nil)), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Defaults also ride into the log payload so they are audited + indexed.
	createdTx := result.GetApply().GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, commonpb.NewStringValue("standard"),
		createdTx.GetAccountMetadata()["users:alice"].GetValues()["tier"])
}

func TestProcessCreateTransaction_ExplicitMetadataWinsOverDefault(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	commonTxMocks(mockStore, ledgerWithDefaults())
	expectWorldToAccountPostingMocks(mockStore, "users:alice")

	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any())

	// Explicit "tier" must win: the default value "standard" must never be written.
	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: acctKey, Key: "tier"},
		commonpb.NewStringValue("premium"),
	)

	explicit := map[string]*commonpb.MetadataMap{
		"users:alice": {Values: map[string]*commonpb.MetadataValue{"tier": commonpb.NewStringValue("premium")}},
	}

	result, err := processor.ProcessOrder(requestToOrder(worldToAccountRequest("users:alice", explicit)), mockStore)
	require.NoError(t, err)

	createdTx := result.GetApply().GetLog().GetData().GetCreatedTransaction()
	require.Equal(t, commonpb.NewStringValue("premium"),
		createdTx.GetAccountMetadata()["users:alice"].GetValues()["tier"])
}

func TestProcessCreateTransaction_ExistingAccountNotDefaulted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	commonTxMocks(mockStore, ledgerWithDefaults())
	expectWorldToAccountPostingMocks(mockStore, "users:alice")

	// Account already exists: marker present. No PutAccount, no default metadata
	// write must happen (gomock fails the test if either is called).
	mockStore.EXPECT().GetAccount(acctKey).Return(&commonpb.AccountState{CreatedByLog: 7}, nil)

	result, err := processor.ProcessOrder(requestToOrder(worldToAccountRequest("users:alice", nil)), mockStore)
	require.NoError(t, err)

	createdTx := result.GetApply().GetLog().GetData().GetCreatedTransaction()
	require.Nil(t, createdTx.GetAccountMetadata()["users:alice"])
}
