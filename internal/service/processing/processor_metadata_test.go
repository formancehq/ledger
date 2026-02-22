package processing

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestProcessAddMetadata_Account(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().PutAccountMetadata(
		dal.MetadataKey{
			AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:123"},
			Key:        "status",
		},
		commonpb.NewStringValue("active"),
	)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:123"},
							},
						},
						Metadata: &commonpb.MetadataSet{
							Metadata: []*commonpb.Metadata{
								{Key: "status", Value: commonpb.NewStringValue("active")},
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)
	require.Equal(t, "test-ledger", applyLog.LedgerName)

	savedMetadata := applyLog.Log.Data.GetSavedMetadata()
	require.NotNil(t, savedMetadata)
}

func TestProcessAddMetadata_Transaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 5, LedgerId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(42))
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 5}, gomock.Any()).Do(
		func(key dal.TransactionKey, update *commonpb.TransactionUpdate) {
			require.Equal(t, uint64(42), update.ByLog) // Global sequence ID
			require.Len(t, update.Updates, 1)
			addMeta := update.Updates[0].GetTransactionModificationAddMetadata()
			require.NotNil(t, addMeta)
			require.Equal(t, "category", addMeta.Metadata.Key)
		},
	)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{Id: 5},
							},
						},
						Metadata: &commonpb.MetadataSet{
							Metadata: []*commonpb.Metadata{
								{Key: "category", Value: commonpb.NewStringValue("payment")},
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestProcessDeleteMetadata_Account(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	metaKey := dal.MetadataKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:123"},
		Key:        "status",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(&commonpb.MetadataValue{}, nil)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().DeleteAccountMetadata(metaKey)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:123"},
							},
						},
						Key: "status",
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	deletedMetadata := applyLog.Log.Data.GetDeletedMetadata()
	require.NotNil(t, deletedMetadata)
	require.Equal(t, "status", deletedMetadata.Key)
}

func TestProcessDeleteMetadata_Account_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}
	metaKey := dal.MetadataKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:123"},
		Key:        "status",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(nil, dal.ErrNotFound)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:123"},
							},
						},
						Key: "status",
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var metaNotFound *ErrMetadataNotFound
	require.ErrorAs(t, err, &metaNotFound)
	require.Equal(t, "users:123", metaNotFound.Target)
	require.Equal(t, "status", metaNotFound.Key)
}
