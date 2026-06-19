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

func TestProcessAddMetadata_NilTarget(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
					AddMetadata: &raftcmdpb.SaveMetadataOrder{
						Target:   nil,
						Metadata: map[string]*commonpb.MetadataValue{},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, domain.ErrTargetRequired)
}

func TestProcessAddMetadata_WithSchema(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	ledgerInfo := &commonpb.LedgerInfo{
		Name: "test-ledger",
		Id:   1,
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"age": {
					Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE,
				},
			},
		},
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo.AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetAccountMetadata(gomock.Any()).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccountMetadata(gomock.Any(), gomock.Any())
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:001"},
							},
						},
						Metadata: map[string]*commonpb.MetadataValue{
							"age": commonpb.NewStringValue("25"),
						},
					},
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestProcessAddMetadata_TransactionNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{
									Identifier: &commonpb.TargetTransaction_Id{Id: 99},
								}, // Beyond NextTransactionId=5
							},
						},
						Metadata: map[string]*commonpb.MetadataValue{
							"status": commonpb.NewStringValue("done"),
						},
					},
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var txNotFound *domain.ErrTransactionNotFound
	require.ErrorAs(t, err, &txNotFound)
	require.Equal(t, uint64(99), txNotFound.TransactionID)
}

func TestProcessDeleteMetadata_NilTarget(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
					DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
						Target: nil,
						Key:    "status",
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, domain.ErrTargetRequired)
}

func TestProcessDeleteMetadata_EmptyKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
					DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:123"},
							},
						},
						Key: "",
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, domain.ErrMetadataKeyRequired)
}

func TestProcessDeleteMetadata_Transaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 5}

	txKey := domain.TransactionKey{LedgerName: "test-ledger", ID: 3}
	existingState := &commonpb.TransactionState{
		CreatedByLog: 1,
		Metadata: map[string]*commonpb.MetadataValue{
			"category": commonpb.NewStringValue("expense"),
			"status":   commonpb.NewStringValue("active"),
		},
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetTransactionState(txKey).Return(existingState.AsReader(), nil)
	mockStore.EXPECT().PutTransactionState(txKey, gomock.Any()).Do(
		func(_ domain.TransactionKey, state *commonpb.TransactionState) {
			// "category" should be removed, only "status" remains
			require.NotNil(t, state.GetMetadata())
			require.Len(t, state.GetMetadata(), 1)
			require.Contains(t, state.GetMetadata(), "status")
		},
	)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{
									Identifier: &commonpb.TargetTransaction_Id{Id: 3},
								},
							},
						},
						Key: "category",
					},
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	deletedMeta := applyLog.GetLog().GetData().GetDeletedMetadata()
	require.NotNil(t, deletedMeta)
	require.Equal(t, "category", deletedMeta.GetKey())
}

func TestProcessDeleteMetadata_TransactionNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 5, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{
									Identifier: &commonpb.TargetTransaction_Id{Id: 99},
								},
							},
						},
						Key: "status",
					},
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var txNotFound *domain.ErrTransactionNotFound
	require.ErrorAs(t, err, &txNotFound)
}
