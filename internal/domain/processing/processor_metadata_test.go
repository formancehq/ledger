package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	internalstatepb "github.com/formancehq/ledger/v3/internal/proto/internalstatepb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestProcessAddMetadata_Account(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectDefaultMetadataLimits(mockStore)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &ledgerpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:123"},
		Key:        "status",
	}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&ledgerpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	expectPutAccountMetadata(t, mockStore, metaKey, ledgerpb.NewStringValue("active"))

	request := &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_AddMetadata{
					AddMetadata: &ledgerpb.SaveMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_Account{
								Account: &ledgerpb.TargetAccount{Addr: "users:123"},
							},
						},
						Metadata: map[string]*ledgerpb.MetadataValue{
							"status": ledgerpb.NewStringValue("active"),
						},
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
	require.Equal(t, "test-ledger", applyLog.GetLedgerName())

	savedMetadata := applyLog.GetLog().GetData().GetSavedMetadata()
	require.NotNil(t, savedMetadata)
}

// TestProcessAddMetadata_StoresClientValueVerbatim pins the no-coerce-on-write
// invariant: the FSM stores the exact MetadataValue the client sent, even when
// it does not match the declared type. Read-time coercion is responsible for
// surfacing values in declared_type to clients.
func TestProcessAddMetadata_StoresClientValueVerbatim(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectDefaultMetadataLimits(mockStore)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &ledgerpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &ledgerpb.LedgerInfo{
		Name: "test-ledger",
		Id:   1,
		MetadataSchema: &ledgerpb.MetadataSchema{
			AccountFields: map[string]*ledgerpb.MetadataFieldSchema{
				"age": {Type: ledgerpb.MetadataType_METADATA_TYPE_UINT64},
			},
		},
	}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:123"},
		Key:        "age",
	}

	// The expected stored value is the EXACT client-sent string "030", not a
	// coerced uint64(30). Leading-zero preservation matters for type round-trips.
	clientSent := ledgerpb.NewStringValue("030")

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	expectPutAccountMetadata(t, mockStore, metaKey, clientSent)

	request := &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_AddMetadata{
					AddMetadata: &ledgerpb.SaveMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_Account{
								Account: &ledgerpb.TargetAccount{Addr: "users:123"},
							},
						},
						Metadata: map[string]*ledgerpb.MetadataValue{"age": clientSent},
					},
				}},
			},
		},
	}

	_, err = processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
}

func TestProcessAddMetadata_Transaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectDefaultMetadataLimits(mockStore)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &ledgerpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 5}

	txKey := domain.TransactionKey{LedgerName: "test-ledger", ID: 5}
	existingState := &internalstatepb.TransactionState{
		CreatedByLog: 1,
	}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&ledgerpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	expectGetTransactionState(mockStore, txKey, existingState.AsReader(), nil)
	expectPutTransactionState(t, mockStore, txKey, nil, func(_ domain.TransactionKey, state *internalstatepb.TransactionState) {
		require.NotNil(t, state.GetMetadata())
		require.Len(t, state.GetMetadata(), 1)
		require.Contains(t, state.GetMetadata(), "category")
		require.Equal(t, ledgerpb.NewStringValue("payment"), state.GetMetadata()["category"])
	})

	request := &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_AddMetadata{
					AddMetadata: &ledgerpb.SaveMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_TransactionId{TransactionId: 5},
						},
						Metadata: map[string]*ledgerpb.MetadataValue{
							"category": ledgerpb.NewStringValue("payment"),
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

func TestProcessDeleteMetadata_Account(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectDefaultMetadataLimits(mockStore)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &ledgerpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:123"},
		Key:        "status",
	}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&ledgerpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
	expectGetAccountMetadata(mockStore, metaKey, (&ledgerpb.MetadataValue{}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	expectDeleteAccountMetadata(t, mockStore, metaKey)

	request := &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &ledgerpb.DeleteMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_Account{
								Account: &ledgerpb.TargetAccount{Addr: "users:123"},
							},
						},
						Key: "status",
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

	deletedMetadata := applyLog.GetLog().GetData().GetDeletedMetadata()
	require.NotNil(t, deletedMetadata)
	require.Equal(t, "status", deletedMetadata.GetKey())
}

func TestProcessDeleteMetadata_Account_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectDefaultMetadataLimits(mockStore)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:123"},
		Key:        "status",
	}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&ledgerpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
	expectGetAccountMetadata(mockStore, metaKey, nil, domain.ErrNotFound)

	request := &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &ledgerpb.DeleteMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_Account{
								Account: &ledgerpb.TargetAccount{Addr: "users:123"},
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

	var metaNotFound *domain.ErrMetadataNotFound
	require.ErrorAs(t, err, &metaNotFound)
	require.Equal(t, "users:123", metaNotFound.Target)
	require.Equal(t, "status", metaNotFound.Key)
}

// A present key may hold a nil value (validation accepts nil), which
// GetAccountMetadata reports as (nil, nil). Deleting such a key still succeeds.
func TestProcessDeleteMetadata_Account_NilValueDeletable(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectDefaultMetadataLimits(mockStore)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &ledgerpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:123"},
		Key:        "status",
	}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&ledgerpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
	expectGetAccountMetadata(mockStore, metaKey, nil, nil)
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	expectDeleteAccountMetadata(t, mockStore, metaKey)

	request := &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &ledgerpb.DeleteMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_Account{
								Account: &ledgerpb.TargetAccount{Addr: "users:123"},
							},
						},
						Key: "status",
					},
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	deletedMetadata := result.GetApply().GetLog().GetData().GetDeletedMetadata()
	require.NotNil(t, deletedMetadata)
	require.Equal(t, "status", deletedMetadata.GetKey())
}

func TestProcessAddMetadata_TransactionTargetMissing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectDefaultMetadataLimits(mockStore)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 5}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&ledgerpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()

	request := &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_AddMetadata{
					AddMetadata: &ledgerpb.SaveMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_TransactionId{TransactionId: 0}, // missing id
						},
						Metadata: map[string]*ledgerpb.MetadataValue{
							"status": ledgerpb.NewStringValue("paid"),
						},
					},
				}},
			},
		},
	}

	_, err = processor.ProcessOrder(requestToOrder(request), mockStore)
	require.ErrorIs(t, err, domain.ErrTransactionTargetMissing)
}
