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

func TestProcessAddMetadata_Account(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:123"},
		Key:        "status",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccountMetadata(metaKey, commonpb.NewStringValue("active"))

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:123"},
							},
						},
						Metadata: map[string]*commonpb.MetadataValue{
							"status": commonpb.NewStringValue("active"),
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
	// No previous values since this is a first write
	require.Empty(t, savedMetadata.GetPreviousValues())
}

func TestProcessAddMetadata_Transaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 5}

	txKey := domain.TransactionKey{LedgerName: "test-ledger", ID: 5}
	existingState := &commonpb.TransactionState{
		CreatedByLog: 1,
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetTransactionState(txKey).Return(existingState.AsReader(), nil)
	mockStore.EXPECT().PutTransactionState(txKey, gomock.Any()).Do(
		func(_ domain.TransactionKey, state *commonpb.TransactionState) {
			require.NotNil(t, state.GetMetadata())
			require.Len(t, state.GetMetadata(), 1)
			require.Contains(t, state.GetMetadata(), "category")
			require.Equal(t, commonpb.NewStringValue("payment"), state.GetMetadata()["category"])
		},
	)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{
									Identifier: &commonpb.TargetTransaction_Id{Id: 5},
								},
							},
						},
						Metadata: map[string]*commonpb.MetadataValue{
							"category": commonpb.NewStringValue("payment"),
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

func TestProcessAddMetadata_CoercesPreviousValue(t *testing.T) {
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
				"age": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		},
	}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:123"},
		Key:        "age",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo.AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	// The stored value is still raw (written before the type was declared; the
	// background conversion has not rewritten it yet).
	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(commonpb.NewStringValue("30"), nil)
	mockStore.EXPECT().PutAccountMetadata(metaKey, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:123"},
							},
						},
						Metadata: map[string]*commonpb.MetadataValue{
							"age": commonpb.NewStringValue("40"),
						},
					},
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	saved := result.GetApply().GetLog().GetData().GetSavedMetadata()
	require.NotNil(t, saved)
	prev := saved.GetPreviousValues()["age"]
	require.NotNil(t, prev)
	require.Equal(t, int64(30), prev.GetIntValue(),
		"previous value must be coerced to the declared INT64 type, not left as raw string")
}

func TestProcessDeleteMetadata_CoercesPreviousValue(t *testing.T) {
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
				"age": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		},
	}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:123"},
		Key:        "age",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo.AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(commonpb.NewStringValue("30"), nil)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().DeleteAccountMetadata(metaKey)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:123"},
							},
						},
						Key: "age",
					},
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	deleted := result.GetApply().GetLog().GetData().GetDeletedMetadata()
	require.NotNil(t, deleted)
	require.Equal(t, int64(30), deleted.GetPreviousValue().GetIntValue(),
		"previous value must be coerced to the declared INT64 type, not left as raw string")
}

func TestProcessDeleteMetadata_Account(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:123"},
		Key:        "status",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetAccountMetadata(metaKey).Return((&commonpb.MetadataValue{}).AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().DeleteAccountMetadata(metaKey)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:123"},
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
	require.NotNil(t, deletedMetadata.GetPreviousValue())
}

func TestProcessDeleteMetadata_Account_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:123"},
		Key:        "status",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(nil, domain.ErrNotFound)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:123"},
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

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:123"},
		Key:        "status",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(nil, nil)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().DeleteAccountMetadata(metaKey)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:123"},
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

func TestProcessAddMetadata_TransactionByReference(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 5}
	refKey := domain.TransactionReferenceKey{LedgerName: "test-ledger", Reference: "invoice:42"}
	txKey := domain.TransactionKey{LedgerName: "test-ledger", ID: 7}
	existingState := &commonpb.TransactionState{CreatedByLog: 3}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetTransactionReference(refKey).Return((&commonpb.TransactionReferenceValue{TransactionId: 7}).AsReader(), nil)
	mockStore.EXPECT().GetTransactionState(txKey).Return(existingState.AsReader(), nil)
	mockStore.EXPECT().PutTransactionState(txKey, gomock.Any()).Do(
		func(_ domain.TransactionKey, state *commonpb.TransactionState) {
			require.Equal(t, commonpb.NewStringValue("paid"), state.GetMetadata()["status"])
		},
	)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{
									Identifier: &commonpb.TargetTransaction_Reference{Reference: "invoice:42"},
								},
							},
						},
						Metadata: map[string]*commonpb.MetadataValue{
							"status": commonpb.NewStringValue("paid"),
						},
					},
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	// The logged target MUST be canonicalised to the Id variant so
	// downstream consumers (sinks, indexbuilder, replay, mirror) read a
	// stable numeric id rather than a Reference variant they don't know
	// how to handle.
	saved := result.GetApply().GetLog().GetData().GetSavedMetadata()
	require.NotNil(t, saved)

	canonical, ok := saved.GetTarget().GetTarget().(*commonpb.Target_Transaction)
	require.True(t, ok)
	require.Equal(t, uint64(7), canonical.Transaction.GetId(), "logged target must carry resolved id")
	require.Empty(t, canonical.Transaction.GetReference(), "logged target must not carry the original reference")
}

func TestProcessAddMetadata_TransactionByReferenceNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 5}
	refKey := domain.TransactionReferenceKey{LedgerName: "test-ledger", Reference: "unknown"}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetTransactionReference(refKey).Return(nil, domain.ErrNotFound)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{
									Identifier: &commonpb.TargetTransaction_Reference{Reference: "unknown"},
								},
							},
						},
						Metadata: map[string]*commonpb.MetadataValue{
							"status": commonpb.NewStringValue("paid"),
						},
					},
				}},
			},
		},
	}

	_, err = processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)

	var refNotFound *domain.ErrTransactionReferenceNotFound
	require.ErrorAs(t, err, &refNotFound)
	require.Equal(t, "unknown", refNotFound.Reference)
}

func TestProcessAddMetadata_TransactionTargetMissing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 5}

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
								Transaction: &commonpb.TargetTransaction{}, // empty identifier
							},
						},
						Metadata: map[string]*commonpb.MetadataValue{
							"status": commonpb.NewStringValue("paid"),
						},
					},
				}},
			},
		},
	}

	_, err = processor.ProcessOrder(requestToOrder(request), mockStore)
	require.ErrorIs(t, err, domain.ErrTransactionTargetMissing)
}

func TestProcessDeleteMetadata_Transaction_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 5}
	refKey := domain.TransactionReferenceKey{LedgerName: "test-ledger", Reference: "invoice:42"}
	txKey := domain.TransactionKey{LedgerName: "test-ledger", ID: 7}
	existingState := &commonpb.TransactionState{
		CreatedByLog: 3,
		Metadata:     map[string]*commonpb.MetadataValue{"status": commonpb.NewStringValue("paid")},
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetTransactionReference(refKey).Return((&commonpb.TransactionReferenceValue{TransactionId: 7}).AsReader(), nil)
	mockStore.EXPECT().GetTransactionState(txKey).Return(existingState.AsReader(), nil)
	// No PutTransactionState / PutBoundaries: deleting a missing key is rejected.

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{
									Identifier: &commonpb.TargetTransaction_Reference{Reference: "invoice:42"},
								},
							},
						},
						Key: "missing",
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
	require.Equal(t, "missing", metaNotFound.Key)
}

func TestProcessDeleteMetadata_TransactionByReference(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 5}
	refKey := domain.TransactionReferenceKey{LedgerName: "test-ledger", Reference: "invoice:42"}
	txKey := domain.TransactionKey{LedgerName: "test-ledger", ID: 7}
	existingState := &commonpb.TransactionState{
		CreatedByLog: 3,
		Metadata:     map[string]*commonpb.MetadataValue{"status": commonpb.NewStringValue("paid")},
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), true)
	mockStore.EXPECT().GetLedger("test-ledger").Return((&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetTransactionReference(refKey).Return((&commonpb.TransactionReferenceValue{TransactionId: 7}).AsReader(), nil)
	mockStore.EXPECT().GetTransactionState(txKey).Return(existingState.AsReader(), nil)
	mockStore.EXPECT().PutTransactionState(txKey, gomock.Any()).Do(
		func(_ domain.TransactionKey, state *commonpb.TransactionState) {
			_, hasStatus := state.GetMetadata()["status"]
			require.False(t, hasStatus, "status metadata should have been removed")
		},
	)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{
									Identifier: &commonpb.TargetTransaction_Reference{Reference: "invoice:42"},
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
	require.NoError(t, err)
	require.NotNil(t, result)
}
