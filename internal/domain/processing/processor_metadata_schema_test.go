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

func TestProcessSetMetadataFieldType_Account(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger", Id: 1}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectGetIndex(mockStore, domain.IndexKey{}, nil, domain.ErrNotFound).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, func(_ string, info *commonpb.LedgerInfo) {
		require.NotNil(t, info.GetMetadataSchema())
		require.NotNil(t, info.GetMetadataSchema().GetAccountFields())
		field := info.GetMetadataSchema().GetAccountFields()["amount"]
		require.NotNil(t, field)
		require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, field.GetType())
	})
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
						SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "amount",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)
	setLog := applyLog.GetLog().GetData().GetSetMetadataFieldType()
	require.NotNil(t, setLog)
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, setLog.GetTargetType())
	require.Equal(t, "amount", setLog.GetKey())
	require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, setLog.GetType())
}

func TestProcessSetMetadataFieldType_Transaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger", Id: 1}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectGetIndex(mockStore, domain.IndexKey{}, nil, domain.ErrNotFound).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, func(_ string, info *commonpb.LedgerInfo) {
		require.NotNil(t, info.GetMetadataSchema())
		require.NotNil(t, info.GetMetadataSchema().GetTransactionFields())
		field := info.GetMetadataSchema().GetTransactionFields()["priority"]
		require.NotNil(t, field)
		require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, field.GetType())
	})
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
						SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
							TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
							Key:        "priority",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestProcessSetMetadataFieldType_Ledger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger", Id: 1}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectGetIndex(mockStore, domain.IndexKey{}, nil, domain.ErrNotFound).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, func(_ string, info *commonpb.LedgerInfo) {
		require.NotNil(t, info.GetMetadataSchema())
		require.NotNil(t, info.GetMetadataSchema().GetLedgerFields())
		field := info.GetMetadataSchema().GetLedgerFields()["env"]
		require.NotNil(t, field)
		require.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, field.GetType())
	})
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
						SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
							TargetType: commonpb.TargetType_TARGET_TYPE_LEDGER,
							Key:        "env",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)
	setLog := applyLog.GetLog().GetData().GetSetMetadataFieldType()
	require.NotNil(t, setLog)
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_LEDGER, setLog.GetTargetType())
	require.Equal(t, "env", setLog.GetKey())
	require.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, setLog.GetType())
}

func TestProcessSetMetadataFieldType_LedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "missing"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "missing"}, nil, domain.ErrNotFound).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "missing",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
						SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "key",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var ledgerNotFound *domain.ErrLedgerNotFound
	require.ErrorAs(t, err, &ledgerNotFound)
}

func TestProcessRemoveMetadataFieldType_Account(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{
		Name: "test-ledger",
		Id:   1,
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"amount": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		},
	}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectGetIndex(mockStore, domain.IndexKey{}, nil, domain.ErrNotFound).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, func(_ string, info *commonpb.LedgerInfo) {
		_, exists := info.GetMetadataSchema().GetAccountFields()["amount"]
		require.False(t, exists, "amount field should have been removed")
	})
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{
						RemoveMetadataFieldType: &raftcmdpb.RemoveMetadataFieldTypeOrder{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "amount",
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)
	removeLog := applyLog.GetLog().GetData().GetRemovedMetadataFieldType()
	require.NotNil(t, removeLog)
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, removeLog.GetTargetType())
	require.Equal(t, "amount", removeLog.GetKey())
}

func TestProcessRemoveMetadataFieldType_Transaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{
		Name: "test-ledger",
		Id:   1,
		MetadataSchema: &commonpb.MetadataSchema{
			TransactionFields: map[string]*commonpb.MetadataFieldSchema{
				"priority": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		},
	}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectGetIndex(mockStore, domain.IndexKey{}, nil, domain.ErrNotFound).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{
						RemoveMetadataFieldType: &raftcmdpb.RemoveMetadataFieldTypeOrder{
							TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
							Key:        "priority",
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestProcessRemoveMetadataFieldType_Ledger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{
		Name: "test-ledger",
		Id:   1,
		MetadataSchema: &commonpb.MetadataSchema{
			LedgerFields: map[string]*commonpb.MetadataFieldSchema{
				"env": {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
			},
		},
	}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectGetIndex(mockStore, domain.IndexKey{}, nil, domain.ErrNotFound).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, func(_ string, info *commonpb.LedgerInfo) {
		_, exists := info.GetMetadataSchema().GetLedgerFields()["env"]
		require.False(t, exists, "env field should have been removed")
	})
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{
						RemoveMetadataFieldType: &raftcmdpb.RemoveMetadataFieldTypeOrder{
							TargetType: commonpb.TargetType_TARGET_TYPE_LEDGER,
							Key:        "env",
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)
	removeLog := applyLog.GetLog().GetData().GetRemovedMetadataFieldType()
	require.NotNil(t, removeLog)
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_LEDGER, removeLog.GetTargetType())
	require.Equal(t, "env", removeLog.GetKey())
}

// TestProcessSetMetadataFieldType_AcceptedDuringRebuild pins the O(1) retype
// contract: a second SetMetadataFieldType for a key whose index is still
// BUILDING is accepted immediately and re-flips the index status to BUILDING
// for the new declared_type. No transient error, no waiting.
func TestProcessSetMetadataFieldType_AcceptedDuringRebuild(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "amount")
	ledgerInfo := &commonpb.LedgerInfo{
		Name: "test-ledger",
		Id:   1,
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"amount": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		},
	}
	existingIndex := &commonpb.Index{
		Ledger: "test-ledger",
		Id:     id,
	}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	expectGetIndex(mockStore, indexes.KeyFor("test-ledger", id), existingIndex.AsReader(), nil)
	expectPutIndex(t, mockStore, indexes.KeyFor("test-ledger", id), nil)
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890})
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
						SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "amount",
							Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err, "retype must succeed even when the index is mid-rebuild")
	require.NotNil(t, result)
}

func TestPopulateInitialSchema(t *testing.T) {
	t.Parallel()

	t.Run("NilCommands", func(t *testing.T) {
		t.Parallel()

		result := populateInitialSchema(nil, 1)
		require.Nil(t, result)
	})

	t.Run("EmptyCommands", func(t *testing.T) {
		t.Parallel()

		result := populateInitialSchema([]*commonpb.SetMetadataFieldTypeCommand{}, 1)
		require.Nil(t, result)
	})

	t.Run("AccountFields", func(t *testing.T) {
		t.Parallel()

		commands := []*commonpb.SetMetadataFieldTypeCommand{
			{
				TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:        "amount",
				Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
			},
		}
		result := populateInitialSchema(commands, 1)
		require.NotNil(t, result)
		require.NotNil(t, result.GetAccountFields())
		field := result.GetAccountFields()["amount"]
		require.NotNil(t, field)
		require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, field.GetType())
	})

	t.Run("TransactionFields", func(t *testing.T) {
		t.Parallel()

		commands := []*commonpb.SetMetadataFieldTypeCommand{
			{
				TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
				Key:        "priority",
				Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
			},
		}
		result := populateInitialSchema(commands, 1)
		require.NotNil(t, result)
		require.NotNil(t, result.GetTransactionFields())
		field := result.GetTransactionFields()["priority"]
		require.NotNil(t, field)
		require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, field.GetType())
	})

	t.Run("LedgerFields", func(t *testing.T) {
		t.Parallel()

		commands := []*commonpb.SetMetadataFieldTypeCommand{
			{
				TargetType: commonpb.TargetType_TARGET_TYPE_LEDGER,
				Key:        "env",
				Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
			},
		}
		result := populateInitialSchema(commands, 1)
		require.NotNil(t, result)
		require.NotNil(t, result.GetLedgerFields())
		field := result.GetLedgerFields()["env"]
		require.NotNil(t, field)
		require.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, field.GetType())
	})

	t.Run("MixedFields", func(t *testing.T) {
		t.Parallel()

		commands := []*commonpb.SetMetadataFieldTypeCommand{
			{
				TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:        "balance",
				Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
			},
			{
				TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
				Key:        "category",
				Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
			},
			{
				TargetType: commonpb.TargetType_TARGET_TYPE_LEDGER,
				Key:        "region",
				Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
			},
		}
		result := populateInitialSchema(commands, 1)
		require.NotNil(t, result)
		require.Len(t, result.GetAccountFields(), 1)
		require.Len(t, result.GetTransactionFields(), 1)
		require.Len(t, result.GetLedgerFields(), 1)
	})
}

// TestProcessSetMetadataFieldType_StampsRevision pins the revision counter:
// the first declaration of a key is revision 1, each retype increments it,
// and the minted log carries the post-apply revision — the mint-time stamp a
// replica folding at any replay distance binds the rewrite's target version
// to.
func TestProcessSetMetadataFieldType_StampsRevision(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	// The ledger already carries "amount" at revision 2 in lineage 5: this
	// retype must stamp revision 3 and CARRY lineage 5, in the schema and in
	// the log. A retype is not a new declaration lineage.
	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger", Id: 1, MetadataSchema: &commonpb.MetadataSchema{
		AccountFields: map[string]*commonpb.MetadataFieldSchema{
			"amount": {Type: commonpb.MetadataType_METADATA_TYPE_STRING, Revision: 2, Incarnation: 5},
		},
	}}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectGetIndex(mockStore, domain.IndexKey{}, nil, domain.ErrNotFound).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, func(_ string, info *commonpb.LedgerInfo) {
		field := info.GetMetadataSchema().GetAccountFields()["amount"]
		require.NotNil(t, field)
		require.Equal(t, uint32(3), field.GetRevision())
		require.Equal(t, uint64(5), field.GetIncarnation(), "a retype stays in the field's declaration lineage")
	})
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
						SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "amount",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)

	setLog := result.GetApply().GetLog().GetData().GetSetMetadataFieldType()
	require.NotNil(t, setLog)
	require.Equal(t, uint32(3), setLog.GetRevision())
	require.Equal(t, uint64(5), setLog.GetIncarnation())
}

// TestProcessSetMetadataFieldType_OpensLineageAfterRemoval pins the other half
// of the stamp: a declaration with no prior entry — a first declaration, or one
// after a removal deleted it — opens a NEW lineage identified by its own
// ledger-log id, and restarts the revision. Without a fresh id here, a rewound
// read store's binding from the dropped incarnation would compare favourably
// against the re-declared schema and serve a purged keyspace.
func TestProcessSetMetadataFieldType_OpensLineageAfterRemoval(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	// The ledger has run for a while: the next ledger-log id is 41, so that is
	// the id this declaration's own log gets and therefore its lineage.
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 41}

	// "amount" was declared and removed earlier, so the schema holds no entry.
	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger", Id: 1, MetadataSchema: &commonpb.MetadataSchema{
		AccountFields: map[string]*commonpb.MetadataFieldSchema{},
	}}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectGetIndex(mockStore, domain.IndexKey{}, nil, domain.ErrNotFound).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, func(_ string, info *commonpb.LedgerInfo) {
		field := info.GetMetadataSchema().GetAccountFields()["amount"]
		require.NotNil(t, field)
		require.Equal(t, uint32(1), field.GetRevision(), "a re-declaration restarts the revision")
		require.Equal(t, uint64(41), field.GetIncarnation(), "and opens a lineage at its own log id")
	})
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
						SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "amount",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)

	setLog := result.GetApply().GetLog().GetData().GetSetMetadataFieldType()
	require.NotNil(t, setLog)
	require.Equal(t, uint32(1), setLog.GetRevision())
	require.Equal(t, uint64(41), setLog.GetIncarnation(),
		"the log carries the lineage so every replica binds the new incarnation identically")
	require.Equal(t, uint64(41), result.GetApply().GetLog().GetId(),
		"the lineage id IS this log's id — the value processApply stamps after the handler returns")
}
