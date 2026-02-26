package processing

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestProcessSetMetadataFieldType_Account(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger"}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any()).Do(
		func(_ string, info *commonpb.LedgerInfo) {
			require.NotNil(t, info.MetadataSchema)
			require.NotNil(t, info.MetadataSchema.AccountFields)
			field := info.MetadataSchema.AccountFields["amount"]
			require.NotNil(t, field)
			require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, field.Type)
			require.Equal(t, commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING, field.Status)
		},
	)
	mockStore.EXPECT().AddMetadataConvertRequest("test-ledger", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "amount", commonpb.MetadataType_METADATA_TYPE_INT64)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
					SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
						TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:        "amount",
						Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
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
	setLog := applyLog.Log.Data.GetSetMetadataFieldType()
	require.NotNil(t, setLog)
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, setLog.TargetType)
	require.Equal(t, "amount", setLog.Key)
	require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, setLog.Type)
}

func TestProcessSetMetadataFieldType_Transaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger"}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any()).Do(
		func(_ string, info *commonpb.LedgerInfo) {
			require.NotNil(t, info.MetadataSchema)
			require.NotNil(t, info.MetadataSchema.TransactionFields)
			field := info.MetadataSchema.TransactionFields["priority"]
			require.NotNil(t, field)
			require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, field.Type)
		},
	)
	mockStore.EXPECT().AddMetadataConvertRequest("test-ledger", commonpb.TargetType_TARGET_TYPE_TRANSACTION, "priority", commonpb.MetadataType_METADATA_TYPE_INT64)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
					SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
						TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
						Key:        "priority",
						Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestProcessSetMetadataFieldType_LedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("missing").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("missing").Return(nil, false)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "missing",
				Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
					SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
						TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:        "key",
						Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
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

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{
		Name:           "test-ledger",
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"amount": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		},
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any()).Do(
		func(_ string, info *commonpb.LedgerInfo) {
			_, exists := info.MetadataSchema.AccountFields["amount"]
			require.False(t, exists, "amount field should have been removed")
		},
	)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{
					RemoveMetadataFieldType: &raftcmdpb.RemoveMetadataFieldTypeOrder{
						TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:        "amount",
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
	removeLog := applyLog.Log.Data.GetRemovedMetadataFieldType()
	require.NotNil(t, removeLog)
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, removeLog.TargetType)
	require.Equal(t, "amount", removeLog.Key)
}

func TestProcessRemoveMetadataFieldType_Transaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{
		Name:           "test-ledger",
		MetadataSchema: &commonpb.MetadataSchema{
			TransactionFields: map[string]*commonpb.MetadataFieldSchema{
				"priority": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		},
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any())
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{
					RemoveMetadataFieldType: &raftcmdpb.RemoveMetadataFieldTypeOrder{
						TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
						Key:        "priority",
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestEnforceSchema(t *testing.T) {
	t.Parallel()

	t.Run("NilSchema", func(t *testing.T) {
		t.Parallel()
		metadata := []*commonpb.Metadata{{Key: "k", Value: commonpb.NewStringValue("v")}}
		enforceSchema(nil, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metadata)
		// Should not panic
		require.Equal(t, "v", commonpb.MetadataValueToString(metadata[0].Value))
	})

	t.Run("EmptyMetadata", func(t *testing.T) {
		t.Parallel()
		schema := &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"amount": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		}
		enforceSchema(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, nil)
		// Should not panic
	})

	t.Run("NoFieldsForTarget", func(t *testing.T) {
		t.Parallel()
		schema := &commonpb.MetadataSchema{
			// Only account fields, no transaction fields
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"amount": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		}
		metadata := []*commonpb.Metadata{{Key: "amount", Value: commonpb.NewStringValue("42")}}
		enforceSchema(schema, commonpb.TargetType_TARGET_TYPE_TRANSACTION, metadata)
		// Should not modify since no transaction fields
		require.Equal(t, "42", commonpb.MetadataValueToString(metadata[0].Value))
	})

	t.Run("UndeclaredKey", func(t *testing.T) {
		t.Parallel()
		schema := &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"amount": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		}
		metadata := []*commonpb.Metadata{{Key: "status", Value: commonpb.NewStringValue("active")}}
		enforceSchema(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metadata)
		// "status" is not declared, should stay as string
		require.Equal(t, "active", commonpb.MetadataValueToString(metadata[0].Value))
	})

	t.Run("NilValue", func(t *testing.T) {
		t.Parallel()
		schema := &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"amount": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		}
		metadata := []*commonpb.Metadata{{Key: "amount", Value: nil}}
		enforceSchema(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metadata)
		// Nil value should be left as nil
		require.Nil(t, metadata[0].Value)
	})
}

func TestPopulateInitialSchema(t *testing.T) {
	t.Parallel()

	t.Run("NilCommands", func(t *testing.T) {
		t.Parallel()
		result := populateInitialSchema(nil)
		require.Nil(t, result)
	})

	t.Run("EmptyCommands", func(t *testing.T) {
		t.Parallel()
		result := populateInitialSchema([]*commonpb.SetMetadataFieldTypeCommand{})
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
		result := populateInitialSchema(commands)
		require.NotNil(t, result)
		require.NotNil(t, result.AccountFields)
		field := result.AccountFields["amount"]
		require.NotNil(t, field)
		require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, field.Type)
		require.Equal(t, commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE, field.Status)
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
		result := populateInitialSchema(commands)
		require.NotNil(t, result)
		require.NotNil(t, result.TransactionFields)
		field := result.TransactionFields["priority"]
		require.NotNil(t, field)
		require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, field.Type)
		require.Equal(t, commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE, field.Status)
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
		}
		result := populateInitialSchema(commands)
		require.NotNil(t, result)
		require.Len(t, result.AccountFields, 1)
		require.Len(t, result.TransactionFields, 1)
	})
}

func TestSchemaFieldForTarget(t *testing.T) {
	t.Parallel()

	t.Run("NilSchema", func(t *testing.T) {
		t.Parallel()
		fields, field := schemaFieldForTarget(nil, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "key")
		require.Nil(t, fields)
		require.Nil(t, field)
	})

	t.Run("NilFieldMap", func(t *testing.T) {
		t.Parallel()
		schema := &commonpb.MetadataSchema{} // No AccountFields or TransactionFields
		fields, field := schemaFieldForTarget(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "key")
		require.Nil(t, fields)
		require.Nil(t, field)
	})

	t.Run("KeyNotFound", func(t *testing.T) {
		t.Parallel()
		schema := &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"amount": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		}
		fields, field := schemaFieldForTarget(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "nonexistent")
		require.NotNil(t, fields)
		require.Nil(t, field)
	})

	t.Run("KeyFound", func(t *testing.T) {
		t.Parallel()
		schema := &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"amount": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		}
		fields, field := schemaFieldForTarget(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "amount")
		require.NotNil(t, fields)
		require.NotNil(t, field)
		require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, field.Type)
	})

	t.Run("TransactionField", func(t *testing.T) {
		t.Parallel()
		schema := &commonpb.MetadataSchema{
			TransactionFields: map[string]*commonpb.MetadataFieldSchema{
				"priority": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		}
		fields, field := schemaFieldForTarget(schema, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "priority")
		require.NotNil(t, fields)
		require.NotNil(t, field)
	})
}
