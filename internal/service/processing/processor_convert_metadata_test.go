package processing

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestProcessConvertMetadataBatch_LedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_ConvertMetadataBatch{
					ConvertMetadataBatch: &raftcmdpb.ConvertMetadataBatchOrder{
						TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:          "age",
						ExpectedType: commonpb.MetadataType_METADATA_TYPE_INT64,
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

func TestProcessConvertMetadataBatch_StaleSchema(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	// Ledger exists but the schema field is no longer CONVERTING
	ledgerInfo := &commonpb.LedgerInfo{
		Name:           "test-ledger",
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"age": {
					Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE, // Not CONVERTING
				},
			},
		},
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_ConvertMetadataBatch{
					ConvertMetadataBatch: &raftcmdpb.ConvertMetadataBatchOrder{
						TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:          "age",
						ExpectedType: commonpb.MetadataType_METADATA_TYPE_INT64,
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

	batchLog := applyLog.Log.Data.GetConvertMetadataBatch()
	require.NotNil(t, batchLog)
	require.Equal(t, uint32(0), batchLog.Count, "stale batch should report count=0")
}

func TestProcessConvertMetadataBatch_Success(t *testing.T) {
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
				"age": {
					Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
				},
			},
		},
	}

	// Build a canonical key for the metadata entry
	mk := domain.MetadataKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "user:001"},
		Key:        "age",
	}
	canonicalKey := mk.Bytes()

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
	// The entry value does NOT match the expected type, so it should be converted
	mockStore.EXPECT().GetAccountMetadata(mk).Return(commonpb.NewStringValue("25"), nil)
	mockStore.EXPECT().PutAccountMetadata(mk, gomock.Any())
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any())
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	convertedValue := commonpb.NewIntValue(25)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_ConvertMetadataBatch{
					ConvertMetadataBatch: &raftcmdpb.ConvertMetadataBatchOrder{
						TargetType:         commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:                "age",
						ExpectedType:       commonpb.MetadataType_METADATA_TYPE_INT64,
						TotalKeys:          10,
						ConvertedKeysSoFar: 5,
						Entries: []*raftcmdpb.ConvertMetadataEntry{
							{
								CanonicalKey:   canonicalKey,
								ConvertedValue: convertedValue,
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

	batchLog := applyLog.Log.Data.GetConvertMetadataBatch()
	require.NotNil(t, batchLog)
	require.Equal(t, uint32(1), batchLog.Count)
}

func TestProcessConvertMetadataBatch_AlreadyMatchesType(t *testing.T) {
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
				"age": {
					Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
				},
			},
		},
	}

	mk := domain.MetadataKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "user:001"},
		Key:        "age",
	}
	canonicalKey := mk.Bytes()

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
	// Value already matches the expected type -- no PutAccountMetadata expected
	mockStore.EXPECT().GetAccountMetadata(mk).Return(commonpb.NewIntValue(25), nil)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any())
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_ConvertMetadataBatch{
					ConvertMetadataBatch: &raftcmdpb.ConvertMetadataBatchOrder{
						TargetType:         commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:                "age",
						ExpectedType:       commonpb.MetadataType_METADATA_TYPE_INT64,
						TotalKeys:          10,
						ConvertedKeysSoFar: 5,
						Entries: []*raftcmdpb.ConvertMetadataEntry{
							{
								CanonicalKey:   canonicalKey,
								ConvertedValue: commonpb.NewIntValue(25),
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
	batchLog := applyLog.Log.Data.GetConvertMetadataBatch()
	require.NotNil(t, batchLog)
	require.Equal(t, uint32(0), batchLog.Count, "value already matches type, count should be 0")
}

func TestProcessMetadataConversionComplete_LedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_ConversionComplete{
					ConversionComplete: &raftcmdpb.MetadataConversionCompleteOrder{
						TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:          "age",
						ExpectedType: commonpb.MetadataType_METADATA_TYPE_INT64,
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

func TestProcessMetadataConversionComplete_Stale(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	// Schema is COMPLETE, so the completion order is stale
	ledgerInfo := &commonpb.LedgerInfo{
		Name:           "test-ledger",
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"age": {
					Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE,
				},
			},
		},
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_ConversionComplete{
					ConversionComplete: &raftcmdpb.MetadataConversionCompleteOrder{
						TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:          "age",
						ExpectedType: commonpb.MetadataType_METADATA_TYPE_INT64,
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

	completeLog := applyLog.Log.Data.GetMetadataConversionComplete()
	require.NotNil(t, completeLog)
	require.Equal(t, "age", completeLog.Key)
}

func TestProcessMetadataConversionComplete_Success(t *testing.T) {
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
				"age": {
					Type:      commonpb.MetadataType_METADATA_TYPE_INT64,
					Status:    commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
					TotalKeys: 10,
				},
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
				Data: &raftcmdpb.LedgerApplyOrder_ConversionComplete{
					ConversionComplete: &raftcmdpb.MetadataConversionCompleteOrder{
						TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:          "age",
						ExpectedType: commonpb.MetadataType_METADATA_TYPE_INT64,
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

	completeLog := applyLog.Log.Data.GetMetadataConversionComplete()
	require.NotNil(t, completeLog)
	require.Equal(t, "age", completeLog.Key)
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, completeLog.TargetType)
}
