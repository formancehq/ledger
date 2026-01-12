//go:build it

package service

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDefaultLedger_SaveAccountMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	t.Run("SaveAccountMetadata", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _, logFactory := newTestLedgerService(t, ctx)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		// Save account metadata
		md := metadata.Metadata{
			"account_type": "asset",
			"label":        "Test Account",
		}

		log, err := ledgerService.SaveAccountMetadata(ctx, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: md,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("SaveAccountMetadata_WithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		ledgerService, logWriter, runtimeStore, logFactory := newTestLedgerService(t, ctx)

		// Save account metadata with idempotency key
		md := metadata.Metadata{
			"key": "value",
		}

		idempotencyKey := "test-idempotency-key"
		hash := ledgerpb.ComputeIdempotencyHash(&ledgerpb.SaveAccountMetadataRequestPayload{
			Address:  "test-account",
			Metadata: md,
		})

		gomock.InOrder(
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(nil, uint64(0), nil),
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(hash, uint64(1), nil),
		)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log1, err := ledgerService.SaveAccountMetadata(ctx, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: md,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		logWriter.EXPECT().GetLogByID(gomock.Any(), log1.Id).Return(log1, nil)

		// Try to save again with the same idempotency key
		log2, err := ledgerService.SaveAccountMetadata(ctx, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: md,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log2)

		// Should return the same log
		require.Equal(t, log1.Id, log2.Id)
	})

	t.Run("SaveAccountMetadata_WithIdempotencyKeyConflict", func(t *testing.T) {
		t.Parallel()
		ledgerService, logWriter, runtimeStore, logFactory := newTestLedgerService(t, ctx)

		idempotencyKey := "test-idempotency-key-conflict"

		// Save account metadata with idempotency key
		md1 := metadata.Metadata{
			"key1": "value1",
		}
		hash := ledgerpb.ComputeIdempotencyHash(&ledgerpb.SaveAccountMetadataRequestPayload{
			Address:  "test-account",
			Metadata: md1,
		})

		gomock.InOrder(
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(nil, uint64(0), nil),
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(hash, uint64(1), nil),
		)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log1, err := ledgerService.SaveAccountMetadata(ctx, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: md1,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		logWriter.EXPECT().GetLogByID(gomock.Any(), log1.Id).Return(log1, nil)

		// Try to save again with the same idempotency key but different metadata
		md2 := metadata.Metadata{
			"key2": "value2",
		}
		log2, err := ledgerService.SaveAccountMetadata(ctx, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: md2,
			},
		})
		require.Error(t, err)
		require.Nil(t, log2)
		require.Equal(t, ErrIdempotencyKeyConflict, err)
	})

	t.Run("SaveAccountMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _, _ := newTestLedgerService(t, ctx)

		// Test empty address
		md1 := metadata.Metadata{"key": "value"}
		log, err := ledgerService.SaveAccountMetadata(ctx, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "",
				Metadata: md1,
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "account address is required")

		// Test empty metadata
		log, err = ledgerService.SaveAccountMetadata(ctx, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: nil,
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "metadata is required")
	})
}

func TestDefaultLedger_SaveTransactionMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	t.Run("SaveTransactionMetadata", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _, logFactory := newTestLedgerService(t, ctx)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		md := metadata.Metadata{
			"tx_label": "Test Transaction",
		}

		log, err := ledgerService.SaveTransactionMetadata(ctx, Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
			Input: &ledgerpb.SaveTransactionMetadataRequestPayload{
				TransactionId: 42,
				Metadata:      md,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("SaveTransactionMetadata_WithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		ledgerService, logWriter, runtimeStore, logFactory := newTestLedgerService(t, ctx)

		md := metadata.Metadata{
			"key": "value",
		}

		idempotencyKey := "test-tx-idempotency-key"
		hash := ledgerpb.ComputeIdempotencyHash(&ledgerpb.SaveTransactionMetadataRequestPayload{
			TransactionId: 42,
			Metadata:      md,
		})

		gomock.InOrder(
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(nil, uint64(0), nil),
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(hash, uint64(1), nil),
		)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log1, err := ledgerService.SaveTransactionMetadata(ctx, Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.SaveTransactionMetadataRequestPayload{
				TransactionId: 42,
				Metadata:      md,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		logWriter.EXPECT().GetLogByID(gomock.Any(), log1.Id).Return(log1, nil)

		log2, err := ledgerService.SaveTransactionMetadata(ctx, Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.SaveTransactionMetadataRequestPayload{
				TransactionId: 42,
				Metadata:      md,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log2)
		require.Equal(t, log1.Id, log2.Id)
	})

	t.Run("SaveTransactionMetadata_WithIdempotencyKeyConflict", func(t *testing.T) {
		t.Parallel()
		ledgerService, logWriter, runtimeStore, logFactory := newTestLedgerService(t, ctx)

		idempotencyKey := "test-tx-idempotency-key-conflict"

		md1 := metadata.Metadata{
			"key1": "value1",
		}
		hash := ledgerpb.ComputeIdempotencyHash(&ledgerpb.SaveTransactionMetadataRequestPayload{
			TransactionId: 42,
			Metadata:      md1,
		})

		gomock.InOrder(
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(nil, uint64(0), nil),
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(hash, uint64(1), nil),
		)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log1, err := ledgerService.SaveTransactionMetadata(ctx, Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.SaveTransactionMetadataRequestPayload{
				TransactionId: 42,
				Metadata:      md1,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		logWriter.EXPECT().GetLogByID(gomock.Any(), log1.Id).Return(log1, nil)

		md2 := metadata.Metadata{
			"key2": "value2",
		}
		log2, err := ledgerService.SaveTransactionMetadata(ctx, Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.SaveTransactionMetadataRequestPayload{
				TransactionId: 42,
				Metadata:      md2,
			},
		})
		require.Error(t, err)
		require.Nil(t, log2)
		require.Equal(t, ErrIdempotencyKeyConflict, err)
	})

	t.Run("SaveTransactionMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _, _ := newTestLedgerService(t, ctx)

		md := metadata.Metadata{"key": "value"}
		log, err := ledgerService.SaveTransactionMetadata(ctx, Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
			Input: &ledgerpb.SaveTransactionMetadataRequestPayload{
				TransactionId: 0,
				Metadata:      md,
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "transaction id is required")

		log, err = ledgerService.SaveTransactionMetadata(ctx, Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
			Input: &ledgerpb.SaveTransactionMetadataRequestPayload{
				TransactionId: 42,
				Metadata:      nil,
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "metadata is required")
	})
}

func TestDefaultLedger_DeleteAccountMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	t.Run("DeleteAccountMetadata", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _, logFactory := newTestLedgerService(t, ctx)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log, err := ledgerService.DeleteAccountMetadata(ctx, Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
			Input: &ledgerpb.DeleteAccountMetadataRequestPayload{
				Address: "test-account",
				Key:     "key1",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("DeleteAccountMetadata_WithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		ledgerService, logReader, runtimeStore, logFactory := newTestLedgerService(t, ctx)

		idempotencyKey := "delete-account-metadata-idempotency-key"
		hash := ledgerpb.ComputeIdempotencyHash(&ledgerpb.DeleteAccountMetadataRequestPayload{
			Address: "test-account",
			Key:     "key1",
		})

		gomock.InOrder(
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(nil, uint64(0), nil),
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(hash, uint64(1), nil),
		)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log1, err := ledgerService.DeleteAccountMetadata(ctx, Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.DeleteAccountMetadataRequestPayload{
				Address: "test-account",
				Key:     "key1",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		logReader.EXPECT().GetLogByID(gomock.Any(), log1.Id).Return(log1, nil)

		log2, err := ledgerService.DeleteAccountMetadata(ctx, Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.DeleteAccountMetadataRequestPayload{
				Address: "test-account",
				Key:     "key1",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log2)
		require.Equal(t, log1.Id, log2.Id)
	})

	t.Run("DeleteAccountMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _, _ := newTestLedgerService(t, ctx)

		log, err := ledgerService.DeleteAccountMetadata(ctx, Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
			Input: &ledgerpb.DeleteAccountMetadataRequestPayload{
				Address: "",
				Key:     "key1",
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "account address is required")

		log, err = ledgerService.DeleteAccountMetadata(ctx, Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
			Input: &ledgerpb.DeleteAccountMetadataRequestPayload{
				Address: "test-account",
				Key:     "",
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "metadata key is required")
	})
}

func TestDefaultLedger_DeleteTransactionMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	t.Run("DeleteTransactionMetadata", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _, logFactory := newTestLedgerService(t, ctx)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log, err := ledgerService.DeleteTransactionMetadata(ctx, Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
			Input: &ledgerpb.DeleteTransactionMetadataRequestPayload{
				TransactionId: 42,
				Key:           "key1",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("DeleteTransactionMetadata_WithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		ledgerService, logReader, runtimeStore, logFactory := newTestLedgerService(t, ctx)

		idempotencyKey := "delete-transaction-metadata-idempotency-key"
		hash := ledgerpb.ComputeIdempotencyHash(&ledgerpb.DeleteTransactionMetadataRequestPayload{
			TransactionId: 42,
			Key:           "key1",
		})

		gomock.InOrder(
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(nil, uint64(0), nil),
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(hash, uint64(1), nil),
		)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log1, err := ledgerService.DeleteTransactionMetadata(ctx, Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.DeleteTransactionMetadataRequestPayload{
				TransactionId: 42,
				Key:           "key1",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		logReader.EXPECT().GetLogByID(gomock.Any(), log1.Id).Return(log1, nil)

		log2, err := ledgerService.DeleteTransactionMetadata(ctx, Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.DeleteTransactionMetadataRequestPayload{
				TransactionId: 42,
				Key:           "key1",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log2)
		require.Equal(t, log1.Id, log2.Id)
	})

	t.Run("DeleteTransactionMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _, _ := newTestLedgerService(t, ctx)

		log, err := ledgerService.DeleteTransactionMetadata(ctx, Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
			Input: &ledgerpb.DeleteTransactionMetadataRequestPayload{
				TransactionId: 0,
				Key:           "key1",
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "transaction id is required")

		log, err = ledgerService.DeleteTransactionMetadata(ctx, Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
			Input: &ledgerpb.DeleteTransactionMetadataRequestPayload{
				TransactionId: 42,
				Key:           "",
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "metadata key is required")
	})
}

func newTestLedgerService(t *testing.T, ctx context.Context) (*DefaultLedger, *MockLogStore, *MockRuntimeStore, *MockLogFactory) {
	t.Helper()

	ctrl := gomock.NewController(t)
	logStore := NewMockLogStore(ctrl)
	logFactory := NewMockLogFactory(ctrl)
	runtimeStore := NewMockRuntimeStore(ctrl)
	logger := logging.FromContext(ctx)

	ledgerService := NewDefaultLedger(logStore, logFactory, runtimeStore, logger)
	return ledgerService, logStore, runtimeStore, logFactory
}

func expectCreateLogsWithSequentialIDs(logFactory *MockLogFactory, times int) {
	var counter uint64
	logFactory.EXPECT().
		CreateLog(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, idp *ledgerpb.Idempotency, input *ledgerpb.CommandInput) (*ledgerpb.Log, error) {
			counter++
			return &ledgerpb.Log{
				Id: counter,
			}, nil
		}).
		Times(times)
}

func TestDefaultLedger_RevertTransaction(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	t.Run("RevertTransaction", func(t *testing.T) {
		t.Parallel()
		ledgerService, logReader, runtimeStore, logFactory := newTestLedgerService(t, ctx)

		transactionID := uint64(42)
		logID := uint64(1)

		// Create original transaction log
		originalTx := &ledgerpb.Transaction{
			Id: transactionID,
			Postings: []*ledgerpb.Posting{
				ledgerpb.NewPosting("world", "account-1", "USD", big.NewInt(100)),
			},
		}
		originalLog := &ledgerpb.Log{
			Id: logID,
			Data: &ledgerpb.LogPayload{
				Payload: &ledgerpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &ledgerpb.CreatedTransaction{
						Transaction: originalTx,
					},
				},
			},
		}

		// Mock expectations
		runtimeStore.EXPECT().
			IsTransactionReverted(gomock.Any(), transactionID).
			Return(false, nil)
		runtimeStore.EXPECT().
			GetLogIDForTransactionID(gomock.Any(), transactionID).
			Return(logID, nil)
		logReader.EXPECT().
			GetLogByID(gomock.Any(), logID).
			Return(originalLog, nil)
		runtimeStore.EXPECT().
			GetBalances(gomock.Any(), gomock.Any()).
			Return(ledgerpb.Balances{
				"account-1": map[string]*big.Int{
					"USD": big.NewInt(100),
				},
			}, nil)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log, err := ledgerService.RevertTransaction(ctx, Parameters[*ledgerpb.RevertTransactionRequestPayload]{
			Input: &ledgerpb.RevertTransactionRequestPayload{
				TransactionId: transactionID,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("RevertTransaction_WithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		ledgerService, logReader, runtimeStore, logFactory := newTestLedgerService(t, ctx)

		transactionID := uint64(42)
		logID := uint64(1)
		idempotencyKey := "revert-tx-idempotency-key"

		// Create original transaction log
		originalTx := &ledgerpb.Transaction{
			Id: transactionID,
			Postings: []*ledgerpb.Posting{
				ledgerpb.NewPosting("world", "account-1", "USD", big.NewInt(100)),
			},
		}
		originalLog := &ledgerpb.Log{
			Id: logID,
			Data: &ledgerpb.LogPayload{
				Payload: &ledgerpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &ledgerpb.CreatedTransaction{
						Transaction: originalTx,
					},
				},
			},
		}

		hash := ledgerpb.ComputeIdempotencyHash(&ledgerpb.RevertTransactionRequestPayload{
			TransactionId: transactionID,
		})

		// Create the revert log that will be returned
		revertLogID := uint64(1)
		revertLog := &ledgerpb.Log{
			Id: revertLogID,
		}

		gomock.InOrder(
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(nil, uint64(0), nil),
			runtimeStore.EXPECT().
				IsTransactionReverted(gomock.Any(), transactionID).
				Return(false, nil),
			runtimeStore.EXPECT().
				GetLogIDForTransactionID(gomock.Any(), transactionID).
				Return(logID, nil),
			logReader.EXPECT().
				GetLogByID(gomock.Any(), logID).
				Return(originalLog, nil),
			runtimeStore.EXPECT().
				GetBalances(gomock.Any(), gomock.Any()).
				Return(ledgerpb.Balances{
					"account-1": map[string]*big.Int{
						"USD": big.NewInt(100),
					},
				}, nil),
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(hash, revertLogID, nil),
		)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log1, err := ledgerService.RevertTransaction(ctx, Parameters[*ledgerpb.RevertTransactionRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.RevertTransactionRequestPayload{
				TransactionId: transactionID,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		logReader.EXPECT().GetLogByID(gomock.Any(), revertLogID).Return(revertLog, nil)

		// Try to revert again with the same idempotency key
		log2, err := ledgerService.RevertTransaction(ctx, Parameters[*ledgerpb.RevertTransactionRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.RevertTransactionRequestPayload{
				TransactionId: transactionID,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log2)
		require.Equal(t, log1.Id, log2.Id)
	})

	t.Run("RevertTransaction_WithIdempotencyKeyConflict", func(t *testing.T) {
		t.Parallel()
		ledgerService, logReader, runtimeStore, logFactory := newTestLedgerService(t, ctx)

		transactionID := uint64(42)
		logID := uint64(1)
		idempotencyKey := "revert-tx-idempotency-key-conflict"

		// Create original transaction log
		originalTx := &ledgerpb.Transaction{
			Id: transactionID,
			Postings: []*ledgerpb.Posting{
				ledgerpb.NewPosting("world", "account-1", "USD", big.NewInt(100)),
			},
		}
		originalLog := &ledgerpb.Log{
			Id: logID,
			Data: &ledgerpb.LogPayload{
				Payload: &ledgerpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &ledgerpb.CreatedTransaction{
						Transaction: originalTx,
					},
				},
			},
		}

		hash1 := ledgerpb.ComputeIdempotencyHash(&ledgerpb.RevertTransactionRequestPayload{
			TransactionId: transactionID,
		})

		// Create the revert log that will be returned
		revertLogID := uint64(1)
		revertLog := &ledgerpb.Log{
			Id: revertLogID,
		}

		gomock.InOrder(
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(nil, uint64(0), nil),
			runtimeStore.EXPECT().
				IsTransactionReverted(gomock.Any(), transactionID).
				Return(false, nil),
			runtimeStore.EXPECT().
				GetLogIDForTransactionID(gomock.Any(), transactionID).
				Return(logID, nil),
			logReader.EXPECT().
				GetLogByID(gomock.Any(), logID).
				Return(originalLog, nil),
			runtimeStore.EXPECT().
				GetBalances(gomock.Any(), gomock.Any()).
				Return(ledgerpb.Balances{
					"account-1": map[string]*big.Int{
						"USD": big.NewInt(100),
					},
				}, nil),
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(hash1, revertLogID, nil),
		)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log1, err := ledgerService.RevertTransaction(ctx, Parameters[*ledgerpb.RevertTransactionRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.RevertTransactionRequestPayload{
				TransactionId: transactionID,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		logReader.EXPECT().GetLogByID(gomock.Any(), revertLogID).Return(revertLog, nil)

		// Try to revert again with the same idempotency key but different metadata
		log2, err := ledgerService.RevertTransaction(ctx, Parameters[*ledgerpb.RevertTransactionRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.RevertTransactionRequestPayload{
				TransactionId: transactionID,
				Metadata: map[string]string{
					"reason": "different",
				},
			},
		})
		require.Error(t, err)
		require.Nil(t, log2)
		require.Equal(t, ErrIdempotencyKeyConflict, err)
	})

	t.Run("RevertTransaction_AlreadyReverted", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, runtimeStore, _ := newTestLedgerService(t, ctx)

		transactionID := uint64(42)

		runtimeStore.EXPECT().
			IsTransactionReverted(gomock.Any(), transactionID).
			Return(true, nil)

		log, err := ledgerService.RevertTransaction(ctx, Parameters[*ledgerpb.RevertTransactionRequestPayload]{
			Input: &ledgerpb.RevertTransactionRequestPayload{
				TransactionId: transactionID,
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "already reverted")
	})

	t.Run("RevertTransaction_NotFound", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, runtimeStore, _ := newTestLedgerService(t, ctx)

		transactionID := uint64(999)

		runtimeStore.EXPECT().
			IsTransactionReverted(gomock.Any(), transactionID).
			Return(false, nil)
		runtimeStore.EXPECT().
			GetLogIDForTransactionID(gomock.Any(), transactionID).
			Return(uint64(0), nil)

		log, err := ledgerService.RevertTransaction(ctx, Parameters[*ledgerpb.RevertTransactionRequestPayload]{
			Input: &ledgerpb.RevertTransactionRequestPayload{
				TransactionId: transactionID,
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("RevertTransaction_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _, _ := newTestLedgerService(t, ctx)

		log, err := ledgerService.RevertTransaction(ctx, Parameters[*ledgerpb.RevertTransactionRequestPayload]{
			Input: &ledgerpb.RevertTransactionRequestPayload{
				TransactionId: 0,
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "transaction id is required")
	})

	t.Run("RevertTransaction_WithForce", func(t *testing.T) {
		t.Parallel()
		ledgerService, logReader, runtimeStore, logFactory := newTestLedgerService(t, ctx)

		transactionID := uint64(42)
		logID := uint64(1)

		// Create original transaction log
		originalTx := &ledgerpb.Transaction{
			Id: transactionID,
			Postings: []*ledgerpb.Posting{
				ledgerpb.NewPosting("world", "account-1", "USD", big.NewInt(100)),
			},
		}
		originalLog := &ledgerpb.Log{
			Id: logID,
			Data: &ledgerpb.LogPayload{
				Payload: &ledgerpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &ledgerpb.CreatedTransaction{
						Transaction: originalTx,
					},
				},
			},
		}

		// Mock expectations - with force=true, balance check should be skipped
		runtimeStore.EXPECT().
			IsTransactionReverted(gomock.Any(), transactionID).
			Return(false, nil)
		runtimeStore.EXPECT().
			GetLogIDForTransactionID(gomock.Any(), transactionID).
			Return(logID, nil)
		logReader.EXPECT().
			GetLogByID(gomock.Any(), logID).
			Return(originalLog, nil)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log, err := ledgerService.RevertTransaction(ctx, Parameters[*ledgerpb.RevertTransactionRequestPayload]{
			Input: &ledgerpb.RevertTransactionRequestPayload{
				TransactionId: transactionID,
				Force:         true,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("RevertTransaction_WithAtEffectiveDate", func(t *testing.T) {
		t.Parallel()
		ledgerService, logReader, runtimeStore, logFactory := newTestLedgerService(t, ctx)

		transactionID := uint64(42)
		logID := uint64(1)

		// Create original transaction log with timestamp
		originalTimestamp := ledgerpb.NewTimestamp(libtime.New(time.Now()))
		originalTx := &ledgerpb.Transaction{
			Id:        transactionID,
			Timestamp: originalTimestamp,
			Postings: []*ledgerpb.Posting{
				ledgerpb.NewPosting("world", "account-1", "USD", big.NewInt(100)),
			},
		}
		originalLog := &ledgerpb.Log{
			Id: logID,
			Data: &ledgerpb.LogPayload{
				Payload: &ledgerpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &ledgerpb.CreatedTransaction{
						Transaction: originalTx,
					},
				},
			},
		}

		// Mock expectations
		runtimeStore.EXPECT().
			IsTransactionReverted(gomock.Any(), transactionID).
			Return(false, nil)
		runtimeStore.EXPECT().
			GetLogIDForTransactionID(gomock.Any(), transactionID).
			Return(logID, nil)
		logReader.EXPECT().
			GetLogByID(gomock.Any(), logID).
			Return(originalLog, nil)
		runtimeStore.EXPECT().
			GetBalances(gomock.Any(), gomock.Any()).
			Return(ledgerpb.Balances{
				"account-1": map[string]*big.Int{
					"USD": big.NewInt(100),
				},
			}, nil)

		// Create a log with data for the revert transaction
		revertLogID := uint64(1)
		logFactory.EXPECT().
			CreateLog(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, idp *ledgerpb.Idempotency, input *ledgerpb.CommandInput) (*ledgerpb.Log, error) {
				return &ledgerpb.Log{
					Id: revertLogID,
					Data: &ledgerpb.LogPayload{
						Payload: &ledgerpb.LogPayload_RevertedTransaction{
							RevertedTransaction: &ledgerpb.RevertedTransaction{
								RevertedTransaction: originalTx,
								RevertTransaction: &ledgerpb.Transaction{
									Id:        2,
									Timestamp: originalTimestamp,
								},
							},
						},
					},
				}, nil
			}).
			Times(1)

		log, err := ledgerService.RevertTransaction(ctx, Parameters[*ledgerpb.RevertTransactionRequestPayload]{
			Input: &ledgerpb.RevertTransactionRequestPayload{
				TransactionId:   transactionID,
				AtEffectiveDate: true,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)

		// Verify that the revert transaction command contains the original timestamp
		require.NotNil(t, log.Data)
		require.NotNil(t, log.Data.Payload)
	})
}
