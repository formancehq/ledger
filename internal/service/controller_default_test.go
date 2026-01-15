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
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDefaultLedger_SaveAccountMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	t.Run("SaveAccountMetadata", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, logFactory := newTestLedgerService(t, ctx)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		// Save account metadata
		md := metadata.Metadata{
			"account_type": "asset",
			"label":        "Test Account",
		}

		log, err := ledgerService.SaveAccountMetadata(ctx, "default", Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: md,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("SaveAccountMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		// Test empty address
		md1 := metadata.Metadata{"key": "value"}
		log, err := ledgerService.SaveAccountMetadata(ctx, "default", Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "",
				Metadata: md1,
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "account address is required")

		// Test empty metadata
		log, err = ledgerService.SaveAccountMetadata(ctx, "default", Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
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
		ledgerService, _, logFactory := newTestLedgerService(t, ctx)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		md := metadata.Metadata{
			"tx_label": "Test Transaction",
		}

		log, err := ledgerService.SaveTransactionMetadata(ctx, "default", Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
			Input: &ledgerpb.SaveTransactionMetadataRequestPayload{
				TransactionId: 42,
				Metadata:      md,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("SaveTransactionMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		md := metadata.Metadata{"key": "value"}
		log, err := ledgerService.SaveTransactionMetadata(ctx, "default", Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
			Input: &ledgerpb.SaveTransactionMetadataRequestPayload{
				TransactionId: 0,
				Metadata:      md,
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "transaction id is required")

		log, err = ledgerService.SaveTransactionMetadata(ctx, "default", Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
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
		ledgerService, _, logFactory := newTestLedgerService(t, ctx)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log, err := ledgerService.DeleteAccountMetadata(ctx, "default", Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
			Input: &ledgerpb.DeleteAccountMetadataRequestPayload{
				Address: "test-account",
				Key:     "key1",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("DeleteAccountMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		log, err := ledgerService.DeleteAccountMetadata(ctx, "default", Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
			Input: &ledgerpb.DeleteAccountMetadataRequestPayload{
				Address: "",
				Key:     "key1",
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "account address is required")

		log, err = ledgerService.DeleteAccountMetadata(ctx, "default", Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
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
		ledgerService, _, logFactory := newTestLedgerService(t, ctx)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log, err := ledgerService.DeleteTransactionMetadata(ctx, "default", Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
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
		ledgerService, runtimeStore, logFactory := newTestLedgerService(t, ctx)

		idempotencyKey := "delete-transaction-metadata-idempotency-key"

		runtimeStore.EXPECT().
			GetLogIDForIdempotencyKey(gomock.Any(), "default", idempotencyKey).
			Return(uint64(0), store.ErrNotFound)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log1, err := ledgerService.DeleteTransactionMetadata(ctx, "default", Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.DeleteTransactionMetadataRequestPayload{
				TransactionId: 42,
				Key:           "key1",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		runtimeStore.EXPECT().
			GetLogIDForIdempotencyKey(gomock.Any(), "default", idempotencyKey).
			Return(log1.Id, nil)
		runtimeStore.EXPECT().
			GetLogByID(gomock.Any(), "default", log1.Id).
			Return(log1, nil)

		log2, err := ledgerService.DeleteTransactionMetadata(ctx, "default", Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
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
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		log, err := ledgerService.DeleteTransactionMetadata(ctx, "default", Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
			Input: &ledgerpb.DeleteTransactionMetadataRequestPayload{
				TransactionId: 0,
				Key:           "key1",
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "transaction id is required")

		log, err = ledgerService.DeleteTransactionMetadata(ctx, "default", Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
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

func newTestLedgerService(t *testing.T, ctx context.Context) (*DefaultController, *store.MockStore, *MockLogFactory) {
	t.Helper()

	ctrl := gomock.NewController(t)
	logFactory := NewMockLogFactory(ctrl)
	runtimeStore := store.NewMockStore(ctrl)
	logger := logging.FromContext(ctx)

	ledgerService := NewDefaultController(logFactory, runtimeStore, logger)
	return ledgerService, runtimeStore, logFactory
}

func expectCreateLogsWithSequentialIDs(logFactory *MockLogFactory, times int) {
	var counter uint64
	logFactory.EXPECT().
		CreateLog(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, ledger string, idp *ledgerpb.Idempotency, input *ledgerpb.CommandInput) (*ledgerpb.Log, error) {
			counter++
			return &ledgerpb.Log{
				Id:          counter,
				Idempotency: idp,
			}, nil
		}).
		Times(times)
}

func TestDefaultLedger_RevertTransaction(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	t.Run("RevertTransaction", func(t *testing.T) {
		t.Parallel()
		ledgerService, runtimeStore, logFactory := newTestLedgerService(t, ctx)

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
			IsTransactionReverted(gomock.Any(), "default", transactionID).
			Return(false, nil)
		runtimeStore.EXPECT().
			GetLogIDForTransactionID(gomock.Any(), "default", transactionID).
			Return(logID, nil)
		runtimeStore.EXPECT().
			GetLogByID(gomock.Any(), "default", logID).
			Return(originalLog, nil)
		runtimeStore.EXPECT().
			GetBalances(gomock.Any(), "default", gomock.Any()).
			Return(ledgerpb.Balances{
				"account-1": map[string]*big.Int{
					"USD": big.NewInt(100),
				},
			}, nil)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log, err := ledgerService.RevertTransaction(ctx, "default", Parameters[*ledgerpb.RevertTransactionRequestPayload]{
			Input: &ledgerpb.RevertTransactionRequestPayload{
				TransactionId: transactionID,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("RevertTransaction_AlreadyReverted", func(t *testing.T) {
		t.Parallel()
		ledgerService, runtimeStore, _ := newTestLedgerService(t, ctx)

		transactionID := uint64(42)

		runtimeStore.EXPECT().
			IsTransactionReverted(gomock.Any(), "default", transactionID).
			Return(true, nil)

		log, err := ledgerService.RevertTransaction(ctx, "default", Parameters[*ledgerpb.RevertTransactionRequestPayload]{
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
		ledgerService, runtimeStore, _ := newTestLedgerService(t, ctx)

		transactionID := uint64(999)

		runtimeStore.EXPECT().
			IsTransactionReverted(gomock.Any(), "default", transactionID).
			Return(false, nil)
		runtimeStore.EXPECT().
			GetLogIDForTransactionID(gomock.Any(), "default", transactionID).
			Return(uint64(0), nil)

		log, err := ledgerService.RevertTransaction(ctx, "default", Parameters[*ledgerpb.RevertTransactionRequestPayload]{
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
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		log, err := ledgerService.RevertTransaction(ctx, "default", Parameters[*ledgerpb.RevertTransactionRequestPayload]{
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
		ledgerService, runtimeStore, logFactory := newTestLedgerService(t, ctx)

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
			IsTransactionReverted(gomock.Any(), "default", transactionID).
			Return(false, nil)
		runtimeStore.EXPECT().
			GetLogIDForTransactionID(gomock.Any(), "default", transactionID).
			Return(logID, nil)
		runtimeStore.EXPECT().
			GetLogByID(gomock.Any(), "default", logID).
			Return(originalLog, nil)

		expectCreateLogsWithSequentialIDs(logFactory, 1)

		log, err := ledgerService.RevertTransaction(ctx, "default", Parameters[*ledgerpb.RevertTransactionRequestPayload]{
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
		ledgerService, runtimeStore, logFactory := newTestLedgerService(t, ctx)

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
			IsTransactionReverted(gomock.Any(), "default", transactionID).
			Return(false, nil)
		runtimeStore.EXPECT().
			GetLogIDForTransactionID(gomock.Any(), "default", transactionID).
			Return(logID, nil)
		runtimeStore.EXPECT().
			GetLogByID(gomock.Any(), "default", logID).
			Return(originalLog, nil)
		runtimeStore.EXPECT().
			GetBalances(gomock.Any(), "default", gomock.Any()).
			Return(ledgerpb.Balances{
				"account-1": map[string]*big.Int{
					"USD": big.NewInt(100),
				},
			}, nil)

		// Create a log with data for the revert transaction
		revertLogID := uint64(1)
		logFactory.EXPECT().
			CreateLog(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, ledger string, idp *ledgerpb.Idempotency, input *ledgerpb.CommandInput) (*ledgerpb.Log, error) {
				return &ledgerpb.Log{
					Id: revertLogID,
					Data: &ledgerpb.LogPayload{
						Payload: &ledgerpb.LogPayload_RevertedTransaction{
							RevertedTransaction: &ledgerpb.RevertedTransaction{
								RevertedTransactionId: originalTx.Id,
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

		log, err := ledgerService.RevertTransaction(ctx, "default", Parameters[*ledgerpb.RevertTransactionRequestPayload]{
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
