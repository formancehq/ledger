//go:build it

package service

import (
	"context"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
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
