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
		ledgerService, logStore, _ := newTestLedgerService(t, ctx)

		expectInsertLogsWithSequentialIDs(logStore, 1)

		// Save account metadata
		md := metadata.Metadata{
			"account_type": "asset",
			"label":        "Test Account",
		}

		log, err := ledgerService.SaveAccountMetadata(ctx, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			DryRun: false,
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: md,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)

		// Verify the log was created correctly
		require.Equal(t, ledgerpb.SetMetadataLogType, ledgerpb.GetLogTypeFromLog(log))
		savedMetadata := log.Data.GetSavedMetadata()
		require.NotNil(t, savedMetadata)
		require.Equal(t, "ACCOUNT", savedMetadata.TargetType)
		require.Equal(t, "test-account", savedMetadata.GetAccountId())
		require.EqualValues(t, md, savedMetadata.Metadata)
	})

	t.Run("SaveAccountMetadata_WithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		ledgerService, logWriter, runtimeStore := newTestLedgerService(t, ctx)

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
				Return("", uint64(0), nil),
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(hash, uint64(1), nil),
		)

		expectInsertLogsWithSequentialIDs(logWriter, 1)

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
		ledgerService, logWriter, runtimeStore := newTestLedgerService(t, ctx)

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
				Return("", uint64(0), nil),
			runtimeStore.EXPECT().
				GetLogForIdempotencyKey(gomock.Any(), idempotencyKey).
				Return(hash, uint64(1), nil),
		)

		expectInsertLogsWithSequentialIDs(logWriter, 1)

		log1, err := ledgerService.SaveAccountMetadata(ctx, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			DryRun:         false,
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
			DryRun:         false,
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

	t.Run("SaveAccountMetadata_DryRun", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		// Save account metadata in dry run mode
		md := metadata.Metadata{
			"key": "value",
		}

		log, err := ledgerService.SaveAccountMetadata(ctx, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			DryRun: true,
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
		log, err := ledgerService.SaveAccountMetadata(ctx, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			DryRun: false,
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
			DryRun: false,
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: nil,
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "metadata is required")
	})

	t.Run("SaveAccountMetadata_MergeWithExisting", func(t *testing.T) {
		t.Parallel()
		ledgerService, logWriter, _ := newTestLedgerService(t, ctx)

		// First, create a transaction with account metadata
		now := libtime.New(time.Now())
		md := metadata.Metadata{
			"key1": "value1",
			"key2": "value2",
		}
		payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.CreatedTransaction{
			Transaction: ledgerpb.NewTransaction().
				WithPostings(
					ledgerpb.NewPosting("world", "test-account", "USD", big.NewInt(100)),
				).
				WithID(1).
				WithTimestamp(now),
			AccountMetadata: map[string]*ledgerpb.Metadata{
				"test-account": {Entries: md},
			},
		})
		txLog := ledgerpb.NewLog(payload).WithDate(now)

		expectInsertLogsWithSequentialIDs(logWriter, 2)

		err := logWriter.InsertLogs(ctx, txLog)
		require.NoError(t, err)

		// Then, save additional metadata
		md2 := metadata.Metadata{
			"key3": "value3",
			"key2": "updated_value2", // This should override key2
		}
		log, err := ledgerService.SaveAccountMetadata(ctx, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			DryRun: false,
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: md2,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})
}

func newTestLedgerService(t *testing.T, ctx context.Context) (*DefaultLedger, *MockLogStore, *MockRuntimeStore) {
	t.Helper()

	ctrl := gomock.NewController(t)
	logStore := NewMockLogStore(ctrl)
	runtimeStore := NewMockRuntimeStore(ctrl)
	logger := logging.FromContext(ctx)

	ledgerService := NewDefaultLedger(logStore, runtimeStore, logger)
	return ledgerService, logStore, runtimeStore
}

func expectInsertLogsWithSequentialIDs(logWriter *MockLogStore, times int) {
	var counter uint64
	logWriter.EXPECT().
		InsertLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, logs ...*ledgerpb.Log) error {
			for _, log := range logs {
				counter++
				log.Id = counter
			}
			return nil
		}).
		Times(times)
}
