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
)

func TestDefaultLedger_SaveAccountMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ledgerName := "test-ledger"

	t.Run("SaveAccountMetadata", func(t *testing.T) {
		t.Parallel()
		store := createSQLiteStore(t)
		logWriter := &mockLogWriter{store: store}
		lockedVolumesStore := NewDefaultLockedBalancesStore(store)
		logger := logging.FromContext(ctx)

		ledgerService := NewDefaultLedger(logWriter, lockedVolumesStore, store, logger)

		// Save account metadata
		md := metadata.Metadata{
			"account_type": "asset",
			"label":        "Test Account",
		}

		log, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
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

		// Verify metadata was stored in the accounts table
		accountsMetadata, err := store.GetAccountMetadata(ctx, []string{"test-account"})
		require.NoError(t, err)
		require.NotNil(t, accountsMetadata)

		accountMetadata, exists := accountsMetadata["test-account"]
		require.True(t, exists)
		require.Equal(t, "asset", accountMetadata["account_type"])
		require.Equal(t, "Test Account", accountMetadata["label"])
	})

	t.Run("SaveAccountMetadata_WithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		store := createSQLiteStore(t)
		logWriter := &mockLogWriter{store: store}
		lockedVolumesStore := NewDefaultLockedBalancesStore(store)
		logger := logging.FromContext(ctx)

		ledgerService := NewDefaultLedger(logWriter, lockedVolumesStore, store, logger)

		// Save account metadata with idempotency key
		md := metadata.Metadata{
			"key": "value",
		}

		idempotencyKey := "test-idempotency-key"

		log1, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: md,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		// Try to save again with the same idempotency key
		log2, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
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
		store := createSQLiteStore(t)
		logWriter := &mockLogWriter{store: store}
		lockedVolumesStore := NewDefaultLockedBalancesStore(store)
		logger := logging.FromContext(ctx)

		ledgerService := NewDefaultLedger(logWriter, lockedVolumesStore, store, logger)

		idempotencyKey := "test-idempotency-key-conflict"

		// Save account metadata with idempotency key
		md1 := metadata.Metadata{
			"key1": "value1",
		}
		log1, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			DryRun:         false,
			IdempotencyKey: idempotencyKey,
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: md1,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		// Try to save again with the same idempotency key but different metadata
		md2 := metadata.Metadata{
			"key2": "value2",
		}
		log2, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
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
		store := createSQLiteStore(t)
		logWriter := &mockLogWriter{store: store}
		lockedVolumesStore := NewDefaultLockedBalancesStore(store)
		logger := logging.FromContext(ctx)

		ledgerService := NewDefaultLedger(logWriter, lockedVolumesStore, store, logger)

		// Save account metadata in dry run mode
		md := metadata.Metadata{
			"key": "value",
		}

		log, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			DryRun: true,
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: md,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)

		// Verify the log was not persisted (dry run)
		lastLog, err := store.GetLastLog(ctx)
		require.NoError(t, err)
		if lastLog != nil {
			require.NotEqual(t, log.Id, lastLog.Id)
		}
	})

	t.Run("SaveAccountMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		store := createSQLiteStore(t)
		logWriter := &mockLogWriter{store: store}
		lockedVolumesStore := NewDefaultLockedBalancesStore(store)
		logger := logging.FromContext(ctx)

		ledgerService := NewDefaultLedger(logWriter, lockedVolumesStore, store, logger)

		// Test empty address
		md1 := metadata.Metadata{"key": "value"}
		log, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
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
		log, err = ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
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
		store := createSQLiteStore(t)
		logWriter := &mockLogWriter{store: store}
		lockedVolumesStore := NewDefaultLockedBalancesStore(store)
		logger := logging.FromContext(ctx)

		ledgerService := NewDefaultLedger(logWriter, lockedVolumesStore, store, logger)

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

		err := logWriter.InsertLogs(ctx, txLog)
		require.NoError(t, err)

		// Then, save additional metadata
		md2 := metadata.Metadata{
			"key3": "value3",
			"key2": "updated_value2", // This should override key2
		}
		log, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
			DryRun: false,
			Input: &ledgerpb.SaveAccountMetadataRequestPayload{
				Address:  "test-account",
				Metadata: md2,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)

		// Verify metadata was merged correctly
		accountsMetadata, err := store.GetAccountMetadata(ctx, []string{"test-account"})
		require.NoError(t, err)
		require.NotNil(t, accountsMetadata)

		accountMetadata, exists := accountsMetadata["test-account"]
		require.True(t, exists)
		require.Equal(t, "value1", accountMetadata["key1"])
		require.Equal(t, "updated_value2", accountMetadata["key2"]) // Should be updated
		require.Equal(t, "value3", accountMetadata["key3"])
	})
}

// mockLogWriter implements LogWriter by delegating to the underlying store
type mockLogWriter struct {
	store   LogStore
	counter uint64
}

func (m *mockLogWriter) InsertLogs(ctx context.Context, logs ...*ledgerpb.Log) error {
	for _, log := range logs {
		m.counter++
		log.Id = m.counter
	}
	return m.store.InsertLogs(ctx, logs...)
}

func (m *mockLogWriter) GetLastLogID(ctx context.Context) (uint64, error) {
	return m.store.GetLastLogID(ctx)
}
