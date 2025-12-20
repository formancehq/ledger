//go:build it
// +build it

package service

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	libtime "github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
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
		metadata := metadata.Metadata{
			"account_type": "asset",
			"label":        "Test Account",
		}

		log, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[SaveAccountMetadata]{
			DryRun: false,
			Input: SaveAccountMetadata{
				Address:  "test-account",
				Metadata: metadata,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
		require.NotNil(t, log.ID)

		// Verify the log was created correctly
		require.Equal(t, ledger.SetMetadataLogType, log.Type)
		savedMetadata, ok := log.Data.(*ledger.SavedMetadata)
		require.True(t, ok)
		require.Equal(t, "ACCOUNT", savedMetadata.TargetType)
		require.Equal(t, "test-account", savedMetadata.TargetID)
		require.Equal(t, metadata, savedMetadata.Metadata)

		// Verify metadata was stored in the accounts table
		accountsMetadata, err := store.GetAccountMetadata(ctx, ledgerName, []string{"test-account"})
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
		metadata := metadata.Metadata{
			"key": "value",
		}

		idempotencyKey := "test-idempotency-key"

		log1, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[SaveAccountMetadata]{
			DryRun:         false,
			IdempotencyKey: idempotencyKey,
			Input: SaveAccountMetadata{
				Address:  "test-account",
				Metadata: metadata,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		// Try to save again with the same idempotency key
		log2, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[SaveAccountMetadata]{
			DryRun:         false,
			IdempotencyKey: idempotencyKey,
			Input: SaveAccountMetadata{
				Address:  "test-account",
				Metadata: metadata,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log2)

		// Should return the same log
		require.Equal(t, log1.ID, log2.ID)
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
		log1, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[SaveAccountMetadata]{
			DryRun:         false,
			IdempotencyKey: idempotencyKey,
			Input: SaveAccountMetadata{
				Address: "test-account",
				Metadata: metadata.Metadata{
					"key1": "value1",
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		// Try to save again with the same idempotency key but different metadata
		log2, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[SaveAccountMetadata]{
			DryRun:         false,
			IdempotencyKey: idempotencyKey,
			Input: SaveAccountMetadata{
				Address: "test-account",
				Metadata: metadata.Metadata{
					"key2": "value2",
				},
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
		metadata := metadata.Metadata{
			"key": "value",
		}

		log, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[SaveAccountMetadata]{
			DryRun: true,
			Input: SaveAccountMetadata{
				Address:  "test-account",
				Metadata: metadata,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)

		// Verify the log was not persisted (dry run)
		lastLog, err := store.GetLastLog(ctx)
		require.NoError(t, err)
		if lastLog != nil {
			require.NotEqual(t, log.ID, lastLog.ID)
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
		log, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[SaveAccountMetadata]{
			DryRun: false,
			Input: SaveAccountMetadata{
				Address:  "",
				Metadata: metadata.Metadata{"key": "value"},
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "account address is required")

		// Test empty metadata
		log, err = ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[SaveAccountMetadata]{
			DryRun: false,
			Input: SaveAccountMetadata{
				Address:  "test-account",
				Metadata: metadata.Metadata{},
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
		txLog := ledger.NewLog(&ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction().
				WithPostings(
					ledger.NewPosting("world", "test-account", "USD", big.NewInt(100)),
				).
				WithID(1).
				WithTimestamp(now),
			AccountMetadata: ledger.AccountMetadata{
				"test-account": metadata.Metadata{
					"key1": "value1",
					"key2": "value2",
				},
			},
		}).
			WithID(1).
			WithSequence(1).
			WithDate(now)

		err := store.InsertLogs(ctx, txLog)
		require.NoError(t, err)

		// Then, save additional metadata
		log, err := ledgerService.SaveAccountMetadata(ctx, ledgerName, Parameters[SaveAccountMetadata]{
			DryRun: false,
			Input: SaveAccountMetadata{
				Address: "test-account",
				Metadata: metadata.Metadata{
					"key3": "value3",
					"key2": "updated_value2", // This should override key2
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)

		// Verify metadata was merged correctly
		accountsMetadata, err := store.GetAccountMetadata(ctx, ledgerName, []string{"test-account"})
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
	store LogStore
}

func (m *mockLogWriter) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
	return m.store.InsertLogs(ctx, logs...)
}

func (m *mockLogWriter) GetLastSequenceID(ctx context.Context) (uint64, error) {
	return m.store.GetLastSequenceID(ctx)
}

