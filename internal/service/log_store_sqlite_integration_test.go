//go:build it
// +build it

package service

import (
	"fmt"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/stretchr/testify/require"
)

func TestSQLiteLogStoreIntegration(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ledgerName := "test-ledger"

	t.Run("InsertLogs", func(t *testing.T) {
		t.Parallel()
		store := createSQLiteStore(t)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)
	})

	t.Run("GetLogWithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		store := createSQLiteStore(t)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Test with existing idempotency key
		log, err := store.GetLogWithIdempotencyKey(ctx, "idempotency-key-1")
		require.NoError(t, err)
		require.NotNil(t, log)
		require.Equal(t, "idempotency-key-1", log.IdempotencyKey)

		// Test with non-existing idempotency key
		log, err = store.GetLogWithIdempotencyKey(ctx, "non-existing-key")
		require.NoError(t, err)
		require.Nil(t, log)
	})

	t.Run("GetLastLog", func(t *testing.T) {
		t.Parallel()
		store := createSQLiteStore(t)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Get last log
		lastLog, err := store.GetLastLog(ctx)
		require.NoError(t, err)
		require.NotNil(t, lastLog)
		// The last log should be the one with the highest ID
		if len(testLogs) > 0 {
			expectedID := testLogs[len(testLogs)-1].Id
			if expectedID != 0 {
				require.Equal(t, expectedID, lastLog.Id)
			}
		}
	})

	t.Run("GetAllLogs", func(t *testing.T) {
		t.Parallel()
		store := createSQLiteStore(t)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Get all logs
		cursor, err := store.GetAllLogs(ctx, 0, 0)
		require.NoError(t, err)
		require.NotNil(t, cursor)
		t.Cleanup(func() { _ = cursor.Close() })

		// Read all logs and filter by ledger
		var logs []*ledgerpb.Log
		for {
			log, err := cursor.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			// All logs belong to this ledger (each store is for a single ledger)
			logs = append(logs, log)
		}

		// Verify we got all logs for this ledger
		require.Equal(t, len(testLogs), len(logs))

		// Verify logs are in ascending order by sequence
		for i := 0; i < len(logs)-1; i++ {
			require.LessOrEqual(t, logs[i].Sequence, logs[i+1].Sequence)
		}
	})

	t.Run("InsertLogsEmpty", func(t *testing.T) {
		t.Parallel()
		store := createSQLiteStore(t)

		err := store.InsertLogs(ctx)
		require.NoError(t, err)
	})

	t.Run("BalancesCalculation", func(t *testing.T) {
		t.Parallel()
		store := createSQLiteStore(t)
		testLogs := createTestLogs(t, ledgerName)

		// Insert logs
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		balances, err := store.GetBalances(ctx, map[string][]string{
			"world": {"USD"},
			"bank":  {"USD"},
			"user":  {"USD"},
		})
		require.NoError(t, err)
		require.Equal(t, big.NewInt(0), balances["world"]["USD"])
		require.Equal(t, big.NewInt(-50), balances["bank"]["USD"])
		require.Equal(t, big.NewInt(50), balances["user"]["USD"])
	})

	t.Run("GetAccountMetadata", func(t *testing.T) {
		t.Parallel()
		store := createSQLiteStore(t)
		testLogs := createTestLogs(t, ledgerName)

		// Insert logs
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Test GetAccountMetadata for multiple accounts
		accountsMetadata, err := store.GetAccountMetadata(ctx, []string{"bank", "user", "world", "non-existing"})
		require.NoError(t, err)
		require.NotNil(t, accountsMetadata)

		// Verify "bank" account metadata
		// Should have account_type from accountMetadata and label from SET_METADATA
		bankMetadata, exists := accountsMetadata["bank"]
		require.True(t, exists)
		require.Equal(t, "asset", bankMetadata["account_type"])
		require.Equal(t, "Bank Account", bankMetadata["label"])

		// Verify "user" account metadata (no metadata set)
		userMetadata, exists := accountsMetadata["user"]
		require.True(t, exists)
		require.Empty(t, userMetadata)

		// Verify "world" account metadata (no metadata set)
		worldMetadata, exists := accountsMetadata["world"]
		require.True(t, exists)
		require.Empty(t, worldMetadata)

		// Verify non-existing account (should return empty metadata)
		nonExistingMetadata, exists := accountsMetadata["non-existing"]
		require.True(t, exists)
		require.Empty(t, nonExistingMetadata)

		// Test with empty array
		emptyMetadata, err := store.GetAccountMetadata(ctx, []string{})
		require.NoError(t, err)
		require.NotNil(t, emptyMetadata)
		require.Empty(t, emptyMetadata)
	})

	t.Run("GetAccountMetadataWithMergeAndDelete", func(t *testing.T) {
		t.Parallel()
		store := createSQLiteStore(t)
		now := libtime.New(time.Now())

		// Create logs with account metadata
		logs := []*ledgerpb.Log{
			// Transaction with account metadata
			func() *ledgerpb.Log {
				payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.CreatedTransaction{
					Transaction: ledgerpb.NewTransaction().
						WithPostings(
							ledgerpb.NewPosting("world", "test-account", "USD", big.NewInt(100)),
						).
						WithID(1).
						WithTimestamp(now),
					AccountMetadata: map[string]*ledgerpb.Metadata{
						"test-account": {Entries: metadata.Metadata{
							"key1": "value1",
							"key2": "value2",
						}},
					},
				})
				return ledgerpb.NewLog(payload).
					WithID(1).
					WithSequence(1).
					WithDate(now)
			}(),

			// SET_METADATA for the same account
			func() *ledgerpb.Log {
				payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.SavedMetadata{
					TargetType: "ACCOUNT",
					TargetId:   &ledgerpb.SavedMetadata_AccountId{AccountId: "test-account"},
					Metadata: metadata.Metadata{
						"key3": "value3",
						"key2": "updated_value2", // This should override key2
					},
				})
				return ledgerpb.NewLog(payload).
					WithID(2).
					WithSequence(2).
					WithDate(now.Add(time.Second))
			}(),

			// DELETE_METADATA for the same account
			func() *ledgerpb.Log {
				payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.DeletedMetadata{
					TargetType: "ACCOUNT",
					TargetId:   &ledgerpb.DeletedMetadata_AccountId{AccountId: "test-account"},
					Key:        "key1",
				})
				return ledgerpb.NewLog(payload).
					WithID(3).
					WithSequence(3).
					WithDate(now.Add(2 * time.Second))
			}(),
		}

		err := store.InsertLogs(ctx, logs...)
		require.NoError(t, err)

		// Get account metadata and verify
		accountsMetadata, err := store.GetAccountMetadata(ctx, []string{"test-account"})
		require.NoError(t, err)
		require.NotNil(t, accountsMetadata)

		accountMetadata, exists := accountsMetadata["test-account"]
		require.True(t, exists)

		// Verify metadata: key1 should be deleted, key2 should be updated, key3 should exist
		require.NotContains(t, accountMetadata, "key1")
		require.Equal(t, "updated_value2", accountMetadata["key2"])
		require.Equal(t, "value3", accountMetadata["key3"])
	})

}

func createSQLiteStore(t *testing.T) *SQLiteLogStore {
	tmpDir := t.TempDir()
	dsn := fmt.Sprintf("file:%s/test.db?mode=memory&cache=shared", tmpDir)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	store, err := NewSQLiteLogStore(ctx, dsn, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}
