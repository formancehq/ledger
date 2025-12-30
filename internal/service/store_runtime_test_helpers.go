//go:build it

package service

import (
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/stretchr/testify/require"
)

// TestRuntimeStoreIntegrationCommon runs common tests for runtime stores (RuntimeStore + LogWriter)
func TestRuntimeStoreIntegrationCommon(t *testing.T, createStore func(*testing.T) interface {
	RuntimeStore
	LogWriter
}) {
	t.Parallel()

	ctx := logging.TestingContext()
	ledgerName := "test-ledger"

	t.Run("InsertLogs", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)
	})

	t.Run("BalancesCalculation", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)
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
		store := createStore(t)
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
		store := createStore(t)
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

	t.Run("GetLogForIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)
		testLogs := createTestLogs(t, ledgerName)

		// Insert logs
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Test with existing idempotency key
		hash, logID, err := store.GetLogForIdempotencyKey(ctx, "idempotency-key-1")
		require.NoError(t, err)
		require.NotEmpty(t, hash)
		require.Equal(t, uint64(1), logID)

		// Test with non-existing idempotency key
		hash, logID, err = store.GetLogForIdempotencyKey(ctx, "non-existing-key")
		require.NoError(t, err)
		require.Empty(t, hash)
		require.Equal(t, uint64(0), logID)

		// Test with empty key
		hash, logID, err = store.GetLogForIdempotencyKey(ctx, "")
		require.NoError(t, err)
		require.Empty(t, hash)
		require.Equal(t, uint64(0), logID)
	})

	t.Run("InsertLogsEmpty", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)

		err := store.InsertLogs(ctx)
		require.NoError(t, err)
	})
}
