//go:build it

package sqlite

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
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/stretchr/testify/require"
)

func TestSQLiteMattnStore(t *testing.T) {
	testStoreCommon(t, func(t *testing.T) store.Store {
		tmpDir := t.TempDir()
		runtimeDSN := fmt.Sprintf("file:%s/test-runtime.db", tmpDir)
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)

		store, err := NewMattnStore(runtimeDSN, logger)
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close(ctx) })

		return store
	})
}

func TestSQLiteModernStore(t *testing.T) {
	testStoreCommon(t, func(t *testing.T) store.Store {
		tmpDir := t.TempDir()
		runtimeDSN := fmt.Sprintf("file:%s/test-runtime.db", tmpDir)
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)

		store, err := NewModernStore(runtimeDSN, logger)
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close(ctx) })

		return store
	})
}

func testStoreCommon(t *testing.T, createStore func(*testing.T) store.Store) {
	t.Parallel()

	ctx := logging.TestingContext()
	testLedger := "test-ledger"

	t.Run("AppendLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		testLogs := createTestLogs(testLedger)
		err := s.AppendLogs(ctx, 0, testLogs...)
		require.NoError(t, err)
	})

	t.Run("BalancesCalculation", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)
		testLogs := createTestLogs(testLedger)

		err := s.AppendLogs(ctx, 0, testLogs...)
		require.NoError(t, err)

		balances, err := s.GetBalances(ctx, testLedger, map[string][]string{
			"world": {"USD"},
			"bank":  {"USD"},
			"user":  {"USD"},
		})
		require.NoError(t, err)
		require.Equal(t, big.NewInt(-100), balances["world"]["USD"])
		require.Equal(t, big.NewInt(50), balances["bank"]["USD"])
		require.Equal(t, big.NewInt(50), balances["user"]["USD"])
	})

	t.Run("GetAllLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		testLogs := createTestLogs(testLedger)
		err := s.AppendLogs(ctx, 0, testLogs...)
		require.NoError(t, err)

		cursor, err := s.GetAllLogs(ctx, testLedger, 0, 0)
		require.NoError(t, err)
		require.NotNil(t, cursor)
		t.Cleanup(func() { _ = cursor.Close() })

		var logs []*ledgerpb.Log
		for {
			log, err := cursor.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			logs = append(logs, log)
		}

		require.Equal(t, len(testLogs), len(logs))

		for i := 0; i < len(logs)-1; i++ {
			require.LessOrEqual(t, logs[i].Id, logs[i+1].Id)
		}
	})

	t.Run("GetLogByID", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		testLogs := createTestLogs(testLedger)
		err := s.AppendLogs(ctx, 0, testLogs...)
		require.NoError(t, err)

		log, err := s.GetLogByID(ctx, testLedger, 1)
		require.NoError(t, err)
		require.NotNil(t, log)
		require.Equal(t, uint64(1), log.Id)

		log, err = s.GetLogByID(ctx, testLedger, 999)
		require.NoError(t, err)
		require.Nil(t, log)
	})

	t.Run("GetAccountMetadata", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)
		testLogs := createTestLogs(testLedger)

		err := s.AppendLogs(ctx, 0, testLogs...)
		require.NoError(t, err)

		accountsMetadata, err := s.GetAccountMetadata(ctx, testLedger, []string{"bank", "user", "world", "non-existing"})
		require.NoError(t, err)
		require.NotNil(t, accountsMetadata)

		bankMetadata, exists := accountsMetadata["bank"]
		require.True(t, exists)
		require.Equal(t, "asset", bankMetadata["account_type"])
		require.Equal(t, "Bank Account", bankMetadata["label"])

		userMetadata, exists := accountsMetadata["user"]
		require.True(t, exists)
		require.Empty(t, userMetadata)

		worldMetadata, exists := accountsMetadata["world"]
		require.True(t, exists)
		require.Empty(t, worldMetadata)

		nonExistingMetadata, exists := accountsMetadata["non-existing"]
		require.True(t, exists)
		require.Empty(t, nonExistingMetadata)

		emptyMetadata, err := s.GetAccountMetadata(ctx, testLedger, []string{})
		require.NoError(t, err)
		require.NotNil(t, emptyMetadata)
		require.Empty(t, emptyMetadata)
	})

	t.Run("GetLogForIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)
		testLogs := createTestLogs(testLedger)

		err := s.AppendLogs(ctx, 0, testLogs...)
		require.NoError(t, err)

		logID, err := s.GetLogIDForIdempotencyKey(ctx, testLedger, "idempotency-key-1")
		require.NoError(t, err)
		require.Equal(t, uint64(1), logID)

		logID, err = s.GetLogIDForIdempotencyKey(ctx, testLedger, "non-existing-key")
		require.NoError(t, err)
		require.Equal(t, uint64(0), logID)

		logID, err = s.GetLogIDForIdempotencyKey(ctx, testLedger, "")
		require.NoError(t, err)
		require.Equal(t, uint64(0), logID)
	})

	t.Run("AppendLogsEmpty", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		err := s.AppendLogs(ctx, 0)
		require.NoError(t, err)
	})

	t.Run("GetLastLogID", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		// Test with no logs - should return 0
		lastLogID, err := s.GetLastLogID(ctx, testLedger)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastLogID)

		// Insert logs and verify last ID
		testLogs := createTestLogs(testLedger)
		err = s.AppendLogs(ctx, 0, testLogs...)
		require.NoError(t, err)

		lastLogID, err = s.GetLastLogID(ctx, testLedger)
		require.NoError(t, err)
		require.Equal(t, uint64(4), lastLogID) // Last log has ID 4

		// Test with non-existent ledger
		lastLogID, err = s.GetLastLogID(ctx, "non-existent-ledger")
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastLogID)
	})

	t.Run("GetLastAppliedIndex", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		// Test initial state - should return 0
		lastAppliedIndex, err := s.GetLastAppliedIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastAppliedIndex)

		// Insert logs with a specific lastAppliedIndex
		testLogs := createTestLogs(testLedger)
		err = s.AppendLogs(ctx, 42, testLogs...)
		require.NoError(t, err)

		// Verify the lastAppliedIndex was stored
		lastAppliedIndex, err = s.GetLastAppliedIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(42), lastAppliedIndex)

		// Update with just lastAppliedIndex (no logs)
		err = s.AppendLogs(ctx, 200)
		require.NoError(t, err)

		lastAppliedIndex, err = s.GetLastAppliedIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(200), lastAppliedIndex)
	})

	t.Run("DeleteLedger", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		// Insert logs for two ledgers
		ledger1 := "ledger-1"
		ledger2 := "ledger-2"

		logs1 := createTestLogs(ledger1)
		logs2 := createTestLogs(ledger2)

		err := s.AppendLogs(ctx, 0, logs1...)
		require.NoError(t, err)

		err = s.AppendLogs(ctx, 0, logs2...)
		require.NoError(t, err)

		// Verify both ledgers have data
		lastLogID1, err := s.GetLastLogID(ctx, ledger1)
		require.NoError(t, err)
		require.Equal(t, uint64(4), lastLogID1)

		lastLogID2, err := s.GetLastLogID(ctx, ledger2)
		require.NoError(t, err)
		require.Equal(t, uint64(4), lastLogID2)

		// Delete ledger1
		err = s.DeleteLedger(ledger1)
		require.NoError(t, err)

		// Verify ledger1 data is gone
		lastLogID1, err = s.GetLastLogID(ctx, ledger1)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastLogID1)

		// Verify ledger2 data is still there
		lastLogID2, err = s.GetLastLogID(ctx, ledger2)
		require.NoError(t, err)
		require.Equal(t, uint64(4), lastLogID2)

		// Verify balances are also deleted for ledger1
		balances, err := s.GetBalances(ctx, ledger1, map[string][]string{
			"world": {"USD"},
		})
		require.NoError(t, err)
		require.Equal(t, big.NewInt(0), balances["world"]["USD"])

		// Verify balances are still there for ledger2
		balances, err = s.GetBalances(ctx, ledger2, map[string][]string{
			"world": {"USD"},
		})
		require.NoError(t, err)
		require.Equal(t, big.NewInt(-100), balances["world"]["USD"])
	})
}

func createTestLogs(ledger string) []*ledgerpb.Log {
	now := libtime.New(time.Now())

	logs := []*ledgerpb.Log{
		ledgerpb.NewLog(&ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_CreatedTransaction{
				CreatedTransaction: &ledgerpb.CreatedTransaction{
					Transaction: ledgerpb.NewTransaction().
						WithPostings(
							ledgerpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
						).
						WithID(1).
						WithTimestamp(now),
					AccountMetadata: map[string]*ledgerpb.Metadata{
						"bank": {Entries: metadata.Metadata{
							"account_type": "asset",
						}},
					},
				},
			},
		}).
			WithLedger(ledger).
			WithID(1).
			WithIdempotency("idempotency-key-1", []byte("hash-1")).
			WithDate(now),
		ledgerpb.NewLog(&ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_CreatedTransaction{
				CreatedTransaction: &ledgerpb.CreatedTransaction{
					Transaction: ledgerpb.NewTransaction().
						WithPostings(
							ledgerpb.NewPosting("bank", "user", "USD", big.NewInt(50)),
						).
						WithID(2).
						WithTimestamp(now),
				},
			},
		}).
			WithLedger(ledger).
			WithID(2).
			WithIdempotency("idempotency-key-2", []byte("hash-2")).
			WithDate(now.Add(time.Second)),
		ledgerpb.NewLog(&ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_SavedMetadata{
				SavedMetadata: &ledgerpb.SavedMetadata{
					Target: &ledgerpb.Target{
						Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{
							Addr: "bank",
						}},
					},
					Metadata: metadata.Metadata{
						"label": "Bank Account",
					},
				},
			},
		}).
			WithLedger(ledger).
			WithID(3).
			WithDate(now.Add(2 * time.Second)),
		ledgerpb.NewLog(&ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_DeletedMetadata{
				DeletedMetadata: &ledgerpb.DeletedMetadata{
					Target: &ledgerpb.Target{
						Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{
							Addr: "bank",
						}},
					},
					Key: "old_key",
				},
			},
		}).
			WithLedger(ledger).
			WithID(4).
			WithDate(now.Add(3 * time.Second)),
	}

	return logs
}
