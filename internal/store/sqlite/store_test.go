//go:build it

package sqlite

import (
	"context"
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

// appendLogs is a helper function to append logs using the batch pattern
func appendLogs(ctx context.Context, t *testing.T, s store.Store, lastAppliedIndex uint64, logs ...*ledgerpb.Log) {
	t.Helper()
	batch := s.NewBatch(lastAppliedIndex)
	err := batch.AppendLogs(ctx, logs...)
	require.NoError(t, err)
	err = batch.Commit(ctx)
	require.NoError(t, err)
}

func testStoreCommon(t *testing.T, createStore func(*testing.T) store.Store) {
	t.Parallel()

	ctx := logging.TestingContext()
	testLedger := "test-ledger"

	t.Run("AppendLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		testLogs := createTestLogs(testLedger)
		appendLogs(ctx, t, s, 0, testLogs...)
	})

	t.Run("BalancesCalculation", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		batch := s.NewBatch(0)
		require.NoError(t, batch.AppendBalanceDiff(ctx, testLedger, "world", "USD", big.NewInt(-100)))
		require.NoError(t, batch.AppendBalanceDiff(ctx, testLedger, "bank", "USD", big.NewInt(100)))
		require.NoError(t, batch.AppendBalanceDiff(ctx, testLedger, "user", "USD", big.NewInt(50)))
		require.NoError(t, batch.AppendBalanceDiff(ctx, testLedger, "bank", "USD", big.NewInt(-50)))
		require.NoError(t, batch.Commit(ctx))

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
		appendLogs(ctx, t, s, 0, testLogs...)

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
		appendLogs(ctx, t, s, 0, testLogs...)

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

		batch := s.NewBatch(0)
		require.NoError(t, batch.SaveAccountMetadata(ctx, testLedger, "bank", &ledgerpb.Metadata{
			Entries: metadata.Metadata{
				"account_type": "asset",
			},
		}))
		require.NoError(t, batch.SaveAccountMetadata(ctx, testLedger, "bank", &ledgerpb.Metadata{
			Entries: metadata.Metadata{
				"label": "Bank Account",
			},
		}))
		require.NoError(t, batch.DeleteAccountMetadata(ctx, testLedger, "bank", []string{"old_key"}))
		require.NoError(t, batch.Commit(ctx))

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

		appendLogs(ctx, t, s, 0, testLogs...)

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

		appendLogs(ctx, t, s, 0)
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
		appendLogs(ctx, t, s, 0, testLogs...)

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
		appendLogs(ctx, t, s, 42, testLogs...)

		// Verify the lastAppliedIndex was stored
		lastAppliedIndex, err = s.GetLastAppliedIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(42), lastAppliedIndex)

		// Update with just lastAppliedIndex (no logs)
		appendLogs(ctx, t, s, 200)

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

		appendLogs(ctx, t, s, 0, logs1...)
		appendLogs(ctx, t, s, 0, logs2...)

		// Delete ledger1
		err := s.DeleteLedger(ctx, ledger1)
		require.NoError(t, err)

		// Verify ledger1 data is gone
		lastLogID1, err := s.GetLastLogID(ctx, ledger1)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastLogID1)

		// Verify ledger2 data is still there
		lastLogID2, err := s.GetLastLogID(ctx, ledger2)
		require.NoError(t, err)
		require.Equal(t, uint64(4), lastLogID2)
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
					Metadata: &ledgerpb.Metadata{Entries: metadata.Metadata{
						"label": "Bank Account",
					}},
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
