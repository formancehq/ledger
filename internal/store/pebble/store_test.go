package pebble

import (
	"context"
	"io"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestPebbleStore(t *testing.T) {
	testStoreCommon(t, func(t *testing.T) store.Store {
		tmpDir := t.TempDir()
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)
		meter := noop.NewMeterProvider().Meter("test")

		s, err := NewStore(tmpDir, logger, meter)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close(ctx) })

		return s
	})
}

// registerLedger is a helper function to register a ledger and return its ID
func registerLedger(ctx context.Context, t *testing.T, s store.Store, name string, id uint32) {
	t.Helper()
	batch := s.NewBatch(0)
	err := batch.RegisterLedger(ctx, &commonpb.LedgerInfo{
		Id:        id,
		Name:      name,
		CreatedAt: commonpb.NewTimestamp(time.Now()),
	})
	require.NoError(t, err)
	err = batch.Commit(ctx)
	require.NoError(t, err)
}

// appendLogs is a helper function to append logs using the batch pattern
func appendLogs(ctx context.Context, t *testing.T, s store.Store, lastAppliedIndex uint64, logs ...*commonpb.Log) {
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
	var testLedgerID uint32 = 1

	t.Run("AppendLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(ctx, t, s, 0, testLogs...)
	})

	t.Run("BalancesCalculation", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)
		batch := s.NewBatch(0)
		require.NoError(t, batch.AppendBalanceDiff(ctx, testLedgerID, "world", "USD", commonpb.NewBigInt(big.NewInt(-100)), 1))
		require.NoError(t, batch.AppendBalanceDiff(ctx, testLedgerID, "bank", "USD", commonpb.NewBigInt(big.NewInt(100)), 1))
		require.NoError(t, batch.AppendBalanceDiff(ctx, testLedgerID, "user", "USD", commonpb.NewBigInt(big.NewInt(50)), 2))
		require.NoError(t, batch.AppendBalanceDiff(ctx, testLedgerID, "bank", "USD", commonpb.NewBigInt(big.NewInt(-50)), 2))
		require.NoError(t, batch.Commit(ctx))

		balances, err := s.GetBalances(ctx, testLedgerID, map[string][]string{
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

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(ctx, t, s, 0, testLogs...)

		cursor, err := s.GetAllLogs(ctx, testLedgerID, 0, 0)
		require.NoError(t, err)
		require.NotNil(t, cursor)
		t.Cleanup(func() { _ = cursor.Close() })

		var logs []*commonpb.Log
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

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(ctx, t, s, 0, testLogs...)

		log, err := s.GetLogByID(ctx, testLedgerID, 1)
		require.NoError(t, err)
		require.NotNil(t, log)
		require.Equal(t, uint64(1), log.Id)

		log, err = s.GetLogByID(ctx, testLedgerID, 999)
		require.NoError(t, err)
		require.Nil(t, log)
	})

	t.Run("GetAccountMetadata", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)
		batch := s.NewBatch(0)
		require.NoError(t, batch.SaveAccountMetadata(ctx, testLedgerID, "bank", &commonpb.Metadata{
			Entries: metadata.Metadata{
				"account_type": "asset",
			},
		}))
		require.NoError(t, batch.SaveAccountMetadata(ctx, testLedgerID, "bank", &commonpb.Metadata{
			Entries: metadata.Metadata{
				"label": "Bank Account",
			},
		}))
		require.NoError(t, batch.DeleteAccountMetadata(ctx, testLedgerID, "bank", []string{"old_key"}))
		require.NoError(t, batch.Commit(ctx))

		accountsMetadata, err := s.GetAccountMetadata(ctx, testLedgerID, []string{"bank", "user", "world", "non-existing"})
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

		emptyMetadata, err := s.GetAccountMetadata(ctx, testLedgerID, []string{})
		require.NoError(t, err)
		require.NotNil(t, emptyMetadata)
		require.Empty(t, emptyMetadata)
	})

	t.Run("GetLogForIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(ctx, t, s, 0, testLogs...)

		logID, err := s.GetLogIDForIdempotencyKey(ctx, testLedgerID, "idempotency-key-1")
		require.NoError(t, err)
		require.Equal(t, uint64(1), logID)

		logID, err = s.GetLogIDForIdempotencyKey(ctx, testLedgerID, "non-existing-key")
		require.NoError(t, err)
		require.Equal(t, uint64(0), logID)

		logID, err = s.GetLogIDForIdempotencyKey(ctx, testLedgerID, "")
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

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)

		// Test with no logs - should return 0
		lastLogID, err := s.GetLastLogID(ctx, testLedgerID)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastLogID)

		// Insert logs and verify last ID
		testLogs := createTestLogs(testLedgerID)
		appendLogs(ctx, t, s, 0, testLogs...)

		lastLogID, err = s.GetLastLogID(ctx, testLedgerID)
		require.NoError(t, err)
		require.Equal(t, uint64(4), lastLogID) // Last log has ID 4

		// Test with non-existent ledger
		lastLogID, err = s.GetLastLogID(ctx, 999)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastLogID)
	})

	t.Run("GetLastAppliedIndex", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)

		// Test initial state - should return 0
		lastAppliedIndex, err := s.GetLastAppliedIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastAppliedIndex)

		// Insert logs with a specific lastAppliedIndex
		testLogs := createTestLogs(testLedgerID)
		appendLogs(ctx, t, s, 42, testLogs...)

		// Verify the lastAppliedIndex was stored
		lastAppliedIndex, err = s.GetLastAppliedIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(42), lastAppliedIndex)

		// Update with a new lastAppliedIndex
		appendLogs(ctx, t, s, 100, testLogs...)

		lastAppliedIndex, err = s.GetLastAppliedIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(100), lastAppliedIndex)

		// Update with just lastAppliedIndex (no logs)
		appendLogs(ctx, t, s, 200)

		lastAppliedIndex, err = s.GetLastAppliedIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(200), lastAppliedIndex)
	})

	t.Run("ListLedgersAndGetByName", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		// Register ledgers
		registerLedger(ctx, t, s, "ledger-1", 1)
		registerLedger(ctx, t, s, "ledger-2", 2)

		// List all ledgers
		ledgers, err := s.ListLedgers(ctx)
		require.NoError(t, err)
		require.Len(t, ledgers, 2)

		// Get by name
		ledger, err := s.GetLedgerByName(ctx, "ledger-1")
		require.NoError(t, err)
		require.NotNil(t, ledger)
		require.Equal(t, uint32(1), ledger.Id)
		require.Equal(t, "ledger-1", ledger.Name)

		// Get non-existing ledger
		_, err = s.GetLedgerByName(ctx, "non-existing")
		require.Error(t, err)
	})

	t.Run("DeleteLedger", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		// Insert logs for two ledgers
		var ledger1ID uint32 = 1
		var ledger2ID uint32 = 2

		registerLedger(ctx, t, s, "ledger-1", ledger1ID)
		registerLedger(ctx, t, s, "ledger-2", ledger2ID)

		logs1 := createTestLogs(ledger1ID)
		logs2 := createTestLogs(ledger2ID)

		appendLogs(ctx, t, s, 0, logs1...)
		appendLogs(ctx, t, s, 0, logs2...)

		// Delete ledger1 via batch
		batch := s.NewBatch(0)
		err := batch.DeleteLedger(ctx, ledger1ID)
		require.NoError(t, err)
		err = batch.Commit(ctx)
		require.NoError(t, err)

		// Verify ledger1 data is gone
		lastLogID1, err := s.GetLastLogID(ctx, ledger1ID)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastLogID1)

		// Verify ledger2 data is still there
		lastLogID2, err := s.GetLastLogID(ctx, ledger2ID)
		require.NoError(t, err)
		require.Equal(t, uint64(4), lastLogID2)
	})
}

func createTestLogs(ledgerID uint32) []*commonpb.Log {
	now := time.Now()

	logs := []*commonpb.Log{
		commonpb.NewLog(&commonpb.LogPayload{
			Payload: &commonpb.LogPayload_CreatedTransaction{
				CreatedTransaction: &commonpb.CreatedTransaction{
					Transaction: commonpb.NewTransaction().
						WithPostings(
							commonpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
						).
						WithID(1).
						WithTimestamp(now),
					AccountMetadata: map[string]*commonpb.Metadata{
						"bank": {Entries: metadata.Metadata{
							"account_type": "asset",
						}},
					},
				},
			},
		}).
			WithLedgerID(ledgerID).
			WithID(1).
			WithIdempotency("idempotency-key-1", []byte("hash-1")).
			WithDate(now),
		commonpb.NewLog(&commonpb.LogPayload{
			Payload: &commonpb.LogPayload_CreatedTransaction{
				CreatedTransaction: &commonpb.CreatedTransaction{
					Transaction: commonpb.NewTransaction().
						WithPostings(
							commonpb.NewPosting("bank", "user", "USD", big.NewInt(50)),
						).
						WithID(2).
						WithTimestamp(now),
				},
			},
		}).
			WithLedgerID(ledgerID).
			WithID(2).
			WithIdempotency("idempotency-key-2", []byte("hash-2")).
			WithDate(now.Add(time.Second)),
		commonpb.NewLog(&commonpb.LogPayload{
			Payload: &commonpb.LogPayload_SavedMetadata{
				SavedMetadata: &commonpb.SavedMetadata{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{
							Addr: "bank",
						}},
					},
					Metadata: &commonpb.Metadata{Entries: metadata.Metadata{
						"label": "Bank Account",
					}},
				},
			},
		}).
			WithLedgerID(ledgerID).
			WithID(3).
			WithDate(now.Add(2 * time.Second)),
		commonpb.NewLog(&commonpb.LogPayload{
			Payload: &commonpb.LogPayload_DeletedMetadata{
				DeletedMetadata: &commonpb.DeletedMetadata{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{
							Addr: "bank",
						}},
					},
					Key: "old_key",
				},
			},
		}).
			WithLedgerID(ledgerID).
			WithID(4).
			WithDate(now.Add(3 * time.Second)),
	}

	return logs
}

func TestPebbleStoreSnapshots(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := NewStore(tmpDir, logger, meter)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	// Register ledger
	var ledgerID uint32 = 1
	batch := store.NewBatch(0)
	err = batch.RegisterLedger(ctx, &commonpb.LedgerInfo{
		Id:        ledgerID,
		Name:      "default",
		CreatedAt: commonpb.NewTimestamp(time.Now()),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit(ctx))

	now := time.Now()
	for i := range uint64(10) {
		batch := store.NewBatch(0)
		err := batch.AppendLogs(ctx,
			commonpb.NewLog(&commonpb.LogPayload{
				Payload: &commonpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{
						Transaction: commonpb.NewTransaction().
							WithPostings(
								commonpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
							).
							WithID(i).
							WithTimestamp(now),
					},
				},
			}).
				WithLedgerID(ledgerID).
				WithID(i).
				WithDate(now),
		)
		require.NoError(t, err)
		require.NoError(t, batch.Commit(ctx))
	}

	require.NoError(t, store.CreateSnapshot(ctx))

	for i := range uint64(5) {
		batch := store.NewBatch(0)
		err := batch.AppendLogs(ctx,
			commonpb.NewLog(&commonpb.LogPayload{
				Payload: &commonpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{
						Transaction: commonpb.NewTransaction().
							WithPostings(
								commonpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
							).
							WithID(10 + i).
							WithTimestamp(now),
					},
				},
			}).
				WithLedgerID(ledgerID).
				WithID(10+i).
				WithDate(now),
		)
		require.NoError(t, err)
		require.NoError(t, batch.Commit(ctx))
	}

	cursor, err := store.GetAllLogs(ctx, ledgerID, 0, 0)
	require.NoError(t, err)

	count := 0
	for {
		_, err := cursor.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		count++
	}

	require.Equal(t, 15, count)
	require.NoError(t, cursor.Close())
	require.NoError(t, store.Close(ctx))

	store, err = NewStore(tmpDir, logger, meter)
	require.NoError(t, err)

	cursor, err = store.GetAllLogs(ctx, ledgerID, 0, 0)
	require.NoError(t, err)

	count = 0
	for {
		_, err := cursor.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		count++
	}

	// Should have restored the last snapshot
	require.Equal(t, 10, count)
	require.NoError(t, cursor.Close())

}
