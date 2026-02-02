package pebble

import (
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

// collectLedgers collects all ledgers from a cursor into a slice
func collectLedgers(cursor store.Cursor[*commonpb.LedgerInfo]) ([]*commonpb.LedgerInfo, error) {
	defer func() { _ = cursor.Close() }()
	var ledgers []*commonpb.LedgerInfo
	for {
		ledger, err := cursor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ledgers = append(ledgers, ledger)
	}
	return ledgers, nil
}

func TestPebbleStore(t *testing.T) {
	testStoreCommon(t, func(t *testing.T) store.Store {
		tmpDir := t.TempDir()
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)
		meter := noop.NewMeterProvider().Meter("test")

		s, err := NewStore(tmpDir, logger, meter, DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		return s
	})
}

// registerLedger is a helper function to register a ledger and return its ID
func registerLedger(t *testing.T, s store.Store, name string, id uint32) {
	t.Helper()
	batch := s.NewBatch(0)
	err := batch.SaveLedger(&commonpb.LedgerInfo{
		Id:        id,
		Name:      name,
		CreatedAt: commonpb.NewTimestamp(time.Now()),
	})
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)
}

// appendLogs is a helper function to append logs using the batch pattern
func appendLogs(t *testing.T, s store.Store, lastAppliedIndex uint64, logs ...*commonpb.Log) {
	t.Helper()
	batch := s.NewBatch(lastAppliedIndex)
	err := batch.AppendLogs(logs...)
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)
}

func testStoreCommon(t *testing.T, createStore func(*testing.T) store.Store) {
	t.Parallel()

	var testLedgerID uint32 = 1

	t.Run("AppendLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, "test-ledger", testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(t, s, 0, testLogs...)
	})

	t.Run("BalancesCalculation", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, "test-ledger", testLedgerID)
		batch := s.NewBatch(0)
		require.NoError(t, batch.AppendBalanceDiff(store.BalanceDiff{LedgerID: testLedgerID, Account: "world", Asset: "USD", Diff: commonpb.NewBigInt(big.NewInt(-100)), RaftIndex: 1}))
		require.NoError(t, batch.AppendBalanceDiff(store.BalanceDiff{LedgerID: testLedgerID, Account: "bank", Asset: "USD", Diff: commonpb.NewBigInt(big.NewInt(100)), RaftIndex: 1}))
		require.NoError(t, batch.AppendBalanceDiff(store.BalanceDiff{LedgerID: testLedgerID, Account: "user", Asset: "USD", Diff: commonpb.NewBigInt(big.NewInt(50)), RaftIndex: 2}))
		require.NoError(t, batch.AppendBalanceDiff(store.BalanceDiff{LedgerID: testLedgerID, Account: "bank", Asset: "USD", Diff: commonpb.NewBigInt(big.NewInt(-50)), RaftIndex: 2}))
		require.NoError(t, batch.Commit())

		diffs, err := s.GetBalanceDiffs(testLedgerID, map[string][]string{
			"world": {"USD"},
			"bank":  {"USD"},
			"user":  {"USD"},
		})
		require.NoError(t, err)

		// Compute balances from diffs
		computeBalance := func(diffs []store.StoredBalanceDiff) *big.Int {
			balance := big.NewInt(0)
			for _, d := range diffs {
				balance = balance.Add(balance, d.Diff.Value())
			}
			return balance
		}

		require.Equal(t, big.NewInt(-100), computeBalance(diffs["world"]["USD"]))
		require.Equal(t, big.NewInt(50), computeBalance(diffs["bank"]["USD"]))
		require.Equal(t, big.NewInt(50), computeBalance(diffs["user"]["USD"]))
	})

	t.Run("GetAllLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, "test-ledger", testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(t, s, 0, testLogs...)

		// Test GetAllLogs (global logs by sequence)
		cursor, err := s.GetAllLogs(0, 0)
		require.NoError(t, err)
		require.NotNil(t, cursor)
		t.Cleanup(func() { _ = cursor.Close() })

		var logs []*commonpb.Log
		for {
			log, err := cursor.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			logs = append(logs, log)
		}

		require.Equal(t, len(testLogs), len(logs))

		// Verify logs are in sequence order
		for i := 0; i < len(logs)-1; i++ {
			require.LessOrEqual(t, logs[i].Sequence, logs[i+1].Sequence)
		}
	})

	t.Run("GetLogBySequence", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, "test-ledger", testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(t, s, 0, testLogs...)

		log, err := s.GetLogBySequence(1)
		require.NoError(t, err)
		require.NotNil(t, log)
		require.Equal(t, uint64(1), log.Sequence)

		log, err = s.GetLogBySequence(999)
		require.NoError(t, err)
		require.Nil(t, log)
	})

	t.Run("GetAccountMetadata", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, "test-ledger", testLedgerID)
		batch := s.NewBatch(0)
		require.NoError(t, batch.SaveAccountMetadata(testLedgerID, "bank", &commonpb.Metadata{
			Entries: metadata.Metadata{
				"account_type": "asset",
			},
		}))
		require.NoError(t, batch.SaveAccountMetadata(testLedgerID, "bank", &commonpb.Metadata{
			Entries: metadata.Metadata{
				"label": "Bank Account",
			},
		}))
		require.NoError(t, batch.DeleteAccountMetadata(testLedgerID, "bank", []string{"old_key"}))
		require.NoError(t, batch.Commit())

		accountsMetadata, err := s.GetAccountMetadata(testLedgerID, []string{"bank", "user", "world", "non-existing"})
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

		emptyMetadata, err := s.GetAccountMetadata(testLedgerID, []string{})
		require.NoError(t, err)
		require.NotNil(t, emptyMetadata)
		require.Empty(t, emptyMetadata)
	})

	t.Run("GetSequenceForIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, "test-ledger", testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(t, s, 0, testLogs...)

		sequence, err := s.GetSequenceForIdempotencyKey("idempotency-key-1")
		require.NoError(t, err)
		require.Equal(t, uint64(1), sequence)

		sequence, err = s.GetSequenceForIdempotencyKey("non-existing-key")
		require.NoError(t, err)
		require.Equal(t, uint64(0), sequence)

		sequence, err = s.GetSequenceForIdempotencyKey("")
		require.NoError(t, err)
		require.Equal(t, uint64(0), sequence)
	})

	t.Run("AppendLogsEmpty", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		appendLogs(t, s, 0)
	})

	t.Run("GetLastSequence", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, "test-ledger", testLedgerID)

		// Test with no logs - should return 0
		lastSequence, err := s.GetLastSequence()
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastSequence)

		// Insert logs and verify last sequence
		testLogs := createTestLogs(testLedgerID)
		appendLogs(t, s, 0, testLogs...)

		lastSequence, err = s.GetLastSequence()
		require.NoError(t, err)
		require.Equal(t, uint64(4), lastSequence) // Last log has sequence 4
	})

	t.Run("ListLedgers", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		// Initially no ledgers
		cursor, err := s.ListLedgers()
		require.NoError(t, err)
		ledgers, err := collectLedgers(cursor)
		require.NoError(t, err)
		require.Empty(t, ledgers)

		// Register first ledger
		registerLedger(t, s, "ledger-1", 1)
		cursor, err = s.ListLedgers()
		require.NoError(t, err)
		ledgers, err = collectLedgers(cursor)
		require.NoError(t, err)
		require.Len(t, ledgers, 1)
		require.Equal(t, "ledger-1", ledgers[0].Name)
		require.Equal(t, uint32(1), ledgers[0].Id)

		// Register second ledger
		registerLedger(t, s, "ledger-2", 2)
		cursor, err = s.ListLedgers()
		require.NoError(t, err)
		ledgers, err = collectLedgers(cursor)
		require.NoError(t, err)
		require.Len(t, ledgers, 2)
	})

	t.Run("GetLedgerByName", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, "my-ledger", 42)

		ledger, err := s.GetLedgerByName("my-ledger")
		require.NoError(t, err)
		require.NotNil(t, ledger)
		require.Equal(t, "my-ledger", ledger.Name)
		require.Equal(t, uint32(42), ledger.Id)

		ledger, err = s.GetLedgerByName("non-existing")
		require.Error(t, err)
		require.Nil(t, ledger)
	})

}

// createTestLogs creates test logs wrapped in Log with ApplyLog payload
func createTestLogs(ledgerID uint32) []*commonpb.Log {
	return createTestLogsForLedger(ledgerID, 1)
}

// createTestLogsForLedger creates test logs with custom starting sequence
func createTestLogsForLedger(ledgerID uint32, startSequence uint64) []*commonpb.Log {
	now := time.Now()

	logs := []*commonpb.Log{
		{
			Sequence: startSequence,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerId: ledgerID,
					Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
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
						WithID(1).
						WithDate(now),
			},
		}},
		Idempotency: &commonpb.Idempotency{
			Key:  "idempotency-key-1",
				Hash: []byte("hash-1"),
			},
		},
		{
			Sequence: startSequence + 1,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerId: ledgerID,
					Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
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
						WithID(2).
						WithDate(now.Add(time.Second)),
			},
		}},
		Idempotency: &commonpb.Idempotency{
			Key:  "idempotency-key-2",
				Hash: []byte("hash-2"),
			},
		},
	{
		Sequence: startSequence + 2,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerId: ledgerID,
				Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_SavedMetadata{
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
					WithID(3).
					WithDate(now.Add(2 * time.Second)),
			},
		}},
	},
	{
		Sequence: startSequence + 3,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerId: ledgerID,
				Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
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
					WithID(4).
					WithDate(now.Add(3 * time.Second)),
			},
		}},
	},
}

	return logs
}

func TestStoreSnapshot(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	tmpDir := t.TempDir()
	s, err := NewStore(tmpDir, logger, meter, DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Create some data
	registerLedger(t, s, "test-ledger", 1)
	testLogs := createTestLogs(1)
	appendLogs(t, s, 0, testLogs...)

	// Create snapshot
	err = s.CreateSnapshot()
	require.NoError(t, err)

	// Verify data still accessible after snapshot
	lastSequence, err := s.GetLastSequence()
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastSequence)
}

func TestStoreLastAppliedIndex(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	tmpDir := t.TempDir()
	s, err := NewStore(tmpDir, logger, meter, DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Initial value should be 0
	lastIndex, err := s.GetLastAppliedIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastIndex)

	// Create batch with index 5
	batch := s.NewBatch(5)
	require.NoError(t, batch.SaveLedger(&commonpb.LedgerInfo{
		Id:   1,
		Name: "test",
	}))
	require.NoError(t, batch.Commit())

	// Verify last applied index updated
	lastIndex, err = s.GetLastAppliedIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), lastIndex)

	// Create another batch with index 10
	batch = s.NewBatch(10)
	require.NoError(t, batch.SaveLedger(&commonpb.LedgerInfo{
		Id:   2,
		Name: "test2",
	}))
	require.NoError(t, batch.Commit())

	lastIndex, err = s.GetLastAppliedIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(10), lastIndex)
}

func TestStoreTransactionIDIndex(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	tmpDir := t.TempDir()
	s, err := NewStore(tmpDir, logger, meter, DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	var ledgerID uint32 = 1
	registerLedger(t, s, "test-ledger", ledgerID)

	// Store transaction IDs
	batch := s.NewBatch(1)
	require.NoError(t, batch.StoreTransactionID(ledgerID, 100, 1))
	require.NoError(t, batch.StoreTransactionID(ledgerID, 200, 2))
	require.NoError(t, batch.Commit())

	// Retrieve transaction IDs
	sequence, err := s.GetSequenceForTransactionID(ledgerID, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(1), sequence)

	sequence, err = s.GetSequenceForTransactionID(ledgerID, 200)
	require.NoError(t, err)
	require.Equal(t, uint64(2), sequence)

	// Non-existing transaction
	sequence, err = s.GetSequenceForTransactionID(ledgerID, 999)
	require.NoError(t, err)
	require.Equal(t, uint64(0), sequence)
}

func TestStoreRevertedTransactionIndex(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	tmpDir := t.TempDir()
	s, err := NewStore(tmpDir, logger, meter, DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	var ledgerID uint32 = 1
	registerLedger(t, s, "test-ledger", ledgerID)

	// Initially not reverted
	reverted, err := s.IsTransactionReverted(ledgerID, 100)
	require.NoError(t, err)
	require.False(t, reverted)

	// Mark as reverted
	batch := s.NewBatch(1)
	require.NoError(t, batch.StoreRevertedTransactionID(ledgerID, 100, 1))
	require.NoError(t, batch.Commit())

	// Now should be reverted
	reverted, err = s.IsTransactionReverted(ledgerID, 100)
	require.NoError(t, err)
	require.True(t, reverted)

	// Other transaction still not reverted
	reverted, err = s.IsTransactionReverted(ledgerID, 200)
	require.NoError(t, err)
	require.False(t, reverted)
}

func TestStoreDeleteLedger(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	tmpDir := t.TempDir()
	s, err := NewStore(tmpDir, logger, meter, DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	var ledgerID uint32 = 1
	registerLedger(t, s, "test-ledger", ledgerID)

	// Add some data
	batch := s.NewBatch(1)
	require.NoError(t, batch.AppendBalanceDiff(store.BalanceDiff{LedgerID: ledgerID, Account: "world", Asset: "USD", Diff: commonpb.NewBigInt(big.NewInt(-100)), RaftIndex: 1}))
	require.NoError(t, batch.SaveAccountMetadata(ledgerID, "bank", &commonpb.Metadata{
		Entries: metadata.Metadata{"key": "value"},
	}))
	require.NoError(t, batch.StoreTransactionID(ledgerID, 1, 1))
	require.NoError(t, batch.Commit())

	// Verify data exists
	cursor, err := s.ListLedgers()
	require.NoError(t, err)
	ledgers, err := collectLedgers(cursor)
	require.NoError(t, err)
	require.Len(t, ledgers, 1)

	// Delete ledger
	batch = s.NewBatch(2)
	require.NoError(t, batch.DeleteLedger(ledgerID))
	require.NoError(t, batch.Commit())

	// Verify ledger deleted
	cursor, err = s.ListLedgers()
	require.NoError(t, err)
	ledgers, err = collectLedgers(cursor)
	require.NoError(t, err)
	require.Empty(t, ledgers)
}
