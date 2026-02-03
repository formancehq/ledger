package store

import (
	"io"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

// collectLedgers collects all ledgers from a cursor into a slice
func collectLedgers(cursor Cursor[*commonpb.LedgerInfo]) ([]*commonpb.LedgerInfo, error) {
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

// ptr returns a pointer to the given string value
func ptr(s string) *string {
	return &s
}

func TestPebbleStore(t *testing.T) {
	testStoreCommon(t, func(t *testing.T) *Store {
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
func registerLedger(t *testing.T, s *Store, name string, id uint32) {
	t.Helper()
	batch := s.NewBatch()
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
func appendLogs(t *testing.T, s *Store, lastAppliedIndex uint64, logs ...*commonpb.Log) {
	t.Helper()
	batch := s.NewBatch()
	err := batch.AppendLogs(logs...)
	require.NoError(t, err)
	require.NoError(t, batch.SetAppliedIndex(lastAppliedIndex))
	require.NoError(t, batch.Commit())
}

func testStoreCommon(t *testing.T, createStore func(*testing.T) *Store) {
	t.Parallel()

	const (
		testLedgerName = "test-ledger"
		testLedgerID   = uint32(1)
	)

	t.Run("AppendLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, testLedgerName, testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(t, s, 0, testLogs...)
	})

	t.Run("BalancesCalculation", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, testLedgerName, testLedgerID)
		batch := s.NewBatch()
		require.NoError(t, batch.AppendBalanceDiff(TimestampedBalanceKey{TimestampedAccountKey: TimestampedAccountKey{AccountKey: AccountKey{LedgerName: testLedgerName, Account: "world"}, RaftIndex: 1}, Asset: "USD"}, commonpb.NewBigInt(big.NewInt(-100))))
		require.NoError(t, batch.AppendBalanceDiff(TimestampedBalanceKey{TimestampedAccountKey: TimestampedAccountKey{AccountKey: AccountKey{LedgerName: testLedgerName, Account: "bank"}, RaftIndex: 1}, Asset: "USD"}, commonpb.NewBigInt(big.NewInt(100))))
		require.NoError(t, batch.AppendBalanceDiff(TimestampedBalanceKey{TimestampedAccountKey: TimestampedAccountKey{AccountKey: AccountKey{LedgerName: testLedgerName, Account: "user"}, RaftIndex: 2}, Asset: "USD"}, commonpb.NewBigInt(big.NewInt(50))))
		require.NoError(t, batch.AppendBalanceDiff(TimestampedBalanceKey{TimestampedAccountKey: TimestampedAccountKey{AccountKey: AccountKey{LedgerName: testLedgerName, Account: "bank"}, RaftIndex: 2}, Asset: "USD"}, commonpb.NewBigInt(big.NewInt(-50))))
		require.NoError(t, batch.Commit())

		diffs, err := s.GetBalanceDiffs(testLedgerName, map[string][]string{
			"world": {"USD"},
			"bank":  {"USD"},
			"user":  {"USD"},
		})
		require.NoError(t, err)

		// Compute balances from diffs
		computeBalance := func(diffs []StoredBalanceDiff) *big.Int {
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

		registerLedger(t, s, testLedgerName, testLedgerID)
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

		registerLedger(t, s, testLedgerName, testLedgerID)
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

		registerLedger(t, s, testLedgerName, testLedgerID)
		batch := s.NewBatch()
		require.NoError(t, batch.AppendMetadataDiff(TimestampedMetadataKey{TimestampedAccountKey: TimestampedAccountKey{AccountKey: AccountKey{LedgerName: testLedgerName, Account: "bank"}, RaftIndex: 1}, Key: "account_type"}, &commonpb.MetadataValue{Value: "asset"}))
		require.NoError(t, batch.AppendMetadataDiff(TimestampedMetadataKey{TimestampedAccountKey: TimestampedAccountKey{AccountKey: AccountKey{LedgerName: testLedgerName, Account: "bank"}, RaftIndex: 2}, Key: "label"}, &commonpb.MetadataValue{Value: "Bank Account"}))
		require.NoError(t, batch.AppendMetadataDiff(TimestampedMetadataKey{TimestampedAccountKey: TimestampedAccountKey{AccountKey: AccountKey{LedgerName: testLedgerName, Account: "bank"}, RaftIndex: 3}, Key: "old_key"}, nil))
		require.NoError(t, batch.Commit())

		accountsMetadata, err := s.GetAccountMetadata(testLedgerName, []string{"bank", "user", "world", "non-existing"})
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

		emptyMetadata, err := s.GetAccountMetadata(testLedgerName, []string{})
		require.NoError(t, err)
		require.NotNil(t, emptyMetadata)
		require.Empty(t, emptyMetadata)
	})

	t.Run("GetSequenceForIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, testLedgerName, testLedgerID)
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

		registerLedger(t, s, testLedgerName, testLedgerID)

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
								AccountMetadata: map[string]*commonpb.MetadataSet{
									"bank": commonpb.MetadataSetFromMap(metadata.Metadata{
										"account_type": "asset",
									}),
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
								Metadata: commonpb.MetadataSetFromMap(metadata.Metadata{
									"label": "Bank Account",
								}),
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
	batch := s.NewBatch()
	require.NoError(t, batch.SaveLedger(&commonpb.LedgerInfo{
		Id:   1,
		Name: "test",
	}))
	require.NoError(t, batch.SetAppliedIndex(5))
	require.NoError(t, batch.Commit())

	// Verify last applied index updated
	lastIndex, err = s.GetLastAppliedIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), lastIndex)

	// Create another batch with index 10
	batch = s.NewBatch()
	require.NoError(t, batch.SaveLedger(&commonpb.LedgerInfo{
		Id:   2,
		Name: "test2",
	}))
	require.NoError(t, batch.SetAppliedIndex(10))
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

	const (
		ledgerName = "test-ledger"
		ledgerID   = uint32(1)
	)
	registerLedger(t, s, ledgerName, ledgerID)

	// Store transaction updates (init)
	batch := s.NewBatch()
	require.NoError(t, batch.StoreTransactionUpdate(ledgerName, 100, &commonpb.TransactionUpdate{
		ByLog: 1,
		Updates: []*commonpb.TransactionUpdateType{
			{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
					TransactionInit: &commonpb.TransactionInit{},
				},
			},
		},
	}))
	require.NoError(t, batch.StoreTransactionUpdate(ledgerName, 200, &commonpb.TransactionUpdate{
		ByLog: 2,
		Updates: []*commonpb.TransactionUpdateType{
			{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
					TransactionInit: &commonpb.TransactionInit{},
				},
			},
		},
	}))
	require.NoError(t, batch.Commit())

	// Retrieve transaction IDs
	sequence, err := s.GetSequenceForTransactionID(ledgerName, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(1), sequence)

	sequence, err = s.GetSequenceForTransactionID(ledgerName, 200)
	require.NoError(t, err)
	require.Equal(t, uint64(2), sequence)

	// Non-existing transaction
	sequence, err = s.GetSequenceForTransactionID(ledgerName, 999)
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

	const (
		ledgerName = "test-ledger"
		ledgerID   = uint32(1)
	)
	registerLedger(t, s, ledgerName, ledgerID)

	// Store transaction init first
	batch := s.NewBatch()
	require.NoError(t, batch.StoreTransactionUpdate(ledgerName, 100, &commonpb.TransactionUpdate{
		ByLog: 1,
		Updates: []*commonpb.TransactionUpdateType{
			{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
					TransactionInit: &commonpb.TransactionInit{},
				},
			},
		},
	}))
	require.NoError(t, batch.Commit())

	// Initially not reverted
	reverted, err := s.IsTransactionReverted(ledgerName, 100)
	require.NoError(t, err)
	require.False(t, reverted)

	// Mark as reverted
	batch = s.NewBatch()
	require.NoError(t, batch.StoreTransactionUpdate(ledgerName, 100, &commonpb.TransactionUpdate{
		ByLog: 2,
		Updates: []*commonpb.TransactionUpdateType{
			{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationRevert{
					TransactionModificationRevert: &commonpb.TransactionUpdateRevert{
						ByTransaction: 101, // ID of the revert transaction
					},
				},
			},
		},
	}))
	require.NoError(t, batch.Commit())

	// Now should be reverted
	reverted, err = s.IsTransactionReverted(ledgerName, 100)
	require.NoError(t, err)
	require.True(t, reverted)

	// Other transaction still not reverted
	reverted, err = s.IsTransactionReverted(ledgerName, 200)
	require.NoError(t, err)
	require.False(t, reverted)
}

func TestStoreSoftDeleteLedger(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	tmpDir := t.TempDir()
	s, err := NewStore(tmpDir, logger, meter, DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	const (
		ledgerName = "test-ledger"
		ledgerID   = uint32(1)
	)
	createdAt := commonpb.NewTimestamp(time.Now())
	batch := s.NewBatch()
	err = batch.SaveLedger(&commonpb.LedgerInfo{
		Id:        ledgerID,
		Name:      ledgerName,
		CreatedAt: createdAt,
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Add some data
	batch = s.NewBatch()
	require.NoError(t, batch.AppendBalanceDiff(TimestampedBalanceKey{TimestampedAccountKey: TimestampedAccountKey{AccountKey: AccountKey{LedgerName: ledgerName, Account: "world"}, RaftIndex: 1}, Asset: "USD"}, commonpb.NewBigInt(big.NewInt(-100))))
	require.NoError(t, batch.AppendMetadataDiff(TimestampedMetadataKey{TimestampedAccountKey: TimestampedAccountKey{AccountKey: AccountKey{LedgerName: ledgerName, Account: "bank"}, RaftIndex: 1}, Key: "key"}, &commonpb.MetadataValue{Value: "value"}))
	require.NoError(t, batch.StoreTransactionUpdate(ledgerName, 1, &commonpb.TransactionUpdate{
		ByLog: 1,
		Updates: []*commonpb.TransactionUpdateType{
			{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
					TransactionInit: &commonpb.TransactionInit{},
				},
			},
		},
	}))
	require.NoError(t, batch.Commit())

	// Verify ledger exists and is not deleted
	cursor, err := s.ListLedgers()
	require.NoError(t, err)
	ledgers, err := collectLedgers(cursor)
	require.NoError(t, err)
	require.Len(t, ledgers, 1)
	require.Nil(t, ledgers[0].DeletedAt)

	// Soft delete ledger
	deletedAt := commonpb.NewTimestamp(time.Now())
	batch = s.NewBatch()
	require.NoError(t, batch.SaveLedger(&commonpb.LedgerInfo{
		Id:        ledgerID,
		Name:      ledgerName,
		CreatedAt: createdAt,
		DeletedAt: deletedAt,
	}))
	require.NoError(t, batch.Commit())

	// Verify ledger still exists but is marked as deleted
	cursor, err = s.ListLedgers()
	require.NoError(t, err)
	ledgers, err = collectLedgers(cursor)
	require.NoError(t, err)
	require.Len(t, ledgers, 1)
	require.NotNil(t, ledgers[0].DeletedAt)
	require.Equal(t, deletedAt.Data, ledgers[0].DeletedAt.Data)

	// Verify data still exists (soft delete doesn't remove data)
	diffs, err := s.GetBalanceDiffs(ledgerName, BalanceDiffsQuery{"world": {"USD"}})
	require.NoError(t, err)
	require.Len(t, diffs["world"]["USD"], 1)
}

func TestStoreBalanceBase(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	tmpDir := t.TempDir()
	s, err := NewStore(tmpDir, logger, meter, DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	const (
		ledgerName = "test-ledger"
		ledgerID   = uint32(1)
	)
	registerLedger(t, s, ledgerName, ledgerID)

	// Initially no balance base should exist
	base, err := s.GetBalanceBase(ledgerName, "bank", "USD", 100)
	require.NoError(t, err)
	require.Nil(t, base)

	// Store balance base at raft index 10
	batch := s.NewBatch()
	require.NoError(t, batch.SetBalanceBase(TimestampedBalanceKey{TimestampedAccountKey: TimestampedAccountKey{AccountKey: AccountKey{LedgerName: ledgerName, Account: "bank"}, RaftIndex: 10}, Asset: "USD"}, commonpb.NewBigInt(big.NewInt(1000))))
	require.NoError(t, batch.Commit())

	// Query with maxRaftIndex >= 10 should return the base
	base, err = s.GetBalanceBase(ledgerName, "bank", "USD", 10)
	require.NoError(t, err)
	require.NotNil(t, base)
	require.Equal(t, uint64(10), base.RaftIndex)
	require.Equal(t, big.NewInt(1000), base.Balance.Value())

	// Query with maxRaftIndex > 10 should also return the base
	base, err = s.GetBalanceBase(ledgerName, "bank", "USD", 100)
	require.NoError(t, err)
	require.NotNil(t, base)
	require.Equal(t, uint64(10), base.RaftIndex)

	// Query with maxRaftIndex < 10 should return nil
	base, err = s.GetBalanceBase(ledgerName, "bank", "USD", 9)
	require.NoError(t, err)
	require.Nil(t, base)

	// Store another balance base at raft index 20
	batch = s.NewBatch()
	require.NoError(t, batch.SetBalanceBase(TimestampedBalanceKey{TimestampedAccountKey: TimestampedAccountKey{AccountKey: AccountKey{LedgerName: ledgerName, Account: "bank"}, RaftIndex: 20}, Asset: "USD"}, commonpb.NewBigInt(big.NewInt(2000))))
	require.NoError(t, batch.Commit())

	// Query with maxRaftIndex = 15 should return base at index 10
	base, err = s.GetBalanceBase(ledgerName, "bank", "USD", 15)
	require.NoError(t, err)
	require.NotNil(t, base)
	require.Equal(t, uint64(10), base.RaftIndex)
	require.Equal(t, big.NewInt(1000), base.Balance.Value())

	// Query with maxRaftIndex = 20 should return base at index 20
	base, err = s.GetBalanceBase(ledgerName, "bank", "USD", 20)
	require.NoError(t, err)
	require.NotNil(t, base)
	require.Equal(t, uint64(20), base.RaftIndex)
	require.Equal(t, big.NewInt(2000), base.Balance.Value())

	// Query with maxRaftIndex = 100 should return base at index 20 (the latest)
	base, err = s.GetBalanceBase(ledgerName, "bank", "USD", 100)
	require.NoError(t, err)
	require.NotNil(t, base)
	require.Equal(t, uint64(20), base.RaftIndex)
	require.Equal(t, big.NewInt(2000), base.Balance.Value())

	// Query for different account should return nil
	base, err = s.GetBalanceBase(ledgerName, "user", "USD", 100)
	require.NoError(t, err)
	require.Nil(t, base)

	// Query for different asset should return nil
	base, err = s.GetBalanceBase(ledgerName, "bank", "EUR", 100)
	require.NoError(t, err)
	require.Nil(t, base)

	// Query for different ledger should return nil
	base, err = s.GetBalanceBase("non-existing-ledger", "bank", "USD", 100)
	require.NoError(t, err)
	require.Nil(t, base)
}
