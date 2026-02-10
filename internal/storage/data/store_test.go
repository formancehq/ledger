package data_test

import (
	"io"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

// collectLedgers collects all ledgers from a cursor into a slice
func collectLedgers(cursor data.Cursor[*commonpb.LedgerInfo]) ([]*commonpb.LedgerInfo, error) {
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
	testStoreCommon(t, func(t *testing.T) *data.Store {
		tmpDir := t.TempDir()
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)
		meter := noop.NewMeterProvider().Meter("test")

		s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		return s
	})
}

// registerLedger is a helper function to register a ledger and return its ID
func registerLedger(t *testing.T, s *data.Store, name string, id uint32) {
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
func appendLogs(t *testing.T, s *data.Store, lastAppliedIndex uint64, logs ...*commonpb.Log) {
	t.Helper()
	batch := s.NewBatch()
	err := batch.AppendLogs(logs...)
	require.NoError(t, err)
	require.NoError(t, batch.SetAppliedIndex(lastAppliedIndex))
	require.NoError(t, batch.Commit())
}

func testStoreCommon(t *testing.T, createStore func(*testing.T) *data.Store) {
	t.Parallel()

	const (
		testLedgerName = "test-ledger"
		testLedgerID   = uint32(1)
	)

	t.Run("AppendLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, testLedgerName, testLedgerID)
		testLogs := createTestLogs(testLedgerName)
		appendLogs(t, s, 0, testLogs...)
	})

	t.Run("InputOutputCalculation", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)
		attrs := attributes.New()

		registerLedger(t, s, testLedgerName, testLedgerID)
		batch := s.NewBatch()

		// world sends 100 (output)
		worldKey := data.VolumeKey{AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "world"}, Asset: "USD"}
		worldCanonicalKey := worldKey.Bytes()
		require.NoError(t, attrs.Output.AddDiff(batch, 1, worldCanonicalKey, commonpb.NewBigInt(big.NewInt(100))))

		// bank receives 100 (input)
		bankKey := data.VolumeKey{AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "bank"}, Asset: "USD"}
		bankCanonicalKey := bankKey.Bytes()
		require.NoError(t, attrs.Input.AddDiff(batch, 1, bankCanonicalKey, commonpb.NewBigInt(big.NewInt(100))))

		// user receives 50 (input)
		userKey := data.VolumeKey{AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "user"}, Asset: "USD"}
		userCanonicalKey := userKey.Bytes()
		require.NoError(t, attrs.Input.AddDiff(batch, 2, userCanonicalKey, commonpb.NewBigInt(big.NewInt(50))))

		// bank sends 50 (output)
		require.NoError(t, attrs.Output.AddDiff(batch, 2, bankCanonicalKey, commonpb.NewBigInt(big.NewInt(50))))

		require.NoError(t, batch.Commit())

		// Test Input/Output for each account
		// world: input=0, output=100 → balance = -100
		worldInput, err := attrs.Input.ComputeValue(s, 100, worldCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(0), worldInput.Value())
		worldOutput, err := attrs.Output.ComputeValue(s, 100, worldCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(100), worldOutput.Value())

		// bank: input=100, output=50 → balance = 50
		bankInput, err := attrs.Input.ComputeValue(s, 100, bankCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(100), bankInput.Value())
		bankOutput, err := attrs.Output.ComputeValue(s, 100, bankCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(50), bankOutput.Value())

		// user: input=50, output=0 → balance = 50
		userInput, err := attrs.Input.ComputeValue(s, 100, userCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(50), userInput.Value())
		userOutput, err := attrs.Output.ComputeValue(s, 100, userCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(0), userOutput.Value())
	})

	t.Run("GetLogBySequence", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, testLedgerName, testLedgerID)
		testLogs := createTestLogs(testLedgerName)
		appendLogs(t, s, 0, testLogs...)

		log, err := s.GetLogBySequence(1)
		require.NoError(t, err)
		require.NotNil(t, log)
		require.Equal(t, uint64(1), log.Sequence)

		log, err = s.GetLogBySequence(999)
		require.NoError(t, err)
		require.Nil(t, log)
	})

	t.Run("GetSequenceForIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, testLedgerName, testLedgerID)
		testLogs := createTestLogs(testLedgerName)
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
		testLogs := createTestLogs(testLedgerName)
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
func createTestLogs(ledgerName string) []*commonpb.Log {
	return createTestLogsForLedger(ledgerName, 1)
}

// createTestLogsForLedger creates test logs with custom starting sequence
func createTestLogsForLedger(ledgerName string, startSequence uint64) []*commonpb.Log {
	now := time.Now()

	logs := []*commonpb.Log{
		{
			Sequence: startSequence,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledgerName,
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
				Key: "idempotency-key-1",
			},
		},
		{
			Sequence: startSequence + 1,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledgerName,
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
				Key: "idempotency-key-2",
			},
		},
		{
			Sequence: startSequence + 2,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledgerName,
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
					LedgerName: ledgerName,
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
	s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Create some data
	registerLedger(t, s, "test-ledger", 1)
	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 0, testLogs...)

	// Create snapshot
	checkpointID, err := s.CreateSnapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), checkpointID)

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
	s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
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

func TestStoreSoftDeleteLedger(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	tmpDir := t.TempDir()
	s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	attrs := attributes.New()

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
	worldKey := data.VolumeKey{AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "world"}, Asset: "USD"}
	worldCanonicalKey := worldKey.Bytes()
	require.NoError(t, attrs.Output.AddDiff(batch, 1, worldCanonicalKey, commonpb.NewBigInt(big.NewInt(100))))
	metadataKey := data.MetadataKey{AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "bank"}, Key: "key"}
	metadataCanonicalKey := metadataKey.Bytes()
	require.NoError(t, attrs.Metadata.AddDiff(batch, 1, metadataCanonicalKey, &commonpb.MetadataValue{Value: "value"}))
	require.NoError(t, batch.StoreTransactionUpdate(data.TransactionKey{LedgerID: ledgerID, ID: 1}, &commonpb.TransactionUpdate{
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
	outputResult, err := attrs.Output.ComputeValue(s, 100, worldCanonicalKey)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(100), outputResult.Value())
}

func TestInputOutput(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	tmpDir := t.TempDir()
	s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	attrs := attributes.New()

	const (
		ledgerName = "test-ledger"
		ledgerID   = uint32(1)
	)
	registerLedger(t, s, ledgerName, ledgerID)

	bankUSD := data.VolumeKey{AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "bank"}, Asset: "USD"}
	userUSD := data.VolumeKey{AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "user"}, Asset: "USD"}
	bankEUR := data.VolumeKey{AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "bank"}, Asset: "EUR"}

	// Pre-compute canonical keys for each key
	bankUSDKey := bankUSD.Bytes()
	userUSDKey := userUSD.Bytes()
	bankEURKey := bankEUR.Bytes()

	getInput := func(canonicalKey []byte, index uint64) *big.Int {
		result, err := attrs.Input.ComputeValue(s, index, canonicalKey)
		require.NoError(t, err)
		return result.Value()
	}

	getOutput := func(canonicalKey []byte, index uint64) *big.Int {
		result, err := attrs.Output.ComputeValue(s, index, canonicalKey)
		require.NoError(t, err)
		return result.Value()
	}

	// Initially input and output should be 0
	require.Equal(t, big.NewInt(0), getInput(bankUSDKey, 100))
	require.Equal(t, big.NewInt(0), getOutput(bankUSDKey, 100))

	// Add some inputs and outputs
	batch := s.NewBatch()
	require.NoError(t, attrs.Input.AddDiff(batch, 1, bankUSDKey, commonpb.NewBigInt(big.NewInt(100))))
	require.NoError(t, attrs.Input.AddDiff(batch, 2, bankUSDKey, commonpb.NewBigInt(big.NewInt(50))))
	require.NoError(t, attrs.Output.AddDiff(batch, 3, bankUSDKey, commonpb.NewBigInt(big.NewInt(30))))
	require.NoError(t, batch.Commit())

	// Input should be sum of inputs: 100 + 50 = 150
	require.Equal(t, big.NewInt(150), getInput(bankUSDKey, 100))
	// Output should be 30
	require.Equal(t, big.NewInt(30), getOutput(bankUSDKey, 100))

	// Input at index 2 should be: 100 + 50 = 150
	require.Equal(t, big.NewInt(150), getInput(bankUSDKey, 2))
	// Output at index 2 should be: 0 (output is at index 3)
	require.Equal(t, big.NewInt(0), getOutput(bankUSDKey, 2))

	// Input at index 1 should be: 100
	require.Equal(t, big.NewInt(100), getInput(bankUSDKey, 1))

	// Input/Output at index 0 should be: 0 (no diffs before index 1)
	require.Equal(t, big.NewInt(0), getInput(bankUSDKey, 0))
	require.Equal(t, big.NewInt(0), getOutput(bankUSDKey, 0))

	// Add a base at index 10
	batch = s.NewBatch()
	require.NoError(t, attrs.Input.SetBase(batch, 10, bankUSDKey, commonpb.NewBigInt(big.NewInt(1000))))
	require.NoError(t, batch.Commit())

	// Input at index 100 should use base 1000 (diffs 1,2 are before base, so ignored)
	require.Equal(t, big.NewInt(1000), getInput(bankUSDKey, 100))

	// Input at index 10 should be the base: 1000
	require.Equal(t, big.NewInt(1000), getInput(bankUSDKey, 10))

	// Input at index 9 should still use diffs only (base not visible): 100 + 50 = 150
	require.Equal(t, big.NewInt(150), getInput(bankUSDKey, 9))

	// Add diffs after the base
	batch = s.NewBatch()
	require.NoError(t, attrs.Input.AddDiff(batch, 11, bankUSDKey, commonpb.NewBigInt(big.NewInt(200))))
	require.NoError(t, attrs.Output.AddDiff(batch, 12, bankUSDKey, commonpb.NewBigInt(big.NewInt(50))))
	require.NoError(t, batch.Commit())

	// Input at index 100 should be: base(1000) + diff at 11(200) = 1200
	require.Equal(t, big.NewInt(1200), getInput(bankUSDKey, 100))

	// Output at index 100 should be: 30 + 50 = 80
	require.Equal(t, big.NewInt(80), getOutput(bankUSDKey, 100))

	// Input at index 11 should be: base(1000) + diff at 11(200) = 1200
	require.Equal(t, big.NewInt(1200), getInput(bankUSDKey, 11))

	// Add a newer base at index 20
	batch = s.NewBatch()
	require.NoError(t, attrs.Input.SetBase(batch, 20, bankUSDKey, commonpb.NewBigInt(big.NewInt(5000))))
	require.NoError(t, batch.Commit())

	// Input at index 100 should use newer base: 5000
	require.Equal(t, big.NewInt(5000), getInput(bankUSDKey, 100))

	// Input at index 15 should use older base + diffs after it: 1000 + 200 = 1200
	require.Equal(t, big.NewInt(1200), getInput(bankUSDKey, 15))

	// Different account should have 0 input/output
	require.Equal(t, big.NewInt(0), getInput(userUSDKey, 100))
	require.Equal(t, big.NewInt(0), getOutput(userUSDKey, 100))

	// Different asset should have 0 input/output
	require.Equal(t, big.NewInt(0), getInput(bankEURKey, 100))
	require.Equal(t, big.NewInt(0), getOutput(bankEURKey, 100))

	// Non-existing ledger should have 0 input/output
	nonExistingKey := data.VolumeKey{AccountKey: data.AccountKey{LedgerID: 999, Account: "bank"}, Asset: "USD"}
	nonExistingCanonicalKey := nonExistingKey.Bytes()
	require.Equal(t, big.NewInt(0), getInput(nonExistingCanonicalKey, 100))
	require.Equal(t, big.NewInt(0), getOutput(nonExistingCanonicalKey, 100))
}
