package data_test

import (
	"io"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
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

		// Index 1: world sends 100 to bank
		worldKey := data.VolumeKey{AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "world"}, Asset: "USD"}
		worldCanonicalKey := worldKey.Bytes()
		require.NoError(t, attrs.Volume.AddDiff(batch, 1, worldCanonicalKey, &raftcmdpb.VolumePair{
			OutputKnown: commonpb.NewUint256FromUint64(100),
		}))

		bankKey := data.VolumeKey{AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "bank"}, Asset: "USD"}
		bankCanonicalKey := bankKey.Bytes()
		require.NoError(t, attrs.Volume.AddDiff(batch, 1, bankCanonicalKey, &raftcmdpb.VolumePair{
			InputKnown: commonpb.NewUint256FromUint64(100),
		}))

		// Index 2: bank sends 50 to user (bank cumulative: input=100, output=50)
		userKey := data.VolumeKey{AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "user"}, Asset: "USD"}
		userCanonicalKey := userKey.Bytes()
		require.NoError(t, attrs.Volume.AddDiff(batch, 2, bankCanonicalKey, &raftcmdpb.VolumePair{
			InputKnown:  commonpb.NewUint256FromUint64(100),
			OutputKnown: commonpb.NewUint256FromUint64(50),
		}))
		require.NoError(t, attrs.Volume.AddDiff(batch, 2, userCanonicalKey, &raftcmdpb.VolumePair{
			InputKnown: commonpb.NewUint256FromUint64(50),
		}))

		require.NoError(t, batch.Commit())

		// world: input=0, output=100 → balance = -100
		worldVolume, err := attrs.Volume.ComputeValue(s, 100, worldCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(0), worldVolume.InputKnown.ToBigInt())
		require.Equal(t, big.NewInt(100), worldVolume.OutputKnown.ToBigInt())

		// bank: input=100, output=50 → balance = 50
		bankVolume, err := attrs.Volume.ComputeValue(s, 100, bankCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(100), bankVolume.InputKnown.ToBigInt())
		require.Equal(t, big.NewInt(50), bankVolume.OutputKnown.ToBigInt())

		// user: input=50, output=0 → balance = 50
		userVolume, err := attrs.Volume.ComputeValue(s, 100, userCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(50), userVolume.InputKnown.ToBigInt())
		require.Equal(t, big.NewInt(0), userVolume.OutputKnown.ToBigInt())
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
	require.NoError(t, attrs.Volume.AddDiff(batch, 1, worldCanonicalKey, &raftcmdpb.VolumePair{
		OutputKnown: commonpb.NewUint256FromUint64(100),
	}))
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
	volumeResult, err := attrs.Volume.ComputeValue(s, 100, worldCanonicalKey)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(100), volumeResult.OutputKnown.ToBigInt())
}

func TestStorePeriods(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	t.Run("EmptyStore", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		periods, err := s.GetPeriods()
		require.NoError(t, err)
		require.Nil(t, periods)

		nextID, err := s.GetNextPeriodID()
		require.NoError(t, err)
		require.Equal(t, uint64(1), nextID)
	})

	t.Run("StoreSinglePeriod", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		batch := s.NewBatch()
		require.NoError(t, batch.StorePeriod(&commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, batch.StoreNextPeriodID(2))
		require.NoError(t, batch.Commit())

		periods, err := s.GetPeriods()
		require.NoError(t, err)
		require.Len(t, periods, 1)
		require.Equal(t, uint64(1), periods[0].Id)
		require.Equal(t, uint64(1000), periods[0].Start.Data)
		require.Equal(t, commonpb.PeriodStatus_PERIOD_OPEN, periods[0].Status)

		nextID, err := s.GetNextPeriodID()
		require.NoError(t, err)
		require.Equal(t, uint64(2), nextID)
	})

	t.Run("StoreMultiplePeriodsOrderedByID", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		// Insert periods out of order
		batch := s.NewBatch()
		require.NoError(t, batch.StorePeriod(&commonpb.Period{
			Id:     3,
			Start:  &commonpb.Timestamp{Data: 3000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, batch.StorePeriod(&commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			End:    &commonpb.Timestamp{Data: 2000},
			Status: commonpb.PeriodStatus_PERIOD_CLOSED,
		}))
		require.NoError(t, batch.StorePeriod(&commonpb.Period{
			Id:            2,
			Start:         &commonpb.Timestamp{Data: 2000},
			End:           &commonpb.Timestamp{Data: 3000},
			Status:        commonpb.PeriodStatus_PERIOD_CLOSED,
			CloseSequence: 10,
			SealingHash:   []byte("hash-2"),
		}))
		require.NoError(t, batch.StoreNextPeriodID(4))
		require.NoError(t, batch.Commit())

		// Verify periods are returned ordered by ID
		periods, err := s.GetPeriods()
		require.NoError(t, err)
		require.Len(t, periods, 3)
		require.Equal(t, uint64(1), periods[0].Id)
		require.Equal(t, uint64(2), periods[1].Id)
		require.Equal(t, uint64(3), periods[2].Id)

		// Verify fields
		require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSED, periods[0].Status)
		require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSED, periods[1].Status)
		require.Equal(t, commonpb.PeriodStatus_PERIOD_OPEN, periods[2].Status)
		require.Equal(t, uint64(10), periods[1].CloseSequence)
		require.Equal(t, []byte("hash-2"), periods[1].SealingHash)

		nextID, err := s.GetNextPeriodID()
		require.NoError(t, err)
		require.Equal(t, uint64(4), nextID)
	})

	t.Run("UpdateExistingPeriod", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		// Store initial period
		batch := s.NewBatch()
		require.NoError(t, batch.StorePeriod(&commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, batch.StoreNextPeriodID(2))
		require.NoError(t, batch.Commit())

		// Update the same period (close it)
		batch = s.NewBatch()
		require.NoError(t, batch.StorePeriod(&commonpb.Period{
			Id:            1,
			Start:         &commonpb.Timestamp{Data: 1000},
			End:           &commonpb.Timestamp{Data: 2000},
			Status:        commonpb.PeriodStatus_PERIOD_CLOSED,
			CloseSequence: 5,
			SealingHash:   []byte("sealed"),
		}))
		require.NoError(t, batch.Commit())

		periods, err := s.GetPeriods()
		require.NoError(t, err)
		require.Len(t, periods, 1)
		require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSED, periods[0].Status)
		require.Equal(t, uint64(5), periods[0].CloseSequence)
		require.Equal(t, []byte("sealed"), periods[0].SealingHash)
	})

	t.Run("PersistAcrossReopen", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()

		// Store periods and close
		s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
		require.NoError(t, err)

		batch := s.NewBatch()
		require.NoError(t, batch.StorePeriod(&commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			End:    &commonpb.Timestamp{Data: 2000},
			Status: commonpb.PeriodStatus_PERIOD_CLOSED,
		}))
		require.NoError(t, batch.StorePeriod(&commonpb.Period{
			Id:     2,
			Start:  &commonpb.Timestamp{Data: 2000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, batch.StoreNextPeriodID(3))
		require.NoError(t, batch.Commit())

		// Create snapshot so data survives reopen (writes use NoSync)
		_, err = s.CreateSnapshot()
		require.NoError(t, err)
		require.NoError(t, s.Close())

		// Reopen and verify
		s2, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s2.Close() })

		periods, err := s2.GetPeriods()
		require.NoError(t, err)
		require.Len(t, periods, 2)
		require.Equal(t, uint64(1), periods[0].Id)
		require.Equal(t, uint64(2), periods[1].Id)

		nextID, err := s2.GetNextPeriodID()
		require.NoError(t, err)
		require.Equal(t, uint64(3), nextID)
	})

	t.Run("NextPeriodIDUpdate", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		// Set to 5
		batch := s.NewBatch()
		require.NoError(t, batch.StoreNextPeriodID(5))
		require.NoError(t, batch.Commit())

		nextID, err := s.GetNextPeriodID()
		require.NoError(t, err)
		require.Equal(t, uint64(5), nextID)

		// Update to 10
		batch = s.NewBatch()
		require.NoError(t, batch.StoreNextPeriodID(10))
		require.NoError(t, batch.Commit())

		nextID, err = s.GetNextPeriodID()
		require.NoError(t, err)
		require.Equal(t, uint64(10), nextID)
	})

	t.Run("AtomicBatchWithPeriodsAndLogs", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		registerLedger(t, s, "test-ledger", 1)

		// Store periods, nextPeriodID, and logs in the same batch
		batch := s.NewBatch()
		require.NoError(t, batch.StorePeriod(&commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, batch.StoreNextPeriodID(2))
		require.NoError(t, batch.SetAppliedIndex(42))
		require.NoError(t, batch.Commit())

		// Verify all data was written atomically
		periods, err := s.GetPeriods()
		require.NoError(t, err)
		require.Len(t, periods, 1)

		nextID, err := s.GetNextPeriodID()
		require.NoError(t, err)
		require.Equal(t, uint64(2), nextID)

		lastIndex, err := s.GetLastAppliedIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(42), lastIndex)
	})
}

func TestVolume(t *testing.T) {
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

	bankUSDKey := bankUSD.Bytes()
	userUSDKey := userUSD.Bytes()
	bankEURKey := bankEUR.Bytes()

	getVolume := func(canonicalKey []byte, index uint64) *raftcmdpb.VolumePair {
		result, err := attrs.Volume.ComputeValue(s, index, canonicalKey)
		require.NoError(t, err)
		return result
	}

	// Initially volume should be {input: 0, output: 0}
	v := getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(0), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())

	// Add cumulative diffs. Each diff is cumulative since the last base.
	// In the merged Volume model, each diff carries both input and output sides.
	batch := s.NewBatch()
	require.NoError(t, attrs.Volume.AddDiff(batch, 1, bankUSDKey, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 2, bankUSDKey, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(150),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 3, bankUSDKey, &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(150),
		OutputKnown: commonpb.NewUint256FromUint64(30),
	}))
	require.NoError(t, batch.Commit())

	// At boundary 100: lastDiff at index 3 → input=150, output=30
	v = getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(150), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(30), v.OutputKnown.ToBigInt())

	// At boundary 2: lastDiff at index 2 → input=150, output=0
	v = getVolume(bankUSDKey, 2)
	require.Equal(t, big.NewInt(150), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())

	// At boundary 1: lastDiff at index 1 → input=100, output=0
	v = getVolume(bankUSDKey, 1)
	require.Equal(t, big.NewInt(100), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())

	// At boundary 0: no diffs → input=0, output=0
	v = getVolume(bankUSDKey, 0)
	require.Equal(t, big.NewInt(0), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())

	// Add a base at index 10 (captures cumulative state: input=1000, output=30)
	batch = s.NewBatch()
	require.NoError(t, attrs.Volume.SetBase(batch, 10, bankUSDKey, &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(1000),
		OutputKnown: commonpb.NewUint256FromUint64(30),
	}))
	require.NoError(t, batch.Commit())

	// At boundary 100: base at 10, no diffs after → input=1000, output=30
	v = getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(1000), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(30), v.OutputKnown.ToBigInt())

	// At boundary 10: base → input=1000, output=30
	v = getVolume(bankUSDKey, 10)
	require.Equal(t, big.NewInt(1000), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(30), v.OutputKnown.ToBigInt())

	// At boundary 9: base not visible, diffs only → lastDiff at 3: input=150, output=30
	v = getVolume(bankUSDKey, 9)
	require.Equal(t, big.NewInt(150), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(30), v.OutputKnown.ToBigInt())

	// Add cumulative diffs after the base
	batch = s.NewBatch()
	require.NoError(t, attrs.Volume.AddDiff(batch, 11, bankUSDKey, &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(200),
		OutputKnown: commonpb.NewUint256FromUint64(50),
	}))
	require.NoError(t, batch.Commit())

	// At boundary 100: base(1000,30) + diff(200,50) → input=1200, output=80
	v = getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(1200), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(80), v.OutputKnown.ToBigInt())

	// At boundary 11: base(1000,30) + diff(200,50) → input=1200, output=80
	v = getVolume(bankUSDKey, 11)
	require.Equal(t, big.NewInt(1200), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(80), v.OutputKnown.ToBigInt())

	// Add a newer base at index 20
	batch = s.NewBatch()
	require.NoError(t, attrs.Volume.SetBase(batch, 20, bankUSDKey, &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(5000),
		OutputKnown: commonpb.NewUint256FromUint64(80),
	}))
	require.NoError(t, batch.Commit())

	// At boundary 100: newer base → input=5000, output=80
	v = getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(5000), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(80), v.OutputKnown.ToBigInt())

	// At boundary 15: older base + diffs → input=1200, output=80
	v = getVolume(bankUSDKey, 15)
	require.Equal(t, big.NewInt(1200), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(80), v.OutputKnown.ToBigInt())

	// Different account should have 0 volume
	v = getVolume(userUSDKey, 100)
	require.Equal(t, big.NewInt(0), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())

	// Different asset should have 0 volume
	v = getVolume(bankEURKey, 100)
	require.Equal(t, big.NewInt(0), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())

	// Non-existing ledger should have 0 volume
	nonExistingKey := data.VolumeKey{AccountKey: data.AccountKey{LedgerID: 999, Account: "bank"}, Asset: "USD"}
	nonExistingCanonicalKey := nonExistingKey.Bytes()
	v = getVolume(nonExistingCanonicalKey, 100)
	require.Equal(t, big.NewInt(0), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())
}

func TestSigningKeyPersistence(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	tmpDir := t.TempDir()
	s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	t.Run("empty store has no signing keys", func(t *testing.T) {
		keys, err := s.LoadSigningKeys()
		require.NoError(t, err)
		require.Empty(t, keys)

		requireSig, err := s.LoadSigningConfig()
		require.NoError(t, err)
		require.False(t, requireSig)
	})

	t.Run("save and load signing keys", func(t *testing.T) {
		pubKey1 := make([]byte, 32)
		pubKey2 := make([]byte, 32)
		for i := range pubKey1 {
			pubKey1[i] = byte(i)
			pubKey2[i] = byte(i + 100)
		}

		batch := s.NewBatch()
		require.NoError(t, batch.SaveSigningKey("key-1", pubKey1))
		require.NoError(t, batch.SaveSigningKey("key-2", pubKey2))
		require.NoError(t, batch.Commit())

		keys, err := s.LoadSigningKeys()
		require.NoError(t, err)
		require.Len(t, keys, 2)
		require.Equal(t, pubKey1, keys["key-1"])
		require.Equal(t, pubKey2, keys["key-2"])
	})

	t.Run("delete signing key", func(t *testing.T) {
		batch := s.NewBatch()
		require.NoError(t, batch.DeleteSigningKey("key-1"))
		require.NoError(t, batch.Commit())

		keys, err := s.LoadSigningKeys()
		require.NoError(t, err)
		require.Len(t, keys, 1)
		require.Nil(t, keys["key-1"])
		require.NotNil(t, keys["key-2"])
	})

	t.Run("save and load signing config", func(t *testing.T) {
		batch := s.NewBatch()
		require.NoError(t, batch.SaveSigningConfig(true))
		require.NoError(t, batch.Commit())

		requireSig, err := s.LoadSigningConfig()
		require.NoError(t, err)
		require.True(t, requireSig)

		batch = s.NewBatch()
		require.NoError(t, batch.SaveSigningConfig(false))
		require.NoError(t, batch.Commit())

		requireSig, err = s.LoadSigningConfig()
		require.NoError(t, err)
		require.False(t, requireSig)
	})

	t.Run("delete all signing keys", func(t *testing.T) {
		// Add some keys first
		batch := s.NewBatch()
		require.NoError(t, batch.SaveSigningKey("a", make([]byte, 32)))
		require.NoError(t, batch.SaveSigningKey("b", make([]byte, 32)))
		require.NoError(t, batch.SaveSigningKey("c", make([]byte, 32)))
		require.NoError(t, batch.Commit())

		keys, err := s.LoadSigningKeys()
		require.NoError(t, err)
		require.Len(t, keys, 4) // key-2 from previous test + a, b, c

		batch = s.NewBatch()
		require.NoError(t, batch.DeleteAllSigningKeys())
		require.NoError(t, batch.Commit())

		keys, err = s.LoadSigningKeys()
		require.NoError(t, err)
		require.Empty(t, keys)
	})
}
