package state

import (
	"io"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func newTestStore(t *testing.T) *dal.Store {
	t.Helper()
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

func registerLedger(t *testing.T, s *dal.Store, name string) {
	t.Helper()
	batch := s.NewBatch()
	err := SaveLedger(batch, &commonpb.LedgerInfo{
		Name:      name,
		CreatedAt: commonpb.NewTimestamp(libtime.Now()),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

func appendLogs(t *testing.T, s *dal.Store, lastAppliedIndex uint64, logs ...*commonpb.Log) {
	t.Helper()
	batch := s.NewBatch()
	err := AppendLogs(batch, logs...)
	require.NoError(t, err)
	require.NoError(t, SetAppliedIndex(batch, lastAppliedIndex))
	require.NoError(t, batch.Commit())
}

func collectLedgers(cursor dal.Cursor[*commonpb.LedgerInfo]) ([]*commonpb.LedgerInfo, error) {
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

func collectLogs(t *testing.T, cursor dal.Cursor[*commonpb.Log]) []*commonpb.Log {
	t.Helper()
	defer func() { _ = cursor.Close() }()

	var logs []*commonpb.Log
	for {
		log, err := cursor.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		logs = append(logs, log)
	}
	return logs
}

func createTestLogs(ledgerName string) []*commonpb.Log {
	return createTestLogsForLedger(ledgerName, 1)
}

func createTestLogsForLedger(ledgerName string, startSequence uint64) []*commonpb.Log {
	now := libtime.Now()

	return []*commonpb.Log{
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
						WithDate(now.Add(libtime.Second)),
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
						WithDate(now.Add(2 * libtime.Second)),
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
						WithDate(now.Add(3 * libtime.Second)),
				},
			}},
		},
	}
}

func TestReadLogBySequence(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	registerLedger(t, s, "test-ledger")
	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 0, testLogs...)

	log, err := ReadLogBySequence(s, 1)
	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, uint64(1), log.Sequence)

	log, err = ReadLogBySequence(s, 999)
	require.NoError(t, err)
	require.Nil(t, log)
}

func TestReadLastSequence(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	registerLedger(t, s, "test-ledger")

	// Test with no logs - should return 0
	lastSequence, err := ReadLastSequence(s)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastSequence)

	// Insert logs and verify last sequence
	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 0, testLogs...)

	lastSequence, err = ReadLastSequence(s)
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastSequence) // Last log has sequence 4
}

func TestReadLedgers(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Initially no ledgers
	cursor, err := ReadLedgers(s)
	require.NoError(t, err)
	ledgers, err := collectLedgers(cursor)
	require.NoError(t, err)
	require.Empty(t, ledgers)

	// Register first ledger
	registerLedger(t, s, "ledger-1")
	cursor, err = ReadLedgers(s)
	require.NoError(t, err)
	ledgers, err = collectLedgers(cursor)
	require.NoError(t, err)
	require.Len(t, ledgers, 1)
	require.Equal(t, "ledger-1", ledgers[0].Name)

	// Register second ledger
	registerLedger(t, s, "ledger-2")
	cursor, err = ReadLedgers(s)
	require.NoError(t, err)
	ledgers, err = collectLedgers(cursor)
	require.NoError(t, err)
	require.Len(t, ledgers, 2)
}

func TestGetLedgerByName(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	registerLedger(t, s, "my-ledger")

	ledger, err := GetLedgerByName(s, "my-ledger")
	require.NoError(t, err)
	require.NotNil(t, ledger)
	require.Equal(t, "my-ledger", ledger.Name)

	ledger, err = GetLedgerByName(s, "non-existing")
	require.Error(t, err)
	require.Nil(t, ledger)
}

func TestReadLastSequenceAfterSnapshot(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Create some data
	registerLedger(t, s, "test-ledger")
	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 0, testLogs...)

	// Create snapshot
	checkpointID, err := s.CreateSnapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), checkpointID)

	// Verify data still accessible after snapshot
	lastSequence, err := ReadLastSequence(s)
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastSequence)
}

func TestReadLastAppliedIndex(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Initial value should be 0
	lastIndex, err := ReadLastAppliedIndex(s)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastIndex)

	// Create batch with index 5
	batch := s.NewBatch()
	require.NoError(t, SaveLedger(batch, &commonpb.LedgerInfo{
		Name: "test",
	}))
	require.NoError(t, SetAppliedIndex(batch, 5))
	require.NoError(t, batch.Commit())

	// Verify last applied index updated
	lastIndex, err = ReadLastAppliedIndex(s)
	require.NoError(t, err)
	require.Equal(t, uint64(5), lastIndex)

	// Create another batch with index 10
	batch = s.NewBatch()
	require.NoError(t, SaveLedger(batch, &commonpb.LedgerInfo{
		Name: "test2",
	}))
	require.NoError(t, SetAppliedIndex(batch, 10))
	require.NoError(t, batch.Commit())

	lastIndex, err = ReadLastAppliedIndex(s)
	require.NoError(t, err)
	require.Equal(t, uint64(10), lastIndex)
}

func TestReadLedgersSoftDelete(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	attrs := attributes.New()

	const ledgerName = "test-ledger"
	createdAt := commonpb.NewTimestamp(libtime.Now())
	batch := s.NewBatch()
	err := SaveLedger(batch, &commonpb.LedgerInfo{
		Name:      ledgerName,
		CreatedAt: createdAt,
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Add some data
	batch = s.NewBatch()
	worldKey := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: ledgerName, Account: "world"}, Asset: "USD"}
	worldCanonicalKey := worldKey.Bytes()
	require.NoError(t, attrs.Volume.AddDiff(batch, 1, worldCanonicalKey, &raftcmdpb.VolumePair{
		OutputKnown: commonpb.NewUint256FromUint64(100),
	}))
	metadataKey := domain.MetadataKey{AccountKey: domain.AccountKey{Ledger: ledgerName, Account: "bank"}, Key: "key"}
	metadataCanonicalKey := metadataKey.Bytes()
	require.NoError(t, attrs.Metadata.AddDiff(batch, 1, metadataCanonicalKey, commonpb.NewStringValue("value")))
	require.NoError(t, StoreTransactionUpdate(batch, domain.TransactionKey{Ledger: ledgerName, ID: 1}, &commonpb.TransactionUpdate{
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
	cursor, err := ReadLedgers(s)
	require.NoError(t, err)
	ledgers, err := collectLedgers(cursor)
	require.NoError(t, err)
	require.Len(t, ledgers, 1)
	require.Nil(t, ledgers[0].DeletedAt)

	// Soft delete ledger
	deletedAt := commonpb.NewTimestamp(libtime.Now())
	batch = s.NewBatch()
	require.NoError(t, SaveLedger(batch, &commonpb.LedgerInfo{
		Name:      ledgerName,
		CreatedAt: createdAt,
		DeletedAt: deletedAt,
	}))
	require.NoError(t, batch.Commit())

	// Verify ledger still exists but is marked as deleted
	cursor, err = ReadLedgers(s)
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

func TestReadPeriods(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	t.Run("EmptyStore", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		periods, err := ReadAllPeriods(s)
		require.NoError(t, err)
		require.Nil(t, periods)

		nextID, err := ReadNextPeriodID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(1), nextID)
	})

	t.Run("StoreSinglePeriod", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		batch := s.NewBatch()
		require.NoError(t, StorePeriod(batch, &commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, StoreNextPeriodID(batch, 2))
		require.NoError(t, batch.Commit())

		periods, err := ReadAllPeriods(s)
		require.NoError(t, err)
		require.Len(t, periods, 1)
		require.Equal(t, uint64(1), periods[0].Id)
		require.Equal(t, uint64(1000), periods[0].Start.Data)
		require.Equal(t, commonpb.PeriodStatus_PERIOD_OPEN, periods[0].Status)

		nextID, err := ReadNextPeriodID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(2), nextID)
	})

	t.Run("StoreMultiplePeriodsOrderedByID", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		// Insert periods out of order
		batch := s.NewBatch()
		require.NoError(t, StorePeriod(batch, &commonpb.Period{
			Id:     3,
			Start:  &commonpb.Timestamp{Data: 3000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, StorePeriod(batch, &commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			End:    &commonpb.Timestamp{Data: 2000},
			Status: commonpb.PeriodStatus_PERIOD_CLOSED,
		}))
		require.NoError(t, StorePeriod(batch, &commonpb.Period{
			Id:            2,
			Start:         &commonpb.Timestamp{Data: 2000},
			End:           &commonpb.Timestamp{Data: 3000},
			Status:        commonpb.PeriodStatus_PERIOD_CLOSED,
			CloseSequence: 10,
			SealingHash:   []byte("hash-2"),
		}))
		require.NoError(t, StoreNextPeriodID(batch, 4))
		require.NoError(t, batch.Commit())

		// Verify periods are returned ordered by ID
		periods, err := ReadAllPeriods(s)
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

		nextID, err := ReadNextPeriodID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(4), nextID)
	})

	t.Run("UpdateExistingPeriod", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		// Store initial period
		batch := s.NewBatch()
		require.NoError(t, StorePeriod(batch, &commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, StoreNextPeriodID(batch, 2))
		require.NoError(t, batch.Commit())

		// Update the same period (close it)
		batch = s.NewBatch()
		require.NoError(t, StorePeriod(batch, &commonpb.Period{
			Id:            1,
			Start:         &commonpb.Timestamp{Data: 1000},
			End:           &commonpb.Timestamp{Data: 2000},
			Status:        commonpb.PeriodStatus_PERIOD_CLOSED,
			CloseSequence: 5,
			SealingHash:   []byte("sealed"),
		}))
		require.NoError(t, batch.Commit())

		periods, err := ReadAllPeriods(s)
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
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)

		batch := s.NewBatch()
		require.NoError(t, StorePeriod(batch, &commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			End:    &commonpb.Timestamp{Data: 2000},
			Status: commonpb.PeriodStatus_PERIOD_CLOSED,
		}))
		require.NoError(t, StorePeriod(batch, &commonpb.Period{
			Id:     2,
			Start:  &commonpb.Timestamp{Data: 2000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, StoreNextPeriodID(batch, 3))
		require.NoError(t, batch.Commit())

		// Create snapshot so data survives reopen (writes use NoSync)
		_, err = s.CreateSnapshot()
		require.NoError(t, err)
		require.NoError(t, s.Close())

		// Reopen and verify
		s2, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s2.Close() })

		periods, err := ReadAllPeriods(s2)
		require.NoError(t, err)
		require.Len(t, periods, 2)
		require.Equal(t, uint64(1), periods[0].Id)
		require.Equal(t, uint64(2), periods[1].Id)

		nextID, err := ReadNextPeriodID(s2)
		require.NoError(t, err)
		require.Equal(t, uint64(3), nextID)
	})

	t.Run("NextPeriodIDUpdate", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		// Set to 5
		batch := s.NewBatch()
		require.NoError(t, StoreNextPeriodID(batch, 5))
		require.NoError(t, batch.Commit())

		nextID, err := ReadNextPeriodID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(5), nextID)

		// Update to 10
		batch = s.NewBatch()
		require.NoError(t, StoreNextPeriodID(batch, 10))
		require.NoError(t, batch.Commit())

		nextID, err = ReadNextPeriodID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(10), nextID)
	})

	t.Run("AtomicBatchWithPeriodsAndLogs", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		registerLedger(t, s, "test-ledger")

		// Store periods, nextPeriodID, and logs in the same batch
		batch := s.NewBatch()
		require.NoError(t, StorePeriod(batch, &commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, StoreNextPeriodID(batch, 2))
		require.NoError(t, SetAppliedIndex(batch, 42))
		require.NoError(t, batch.Commit())

		// Verify all data was written atomically
		periods, err := ReadAllPeriods(s)
		require.NoError(t, err)
		require.Len(t, periods, 1)

		nextID, err := ReadNextPeriodID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(2), nextID)

		lastIndex, err := ReadLastAppliedIndex(s)
		require.NoError(t, err)
		require.Equal(t, uint64(42), lastIndex)
	})
}

func TestReadSigningKeys(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	t.Run("empty store has no signing keys", func(t *testing.T) {
		keys, err := ReadSigningKeys(s)
		require.NoError(t, err)
		require.Empty(t, keys)

		requireSig, err := ReadSigningConfig(s)
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
		require.NoError(t, SaveSigningKey(batch, "key-1", pubKey1, ""))
		require.NoError(t, SaveSigningKey(batch, "key-2", pubKey2, ""))
		require.NoError(t, batch.Commit())

		keys, err := ReadSigningKeys(s)
		require.NoError(t, err)
		require.Len(t, keys, 2)
		require.Equal(t, pubKey1, keys["key-1"].PublicKey)
		require.Equal(t, pubKey2, keys["key-2"].PublicKey)
	})

	t.Run("delete signing key", func(t *testing.T) {
		batch := s.NewBatch()
		require.NoError(t, DeleteSigningKey(batch, "key-1"))
		require.NoError(t, batch.Commit())

		keys, err := ReadSigningKeys(s)
		require.NoError(t, err)
		require.Len(t, keys, 1)
		_, hasKey1 := keys["key-1"]
		require.False(t, hasKey1)
		_, hasKey2 := keys["key-2"]
		require.True(t, hasKey2)
	})

	t.Run("save and load signing config", func(t *testing.T) {
		batch := s.NewBatch()
		require.NoError(t, SaveSigningConfig(batch, true))
		require.NoError(t, batch.Commit())

		requireSig, err := ReadSigningConfig(s)
		require.NoError(t, err)
		require.True(t, requireSig)

		batch = s.NewBatch()
		require.NoError(t, SaveSigningConfig(batch, false))
		require.NoError(t, batch.Commit())

		requireSig, err = ReadSigningConfig(s)
		require.NoError(t, err)
		require.False(t, requireSig)
	})

	t.Run("delete all signing keys", func(t *testing.T) {
		// Add some keys first
		batch := s.NewBatch()
		require.NoError(t, SaveSigningKey(batch, "a", make([]byte, 32), ""))
		require.NoError(t, SaveSigningKey(batch, "b", make([]byte, 32), ""))
		require.NoError(t, SaveSigningKey(batch, "c", make([]byte, 32), ""))
		require.NoError(t, batch.Commit())

		keys, err := ReadSigningKeys(s)
		require.NoError(t, err)
		require.Len(t, keys, 4) // key-2 from previous test + a, b, c

		batch = s.NewBatch()
		require.NoError(t, DeleteAllSigningKeys(batch))
		require.NoError(t, batch.Commit())

		keys, err = ReadSigningKeys(s)
		require.NoError(t, err)
		require.Empty(t, keys)
	})
}

func TestReadLogsSince(t *testing.T) {
	t.Parallel()

	t.Run("EmptyStore", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		cursor, err := ReadLogsSince(s, 0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Empty(t, logs)
	})

	t.Run("AllLogs", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		// afterSequence=0 should return all logs
		cursor, err := ReadLogsSince(s, 0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 4)
		require.Equal(t, uint64(1), logs[0].Sequence)
		require.Equal(t, uint64(4), logs[3].Sequence)
	})

	t.Run("LogsAfterSequence", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		// afterSequence=2 should return logs 3 and 4
		cursor, err := ReadLogsSince(s, 2)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 2)
		require.Equal(t, uint64(3), logs[0].Sequence)
		require.Equal(t, uint64(4), logs[1].Sequence)
	})

	t.Run("LogsAfterLastSequence", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		// afterSequence=4 (last log) should return empty
		cursor, err := ReadLogsSince(s, 4)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Empty(t, logs)
	})

	t.Run("LogsAfterFarFutureSequence", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		cursor, err := ReadLogsSince(s, 999)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Empty(t, logs)
	})

	t.Run("IncrementalRead", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		// Simulate emitter: read all, then read after cursor
		cursor, err := ReadLogsSince(s, 0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 4)

		lastSeq := logs[len(logs)-1].Sequence

		// Append more logs
		moreLogs := createTestLogsForLedger("test-ledger", 5)
		appendLogs(t, s, 2, moreLogs...)

		// Read only new logs
		cursor, err = ReadLogsSince(s, lastSeq)
		require.NoError(t, err)
		newLogs := collectLogs(t, cursor)
		require.Len(t, newLogs, 4) // 4 new logs starting from sequence 5
		require.Equal(t, uint64(5), newLogs[0].Sequence)
	})

	t.Run("LogPayloadTypes", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		now := libtime.Now()
		registerLedger(t, s, "test-ledger")

		// Create logs with different payload types
		mixedLogs := []*commonpb.Log{
			{
				Sequence: 1,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_CreateLedger{
						CreateLedger: &commonpb.CreateLedgerLog{
							Info: &commonpb.LedgerInfo{
								Name:      "new-ledger",
								CreatedAt: commonpb.NewTimestamp(now),
							},
						},
					},
				},
			},
			{
				Sequence: 2,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							LedgerName: "test-ledger",
							Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
								Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
									CreatedTransaction: &commonpb.CreatedTransaction{
										Transaction: commonpb.NewTransaction().
											WithPostings(
												commonpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
											).
											WithID(1).
											WithTimestamp(now),
									},
								},
							}).WithID(1).WithDate(now),
						},
					},
				},
			},
			{
				Sequence: 3,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_DeleteLedger{
						DeleteLedger: &commonpb.DeleteLedgerLog{
							Info: &commonpb.LedgerInfo{
								Name:      "new-ledger",
								DeletedAt: commonpb.NewTimestamp(now),
							},
						},
					},
				},
			},
		}
		appendLogs(t, s, 1, mixedLogs...)

		cursor, err := ReadLogsSince(s, 0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 3)

		// Verify payload types are preserved
		require.NotNil(t, logs[0].Payload.GetCreateLedger())
		require.NotNil(t, logs[1].Payload.GetApply())
		require.NotNil(t, logs[2].Payload.GetDeleteLedger())
	})
}
