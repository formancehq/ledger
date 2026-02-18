package admission

import (
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

const (
	testLedgerName = "test-ledger"
	testLedgerID   = uint32(1)
)

// createTestStore creates a test store with a registered ledger
func createTestStore(t *testing.T) *data.Store {
	t.Helper()
	tmpDir := t.TempDir()
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Register test ledger
	batch := s.NewBatch()
	err = batch.SaveLedger(&commonpb.LedgerInfo{
		Id:        testLedgerID,
		Name:      testLedgerName,
		CreatedAt: commonpb.NewTimestamp(time.Now()),
	})
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)

	return s
}

// createTransactionLog creates a log with a CreatedTransaction payload
func createTransactionLog(sequence uint64, ledgerName string, logID uint64, txID uint64, postings []*commonpb.Posting) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledgerName,
					Log: &commonpb.LedgerLog{
						Id:   logID,
						Date: commonpb.NewTimestamp(time.Now()),
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: &commonpb.Transaction{
										Id:         txID,
										Postings:   postings,
										Timestamp:  commonpb.NewTimestamp(time.Now()),
										InsertedAt: commonpb.NewTimestamp(time.Now()),
										UpdatedAt:  commonpb.NewTimestamp(time.Now()),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// createTestAdmission creates an Admission instance for testing
func createTestAdmission(t *testing.T, store *data.Store) *Admission {
	t.Helper()
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	testCache, _ := cache.New(100, nil)

	// Create noop metrics
	commandDuration, _ := meter.Int64Histogram("test_command_duration")
	proposeQueueLoad, _ := meter.Int64Histogram("test_propose_queue_load")
	proposeQueueFull, _ := meter.Float64Counter("test_propose_queue_full")
	preloadDuration, _ := meter.Int64Histogram("test_preload_duration")
	preloadCounter, _ := meter.Int64Counter("test_preload_counter")

	a := &Admission{
		cache:                     testCache,
		store:                     store,
		logger:                    logger,
		loaders:                   NewLoaders(),
		commandDurationHistogram:  commandDuration,
		proposeQueueLoadHistogram: proposeQueueLoad,
		proposeQueueFullCounter:   proposeQueueFull,
		preloadDurationHistogram:  preloadDuration,
		preloadCounter:            preloadCounter,
	}
	a.nextIndex.Store(1)
	return a
}

func TestGetTransactionPostings(t *testing.T) {
	t.Parallel()

	t.Run("returns postings for existing transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		// Create test postings
		expectedPostings := []*commonpb.Posting{
			{
				Source:      "world",
				Destination: "user:alice",
				Amount:      commonpb.NewUint256FromUint64(100),
				Asset:       "USD",
			},
			{
				Source:      "world",
				Destination: "user:bob",
				Amount:      commonpb.NewUint256FromUint64(50),
				Asset:       "EUR",
			},
		}

		// Create and store a transaction log
		txLog := createTransactionLog(1, testLedgerName, 1, 1, expectedPostings)

		batch := store.NewBatch()
		err := batch.AppendLogs(txLog)
		require.NoError(t, err)

		// Add TransactionUpdate with TransactionInit to link transaction ID to log sequence
		require.NoError(t, batch.StoreTransactionUpdate(data.TransactionKey{LedgerID: testLedgerID, ID: 1}, &commonpb.TransactionUpdate{
			ByLog: 1,
			Updates: []*commonpb.TransactionUpdateType{
				{
					TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
						TransactionInit: &commonpb.TransactionInit{},
					},
				},
			},
		}))
		require.NoError(t, batch.SetAppliedIndex(1))
		require.NoError(t, batch.Commit())

		// Test getTransactionPostings
		postings, err := admission.getTransactionPostings(testLedgerName, 1)
		require.NoError(t, err)
		require.Len(t, postings, 2)
		require.Equal(t, expectedPostings[0].Source, postings[0].Source)
		require.Equal(t, expectedPostings[0].Destination, postings[0].Destination)
		require.Equal(t, expectedPostings[0].Asset, postings[0].Asset)
		require.Equal(t, expectedPostings[1].Source, postings[1].Source)
		require.Equal(t, expectedPostings[1].Destination, postings[1].Destination)
		require.Equal(t, expectedPostings[1].Asset, postings[1].Asset)
	})

	t.Run("returns error for non-existent transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		// Try to get postings for a transaction that doesn't exist
		_, err := admission.getTransactionPostings(testLedgerName, 999)
		require.Error(t, err)
		require.Contains(t, err.Error(), "transaction 999 does not exist")
	})
}

func TestExtractNeededVolumes(t *testing.T) {
	t.Parallel()

	t.Run("extracts volumes for create transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Postings: []*commonpb.Posting{
									{
										Source:      "world",
										Destination: "user:alice",
										Amount:      commonpb.NewUint256FromUint64(100),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Should have 2 volume keys: source (world) and destination (user:alice)
		require.Len(t, volumes, 2)

		worldKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "world"},
			Asset:      "USD",
		}
		aliceKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "user:alice"},
			Asset:      "USD",
		}

		_, hasWorld := volumes[worldKey]
		_, hasAlice := volumes[aliceKey]
		require.True(t, hasWorld, "should have world volume key")
		require.True(t, hasAlice, "should have alice volume key")
	})

	t.Run("extracts volumes for revert transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		// For a revert, original postings are reversed:
		// Original: world -> alice
		// Revert: alice -> world (alice needs balance check, world receives credit)
		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
							RevertTransaction: &raftcmdpb.RevertTransactionOrder{
								TransactionId: 1,
								OriginalPostings: []*commonpb.Posting{
									{
										Source:      "world",
										Destination: "user:alice",
										Amount:      commonpb.NewUint256FromUint64(100),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Should have 2 volume keys: original destination (alice, now source) and original source (world, now destination)
		require.Len(t, volumes, 2)

		worldKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "world"},
			Asset:      "USD",
		}
		aliceKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "user:alice"},
			Asset:      "USD",
		}

		_, hasWorld := volumes[worldKey]
		_, hasAlice := volumes[aliceKey]
		require.True(t, hasWorld, "should have world volume key (destination in revert)")
		require.True(t, hasAlice, "should have alice volume key (source in revert)")
	})

	t.Run("extracts volumes for multiple postings in revert", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
							RevertTransaction: &raftcmdpb.RevertTransactionOrder{
								TransactionId: 1,
								OriginalPostings: []*commonpb.Posting{
									{
										Source:      "world",
										Destination: "user:alice",
										Amount:      commonpb.NewUint256FromUint64(100),
										Asset:       "USD",
									},
									{
										Source:      "user:alice",
										Destination: "user:bob",
										Amount:      commonpb.NewUint256FromUint64(50),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Should have 3 volume keys: world, alice, bob (all in USD)
		require.Len(t, volumes, 3)

		worldKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "world"},
			Asset:      "USD",
		}
		aliceKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "user:alice"},
			Asset:      "USD",
		}
		bobKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "user:bob"},
			Asset:      "USD",
		}

		_, hasWorld := volumes[worldKey]
		_, hasAlice := volumes[aliceKey]
		_, hasBob := volumes[bobKey]
		require.True(t, hasWorld)
		require.True(t, hasAlice)
		require.True(t, hasBob)
	})
}

func TestExtractNeededTransactions(t *testing.T) {
	t.Parallel()

	t.Run("extracts transaction for revert operation", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
							RevertTransaction: &raftcmdpb.RevertTransactionOrder{
								TransactionId: 42,
								OriginalPostings: []*commonpb.Posting{
									{
										Source:      "world",
										Destination: "user:alice",
										Amount:      commonpb.NewUint256FromUint64(100),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		transactions := admission.extractNeededTransactions(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Should have 1 transaction key
		require.Len(t, transactions, 1)

		txKey := data.TransactionKey{
			LedgerID: testLedgerID,
			ID:       42,
		}
		_, hasTx := transactions[txKey]
		require.True(t, hasTx, "should have transaction key for revert")
	})

	t.Run("does not extract transaction for create operation", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Postings: []*commonpb.Posting{
									{
										Source:      "world",
										Destination: "user:alice",
										Amount:      commonpb.NewUint256FromUint64(100),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		transactions := admission.extractNeededTransactions(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Should be empty - create transactions don't need revert status check
		require.Len(t, transactions, 0)
	})

	t.Run("extracts multiple transactions for multiple reverts", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
							RevertTransaction: &raftcmdpb.RevertTransactionOrder{
								TransactionId:    1,
								OriginalPostings: []*commonpb.Posting{},
							},
						},
					},
				},
			},
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
							RevertTransaction: &raftcmdpb.RevertTransactionOrder{
								TransactionId:    2,
								OriginalPostings: []*commonpb.Posting{},
							},
						},
					},
				},
			},
		}

		transactions := admission.extractNeededTransactions(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Should have 2 transaction keys
		require.Len(t, transactions, 2)

		txKey1 := data.TransactionKey{LedgerID: testLedgerID, ID: 1}
		txKey2 := data.TransactionKey{LedgerID: testLedgerID, ID: 2}
		_, hasTx1 := transactions[txKey1]
		_, hasTx2 := transactions[txKey2]
		require.True(t, hasTx1)
		require.True(t, hasTx2)
	})
}

func TestConvertApplyRequest_RevertTransaction(t *testing.T) {
	t.Parallel()

	t.Run("fetches original postings for revert transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		// First, create a transaction to revert
		expectedPostings := []*commonpb.Posting{
			{
				Source:      "world",
				Destination: "user:alice",
				Amount:      commonpb.NewUint256FromUint64(100),
				Asset:       "USD",
			},
		}

		txLog := createTransactionLog(1, testLedgerName, 1, 1, expectedPostings)

		batch := store.NewBatch()
		err := batch.AppendLogs(txLog)
		require.NoError(t, err)
		require.NoError(t, batch.StoreTransactionUpdate(data.TransactionKey{LedgerID: testLedgerID, ID: 1}, &commonpb.TransactionUpdate{
			ByLog: 1,
			Updates: []*commonpb.TransactionUpdateType{
				{
					TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
						TransactionInit: &commonpb.TransactionInit{},
					},
				},
			},
		}))
		require.NoError(t, batch.SetAppliedIndex(1))
		require.NoError(t, batch.Commit())

		// Now convert a revert request
		applyRequest := &servicepb.LedgerApplyRequest{
			Ledger: testLedgerName,
			Data: &servicepb.LedgerApplyRequest_RevertTransaction{
				RevertTransaction: &servicepb.RevertTransactionPayload{
					TransactionId:   1,
					Force:           false,
					AtEffectiveDate: true,
				},
			},
		}

		order, err := admission.convertApplyRequest(applyRequest)
		require.NoError(t, err)
		require.NotNil(t, order)

		// Verify the order contains the original postings
		revertOrder := order.Data.(*raftcmdpb.LedgerApplyOrder_RevertTransaction).RevertTransaction
		require.NotNil(t, revertOrder)
		require.Equal(t, uint64(1), revertOrder.TransactionId)
		require.False(t, revertOrder.Force)
		require.True(t, revertOrder.AtEffectiveDate)
		require.Len(t, revertOrder.OriginalPostings, 1)
		require.Equal(t, "world", revertOrder.OriginalPostings[0].Source)
		require.Equal(t, "user:alice", revertOrder.OriginalPostings[0].Destination)
		require.Equal(t, "USD", revertOrder.OriginalPostings[0].Asset)
	})

	t.Run("returns error when transaction to revert does not exist", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		applyRequest := &servicepb.LedgerApplyRequest{
			Ledger: testLedgerName,
			Data: &servicepb.LedgerApplyRequest_RevertTransaction{
				RevertTransaction: &servicepb.RevertTransactionPayload{
					TransactionId: 999,
				},
			},
		}

		_, err := admission.convertApplyRequest(applyRequest)
		require.Error(t, err)
		require.Contains(t, err.Error(), "getting original transaction postings")
	})
}

func TestExtractNeededVolumes_Force(t *testing.T) {
	t.Parallel()

	t.Run("skips volume extraction when force is true for create transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Force: true, // Force flag skips volume extraction (deltas only)
								Postings: []*commonpb.Posting{
									{
										Source:      "users:alice",
										Destination: "users:bob",
										Amount:      commonpb.NewUint256FromUint64(100),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Should have 0 volume keys with force=true - processor stores deltas only
		require.Len(t, volumes, 0, "force=true should skip volume extraction")
	})

	t.Run("extracts volumes when force is false for create transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Force: false, // Default behavior
								Postings: []*commonpb.Posting{
									{
										Source:      "users:alice",
										Destination: "users:bob",
										Amount:      commonpb.NewUint256FromUint64(100),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Should have 2 volume keys: source and destination
		require.Len(t, volumes, 2, "force=false should extract volumes normally")

		aliceKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "users:alice"},
			Asset:      "USD",
		}
		bobKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "users:bob"},
			Asset:      "USD",
		}

		_, hasAlice := volumes[aliceKey]
		_, hasBob := volumes[bobKey]
		require.True(t, hasAlice)
		require.True(t, hasBob)
	})

	t.Run("mixed orders: force=true skipped, force=false extracted", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			// First order with force=true - should skip volume extraction
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Force: true,
								Postings: []*commonpb.Posting{
									{
										Source:      "users:force_source",
										Destination: "users:force_dest",
										Amount:      commonpb.NewUint256FromUint64(100),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
			// Second order with force=false - should extract volumes
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Force: false,
								Postings: []*commonpb.Posting{
									{
										Source:      "users:normal_source",
										Destination: "users:normal_dest",
										Amount:      commonpb.NewUint256FromUint64(200),
										Asset:       "EUR",
									},
								},
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Should have volumes only from force=false order (2 volume keys)
		require.Len(t, volumes, 2)

		// Verify force=true volumes are NOT present
		forceSourceKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "users:force_source"},
			Asset:      "USD",
		}
		_, hasForceSource := volumes[forceSourceKey]
		require.False(t, hasForceSource, "force=true order should NOT have volumes extracted")

		// Verify force=false volumes are present
		normalSourceKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "users:normal_source"},
			Asset:      "EUR",
		}
		_, hasNormalSource := volumes[normalSourceKey]
		require.True(t, hasNormalSource, "force=false order should have volumes extracted")
	})

	t.Run("force on revert still extracts volumes", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		// Revert transactions still need volume preloading even with force=true
		// because we need to track the reverted amounts correctly
		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
							RevertTransaction: &raftcmdpb.RevertTransactionOrder{
								TransactionId: 1,
								Force:         true, // Force on revert only affects balance check
								OriginalPostings: []*commonpb.Posting{
									{
										Source:      "world",
										Destination: "user:alice",
										Amount:      commonpb.NewUint256FromUint64(100),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Should still have 2 volume keys for revert (even with force=true)
		// because volume preloading is needed for correct accounting
		require.Len(t, volumes, 2, "revert with force=true should still extract volumes")
	})
}

func TestConvertApplyRequest_CreateTransaction_Force(t *testing.T) {
	t.Parallel()

	t.Run("propagates force flag to order", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		applyRequest := &servicepb.LedgerApplyRequest{
			Ledger: testLedgerName,
			Data: &servicepb.LedgerApplyRequest_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{
					Force: true,
					Postings: []*commonpb.Posting{
						{
							Source:      "users:alice",
							Destination: "users:bob",
							Amount:      commonpb.NewUint256FromUint64(100),
							Asset:       "USD",
						},
					},
				},
			},
		}

		order, err := admission.convertApplyRequest(applyRequest)
		require.NoError(t, err)
		require.NotNil(t, order)

		createOrder := order.Data.(*raftcmdpb.LedgerApplyOrder_CreateTransaction).CreateTransaction
		require.True(t, createOrder.Force, "force flag should be propagated to order")
	})
}

func TestRequestToOrder_RevertTransaction(t *testing.T) {
	t.Parallel()

	t.Run("converts revert request with original postings", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		// Setup transaction to revert
		expectedPostings := []*commonpb.Posting{
			{
				Source:      "bank",
				Destination: "user:charlie",
				Amount:      commonpb.NewUint256FromUint64(500),
				Asset:       "EUR",
			},
		}

		txLog := createTransactionLog(1, testLedgerName, 1, 42, expectedPostings)

		batch := store.NewBatch()
		err := batch.AppendLogs(txLog)
		require.NoError(t, err)
		require.NoError(t, batch.StoreTransactionUpdate(data.TransactionKey{LedgerID: testLedgerID, ID: 42}, &commonpb.TransactionUpdate{
			ByLog: 1,
			Updates: []*commonpb.TransactionUpdateType{
				{
					TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
						TransactionInit: &commonpb.TransactionInit{},
					},
				},
			},
		}))
		require.NoError(t, batch.SetAppliedIndex(1))
		require.NoError(t, batch.Commit())

		// Create revert request
		request := &servicepb.Request{
			IdempotencyKey: "revert-tx-42",
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: testLedgerName,
					Data: &servicepb.LedgerApplyRequest_RevertTransaction{
						RevertTransaction: &servicepb.RevertTransactionPayload{
							TransactionId: 42,
							Force:         true,
						},
					},
				},
			},
		}

		order, err := admission.requestToOrder(request)
		require.NoError(t, err)
		require.NotNil(t, order)
		require.NotNil(t, order.Idempotency)
		require.Equal(t, "revert-tx-42", order.Idempotency.Key)

		applyOrder := order.Type.(*raftcmdpb.Order_Apply).Apply
		require.Equal(t, testLedgerName, applyOrder.Ledger)

		revertOrder := applyOrder.Data.(*raftcmdpb.LedgerApplyOrder_RevertTransaction).RevertTransaction
		require.Equal(t, uint64(42), revertOrder.TransactionId)
		require.True(t, revertOrder.Force)
		require.Len(t, revertOrder.OriginalPostings, 1)
		require.Equal(t, "bank", revertOrder.OriginalPostings[0].Source)
		require.Equal(t, "user:charlie", revertOrder.OriginalPostings[0].Destination)
		require.Equal(t, "EUR", revertOrder.OriginalPostings[0].Asset)
	})
}

func TestExtractNeededVolumes_Numscript(t *testing.T) {
	t.Parallel()

	t.Run("discovers volumes from numscript script", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Script: &commonpb.Script{
									Plain: `
										send [USD/2 1000] (
											source = @users:alice
											destination = @users:bob
										)
									`,
								},
								// No explicit postings — Numscript emulation should discover them
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Numscript emulation should discover both source and destination
		require.NotEmpty(t, volumes, "numscript emulation should discover volumes")

		aliceKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "users:alice"},
			Asset:      "USD/2",
		}
		bobKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "users:bob"},
			Asset:      "USD/2",
		}

		_, hasAlice := volumes[aliceKey]
		_, hasBob := volumes[bobKey]
		require.True(t, hasAlice, "should discover source account from numscript")
		require.True(t, hasBob, "should discover destination account from numscript")
	})

	t.Run("skips numscript emulation when force is true", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Force: true,
								Script: &commonpb.Script{
									Plain: `
										send [USD/2 1000] (
											source = @users:alice
											destination = @users:bob
										)
									`,
								},
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Force=true should skip volume extraction entirely
		require.Empty(t, volumes, "force=true should skip numscript emulation")
	})

	t.Run("falls back to postings when script has explicit postings", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission := createTestAdmission(t, store)

		// When both Script and Postings are present, explicit Postings take precedence
		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Script: &commonpb.Script{
									Plain: `send [USD/2 1000] ( source = @world destination = @treasury )`,
								},
								Postings: []*commonpb.Posting{
									{
										Source:      "bank",
										Destination: "merchant",
										Amount:      commonpb.NewUint256FromUint64(100),
										Asset:       "EUR",
									},
								},
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders, map[string]uint32{testLedgerName: testLedgerID})

		// Should use explicit postings, not numscript emulation
		require.Len(t, volumes, 2)

		bankKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "bank"},
			Asset:      "EUR",
		}
		merchantKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: testLedgerID, Account: "merchant"},
			Asset:      "EUR",
		}

		_, hasBank := volumes[bankKey]
		_, hasMerchant := volumes[merchantKey]
		require.True(t, hasBank, "should use explicit posting source")
		require.True(t, hasMerchant, "should use explicit posting destination")
	})
}
