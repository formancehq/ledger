package admission

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/keystore"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

const testLedgerName = "test-ledger"

// createTestStore creates a test store with a registered ledger
func createTestStore(t *testing.T) *dal.Store {
	t.Helper()
	tmpDir := t.TempDir()
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Register test ledger
	batch := s.NewBatch()
	err = state.SaveLedger(batch, &commonpb.LedgerInfo{
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
func createTestAdmission(t *testing.T, store *dal.Store) *Admission {
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

	ks := keystore.NewKeyStore()
	ss := state.NewSharedState()

	a := &Admission{
		cache:                     testCache,
		store:                     store,
		logger:                    logger,
		keyStore:                  ks,
		sharedState:               ss,
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
		err := state.AppendLogs(batch, txLog)
		require.NoError(t, err)

		// Add TransactionUpdate with TransactionInit to link transaction ID to log sequence
		require.NoError(t, state.StoreTransactionUpdate(batch, domain.TransactionKey{Ledger: testLedgerName, ID: 1}, &commonpb.TransactionUpdate{
			ByLog: 1,
			Updates: []*commonpb.TransactionUpdateType{
				{
					TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
						TransactionInit: &commonpb.TransactionInit{},
					},
				},
			},
		}))
		require.NoError(t, state.SetAppliedIndex(batch, 1))
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

		volumes := admission.extractPreloadNeeds(orders).Volumes

		// Should have 2 volume keys: source (world) and destination (user:alice)
		require.Len(t, volumes, 2)

		worldKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "world"},
			Asset:      "USD",
		}
		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "user:alice"},
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

		volumes := admission.extractPreloadNeeds(orders).Volumes

		// Should have 2 volume keys: original destination (alice, now source) and original source (world, now destination)
		require.Len(t, volumes, 2)

		worldKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "world"},
			Asset:      "USD",
		}
		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "user:alice"},
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

		volumes := admission.extractPreloadNeeds(orders).Volumes

		// Should have 3 volume keys: world, alice, bob (all in USD)
		require.Len(t, volumes, 3)

		worldKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "world"},
			Asset:      "USD",
		}
		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "user:alice"},
			Asset:      "USD",
		}
		bobKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "user:bob"},
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

		transactions := admission.extractPreloadNeeds(orders).Transactions

		// Should have 1 transaction key
		require.Len(t, transactions, 1)

		txKey := domain.TransactionKey{
			Ledger: testLedgerName,
			ID:     42,
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

		transactions := admission.extractPreloadNeeds(orders).Transactions

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

		transactions := admission.extractPreloadNeeds(orders).Transactions

		// Should have 2 transaction keys
		require.Len(t, transactions, 2)

		txKey1 := domain.TransactionKey{Ledger: testLedgerName, ID: 1}
		txKey2 := domain.TransactionKey{Ledger: testLedgerName, ID: 2}
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
		err := state.AppendLogs(batch, txLog)
		require.NoError(t, err)
		require.NoError(t, state.StoreTransactionUpdate(batch, domain.TransactionKey{Ledger: testLedgerName, ID: 1}, &commonpb.TransactionUpdate{
			ByLog: 1,
			Updates: []*commonpb.TransactionUpdateType{
				{
					TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
						TransactionInit: &commonpb.TransactionInit{},
					},
				},
			},
		}))
		require.NoError(t, state.SetAppliedIndex(batch, 1))
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

		volumes := admission.extractPreloadNeeds(orders).Volumes

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

		volumes := admission.extractPreloadNeeds(orders).Volumes

		// Should have 2 volume keys: source and destination
		require.Len(t, volumes, 2, "force=false should extract volumes normally")

		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "users:alice"},
			Asset:      "USD",
		}
		bobKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "users:bob"},
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

		volumes := admission.extractPreloadNeeds(orders).Volumes

		// Should have volumes only from force=false order (2 volume keys)
		require.Len(t, volumes, 2)

		// Verify force=true volumes are NOT present
		forceSourceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "users:force_source"},
			Asset:      "USD",
		}
		_, hasForceSource := volumes[forceSourceKey]
		require.False(t, hasForceSource, "force=true order should NOT have volumes extracted")

		// Verify force=false volumes are present
		normalSourceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "users:normal_source"},
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

		volumes := admission.extractPreloadNeeds(orders).Volumes

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
		err := state.AppendLogs(batch, txLog)
		require.NoError(t, err)
		require.NoError(t, state.StoreTransactionUpdate(batch, domain.TransactionKey{Ledger: testLedgerName, ID: 42}, &commonpb.TransactionUpdate{
			ByLog: 1,
			Updates: []*commonpb.TransactionUpdateType{
				{
					TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
						TransactionInit: &commonpb.TransactionInit{},
					},
				},
			},
		}))
		require.NoError(t, state.SetAppliedIndex(batch, 1))
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

		volumes := admission.extractPreloadNeeds(orders).Volumes

		// Numscript emulation should discover both source and destination
		require.NotEmpty(t, volumes, "numscript emulation should discover volumes")

		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "users:alice"},
			Asset:      "USD/2",
		}
		bobKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "users:bob"},
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

		volumes := admission.extractPreloadNeeds(orders).Volumes

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

		volumes := admission.extractPreloadNeeds(orders).Volumes

		// Should use explicit postings, not numscript emulation
		require.Len(t, volumes, 2)

		bankKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "bank"},
			Asset:      "EUR",
		}
		merchantKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "merchant"},
			Asset:      "EUR",
		}

		_, hasBank := volumes[bankKey]
		_, hasMerchant := volumes[merchantKey]
		require.True(t, hasBank, "should use explicit posting source")
		require.True(t, hasMerchant, "should use explicit posting destination")
	})
}

func TestRequestToOrder_IdempotencyKeyValidation(t *testing.T) {
	t.Parallel()

	t.Run("accepts idempotency key within max length", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		req := &servicepb.Request{
			IdempotencyKey: "valid-key-123",
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name: "test",
				},
			},
		}

		order, err := adm.requestToOrder(req)
		require.NoError(t, err)
		require.NotNil(t, order)
		require.NotNil(t, order.Idempotency)
		require.Equal(t, "valid-key-123", order.Idempotency.Key)
	})

	t.Run("accepts idempotency key at exactly max length", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		// 256 characters exactly
		key := strings.Repeat("a", 256)

		req := &servicepb.Request{
			IdempotencyKey: key,
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name: "test",
				},
			},
		}

		order, err := adm.requestToOrder(req)
		require.NoError(t, err)
		require.NotNil(t, order)
		require.NotNil(t, order.Idempotency)
		require.Equal(t, key, order.Idempotency.Key)
	})

	t.Run("rejects idempotency key exceeding max length", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		// 257 characters - one over the limit
		key := strings.Repeat("a", 257)

		req := &servicepb.Request{
			IdempotencyKey: key,
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name: "test",
				},
			},
		}

		_, err := adm.requestToOrder(req)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrIdempotencyKeyTooLong)
	})

	t.Run("no idempotency key is accepted", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		req := &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name: "test",
				},
			},
		}

		order, err := adm.requestToOrder(req)
		require.NoError(t, err)
		require.NotNil(t, order)
		require.Nil(t, order.Idempotency)
	})
}

// generateTestKeyPair generates an Ed25519 key pair for testing.
func generateTestKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pubKey, privKey
}

func TestVerifyAndResolveSignatures(t *testing.T) {
	t.Parallel()

	t.Run("bootstrap: unsigned RegisterSigningKey allowed when no keys exist", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		pubKey, _ := generateTestKeyPair(t)

		requests := []*servicepb.Request{
			{
				Type: &servicepb.Request_RegisterSigningKey{
					RegisterSigningKey: &servicepb.RegisterSigningKeyRequest{
						KeyId:     "first-key",
						PublicKey: []byte(pubKey),
					},
				},
			},
		}

		result, err := adm.verifyAndResolveSignatures(requests)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Nil(t, result[0].Signature, "bootstrap request should have no signature")
	})

	t.Run("unsigned RegisterSigningKey rejected when keys exist", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		existingPubKey, _ := generateTestKeyPair(t)
		adm.keyStore.AddPublicKey("existing", existingPubKey, "")

		newPubKey, _ := generateTestKeyPair(t)

		requests := []*servicepb.Request{
			{
				Type: &servicepb.Request_RegisterSigningKey{
					RegisterSigningKey: &servicepb.RegisterSigningKeyRequest{
						KeyId:     "new-key",
						PublicKey: []byte(newPubKey),
					},
				},
			},
		}

		_, err := adm.verifyAndResolveSignatures(requests)
		require.ErrorIs(t, err, signing.ErrMissingSignature)
	})

	t.Run("unsigned RevokeSigningKey rejected when keys exist", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		pubKey, _ := generateTestKeyPair(t)
		adm.keyStore.AddPublicKey("my-key", pubKey, "")

		requests := []*servicepb.Request{
			{
				Type: &servicepb.Request_RevokeSigningKey{
					RevokeSigningKey: &servicepb.RevokeSigningKeyRequest{
						KeyId: "my-key",
					},
				},
			},
		}

		_, err := adm.verifyAndResolveSignatures(requests)
		require.ErrorIs(t, err, signing.ErrMissingSignature)
	})

	t.Run("unsigned SetSigningConfig rejected when keys exist", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		pubKey, _ := generateTestKeyPair(t)
		adm.keyStore.AddPublicKey("my-key", pubKey, "")

		requests := []*servicepb.Request{
			{
				Type: &servicepb.Request_SetSigningConfig{
					SetSigningConfig: &servicepb.SetSigningConfigRequest{
						RequireSignatures: true,
					},
				},
			},
		}

		_, err := adm.verifyAndResolveSignatures(requests)
		require.ErrorIs(t, err, signing.ErrMissingSignature)
	})

	t.Run("signed RegisterSigningKey works when keys exist", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		existingPubKey, existingPrivKey := generateTestKeyPair(t)
		adm.keyStore.AddPublicKey("existing", existingPubKey, "")

		newPubKey, _ := generateTestKeyPair(t)

		req := &servicepb.Request{
			Type: &servicepb.Request_RegisterSigningKey{
				RegisterSigningKey: &servicepb.RegisterSigningKeyRequest{
					KeyId:     "new-key",
					PublicKey: []byte(newPubKey),
				},
			},
		}

		err := signing.Sign(req, "existing", existingPrivKey)
		require.NoError(t, err)

		result, err := adm.verifyAndResolveSignatures([]*servicepb.Request{req})
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.NotNil(t, result[0].Signature, "signature should be preserved for propagation")
	})

	t.Run("unsigned regular request allowed when requireSignatures is false", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		requests := []*servicepb.Request{
			{
				Type: &servicepb.Request_CreateLedger{
					CreateLedger: &servicepb.CreateLedgerRequest{
						Name: "test-ledger",
					},
				},
			},
		}

		result, err := adm.verifyAndResolveSignatures(requests)
		require.NoError(t, err)
		require.Len(t, result, 1)
	})

	t.Run("unsigned regular request rejected when requireSignatures is true", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		adm.sharedState.SetRequireSignatures(true)

		requests := []*servicepb.Request{
			{
				Type: &servicepb.Request_CreateLedger{
					CreateLedger: &servicepb.CreateLedgerRequest{
						Name: "test-ledger",
					},
				},
			},
		}

		_, err := adm.verifyAndResolveSignatures(requests)
		require.ErrorIs(t, err, signing.ErrMissingSignature)
	})

	t.Run("signed regular request works regardless of requireSignatures", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		pubKey, privKey := generateTestKeyPair(t)
		adm.keyStore.AddPublicKey("my-key", pubKey, "")
		adm.sharedState.SetRequireSignatures(true)

		req := &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name: "signed-ledger",
				},
			},
		}

		err := signing.Sign(req, "my-key", privKey)
		require.NoError(t, err)

		result, err := adm.verifyAndResolveSignatures([]*servicepb.Request{req})
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.NotNil(t, result[0].Signature)
	})

	t.Run("unknown key ID rejected", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		_, privKey := generateTestKeyPair(t)

		req := &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name: "test",
				},
			},
		}

		err := signing.Sign(req, "unknown-key", privKey)
		require.NoError(t, err)

		_, err = adm.verifyAndResolveSignatures([]*servicepb.Request{req})
		require.ErrorIs(t, err, signing.ErrUnknownKeyID)
	})

	t.Run("invalid signature rejected", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm := createTestAdmission(t, store)

		pubKey, _ := generateTestKeyPair(t)
		_, otherPrivKey := generateTestKeyPair(t)
		adm.keyStore.AddPublicKey("my-key", pubKey, "")

		req := &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name: "test",
				},
			},
		}

		// Sign with a different private key
		err := signing.Sign(req, "my-key", otherPrivKey)
		require.NoError(t, err)

		_, err = adm.verifyAndResolveSignatures([]*servicepb.Request{req})
		require.ErrorIs(t, err, signing.ErrInvalidSignature)
	})
}
