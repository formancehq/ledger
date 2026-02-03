package admission

import (
	"math/big"
	"sync/atomic"
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
	var inflight atomic.Int32

	return &Admission{
		cache:                     testCache,
		store:                     store,
		logger:                    logger,
		nextIndex:                 1,
		commandDurationHistogram:  commandDuration,
		proposeQueueLoadHistogram: proposeQueueLoad,
		proposeQueueInflight:      &inflight,
		proposeQueueFullCounter:   proposeQueueFull,
	}
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
				Amount:      commonpb.NewBigInt(big.NewInt(100)),
				Asset:       "USD",
			},
			{
				Source:      "world",
				Destination: "user:bob",
				Amount:      commonpb.NewBigInt(big.NewInt(50)),
				Asset:       "EUR",
			},
		}

		// Create and store a transaction log
		txLog := createTransactionLog(1, testLedgerName, 1, 1, expectedPostings)

		batch := store.NewBatch()
		err := batch.AppendLogs(txLog)
		require.NoError(t, err)

		// Add TransactionUpdate with TransactionInit to link transaction ID to log sequence
		require.NoError(t, batch.StoreTransactionUpdate(data.TransactionKey{LedgerName: testLedgerName, ID: 1}, &commonpb.TransactionUpdate{
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
		require.Contains(t, err.Error(), "transaction 999 not found")
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
										Amount:      commonpb.NewBigInt(big.NewInt(100)),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders)

		// Should have 2 volume keys: source (world) and destination (user:alice)
		require.Len(t, volumes, 2)

		worldKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerName: testLedgerName, Account: "world"},
			Asset:      "USD",
		}
		aliceKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerName: testLedgerName, Account: "user:alice"},
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
										Amount:      commonpb.NewBigInt(big.NewInt(100)),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders)

		// Should have 2 volume keys: original destination (alice, now source) and original source (world, now destination)
		require.Len(t, volumes, 2)

		worldKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerName: testLedgerName, Account: "world"},
			Asset:      "USD",
		}
		aliceKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerName: testLedgerName, Account: "user:alice"},
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
										Amount:      commonpb.NewBigInt(big.NewInt(100)),
										Asset:       "USD",
									},
									{
										Source:      "user:alice",
										Destination: "user:bob",
										Amount:      commonpb.NewBigInt(big.NewInt(50)),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		volumes := admission.extractNeededVolumes(orders)

		// Should have 3 volume keys: world, alice, bob (all in USD)
		require.Len(t, volumes, 3)

		worldKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerName: testLedgerName, Account: "world"},
			Asset:      "USD",
		}
		aliceKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerName: testLedgerName, Account: "user:alice"},
			Asset:      "USD",
		}
		bobKey := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerName: testLedgerName, Account: "user:bob"},
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
										Amount:      commonpb.NewBigInt(big.NewInt(100)),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		transactions := admission.extractNeededTransactions(orders)

		// Should have 1 transaction key
		require.Len(t, transactions, 1)

		txKey := data.TransactionKey{
			LedgerName: testLedgerName,
			ID:         42,
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
										Amount:      commonpb.NewBigInt(big.NewInt(100)),
										Asset:       "USD",
									},
								},
							},
						},
					},
				},
			},
		}

		transactions := admission.extractNeededTransactions(orders)

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

		transactions := admission.extractNeededTransactions(orders)

		// Should have 2 transaction keys
		require.Len(t, transactions, 2)

		txKey1 := data.TransactionKey{LedgerName: testLedgerName, ID: 1}
		txKey2 := data.TransactionKey{LedgerName: testLedgerName, ID: 2}
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
				Amount:      commonpb.NewBigInt(big.NewInt(100)),
				Asset:       "USD",
			},
		}

		txLog := createTransactionLog(1, testLedgerName, 1, 1, expectedPostings)

		batch := store.NewBatch()
		err := batch.AppendLogs(txLog)
		require.NoError(t, err)
		require.NoError(t, batch.StoreTransactionUpdate(data.TransactionKey{LedgerName: testLedgerName, ID: 1}, &commonpb.TransactionUpdate{
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
				Amount:      commonpb.NewBigInt(big.NewInt(500)),
				Asset:       "EUR",
			},
		}

		txLog := createTransactionLog(1, testLedgerName, 1, 42, expectedPostings)

		batch := store.NewBatch()
		err := batch.AppendLogs(txLog)
		require.NoError(t, err)
		require.NoError(t, batch.StoreTransactionUpdate(data.TransactionKey{LedgerName: testLedgerName, ID: 42}, &commonpb.TransactionUpdate{
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
