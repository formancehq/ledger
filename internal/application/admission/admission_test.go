package admission

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger-v3-poc/internal/domain/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing/numscript"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/preload"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

const testLedgerName = "test-ledger"

// createTestStore creates a test store with a registered ledger.
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

// createTransactionLog creates a log with a CreatedTransaction payload.
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

// createTestAdmission creates an Admission instance for testing.
// It returns both the Admission and the Attributes so tests can set up
// transaction state directly in Pebble.
func createTestAdmission(t *testing.T, store *dal.Store) (*Admission, *attributes.Attributes) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	testCache, _ := cache.New(100, nil)
	attrs := attributes.New()
	testPreloader := preload.New(node.NewIndexTracker(1), testCache, attrs, store, nil, logger)

	ks := keystore.NewKeyStore()
	ss := state.NewSharedState()

	return NewAdmission(
		store,
		logger,
		nil, // no proposer needed for unit tests
		testPreloader,
		noop.NewMeterProvider(),
		nil, // no health checker needed for unit tests
		ks,
		ss,
		attrs,
		numscript.NewNumscriptCache(0),
		func(context.Context) error { return nil },
	), attrs
}

func TestGetTransactionPostings(t *testing.T) {
	t.Parallel()

	t.Run("returns postings for existing transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, attrs := createTestAdmission(t, store)

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

		// Store TransactionState to link transaction ID to its creating log
		_, err = attrs.Transaction.Set(batch, domain.TransactionKey{Ledger: testLedgerName, ID: 1}.Bytes(), &commonpb.TransactionState{
			CreatedByLog: 1,
		})
		require.NoError(t, err)
		require.NoError(t, state.SetAppliedIndex(batch, 1))
		require.NoError(t, batch.Commit())

		// Test getTransactionPostings
		postings, err := admission.getTransactionPostings(testLedgerName, 1)
		require.NoError(t, err)
		require.Len(t, postings, 2)
		require.Equal(t, expectedPostings[0].GetSource(), postings[0].GetSource())
		require.Equal(t, expectedPostings[0].GetDestination(), postings[0].GetDestination())
		require.Equal(t, expectedPostings[0].GetAsset(), postings[0].GetAsset())
		require.Equal(t, expectedPostings[1].GetSource(), postings[1].GetSource())
		require.Equal(t, expectedPostings[1].GetDestination(), postings[1].GetDestination())
		require.Equal(t, expectedPostings[1].GetAsset(), postings[1].GetAsset())
	})

	t.Run("returns error for non-existent transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

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
		admission, _ := createTestAdmission(t, store)

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

		needs, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		volumes := needs.Volumes

		// Should have 2 volume keys: both source (world) and destination (user:alice) are preloaded
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
		admission, _ := createTestAdmission(t, store)

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

		needs, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		volumes := needs.Volumes

		// Should have 2 volume keys: both the new source (alice) and new destination (world) are preloaded
		require.Len(t, volumes, 2)

		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "user:alice"},
			Asset:      "USD",
		}
		worldKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "world"},
			Asset:      "USD",
		}

		_, hasAlice := volumes[aliceKey]
		_, hasWorld := volumes[worldKey]
		require.True(t, hasAlice, "should have alice volume key (source in revert)")
		require.True(t, hasWorld, "should have world volume key (destination in revert)")
	})

	t.Run("extracts volumes for multiple postings in revert", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

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

		needs, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		volumes := needs.Volumes

		// Should have 3 volume keys: alice, bob (original destinations become sources in revert)
		// AND world (original source becomes destination in revert) - all volumes preloaded
		require.Len(t, volumes, 3)

		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "user:alice"},
			Asset:      "USD",
		}
		bobKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "user:bob"},
			Asset:      "USD",
		}
		worldKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "world"},
			Asset:      "USD",
		}

		_, hasAlice := volumes[aliceKey]
		_, hasBob := volumes[bobKey]
		_, hasWorld := volumes[worldKey]

		require.True(t, hasAlice)
		require.True(t, hasBob)
		require.True(t, hasWorld)
	})
}

func TestConvertApplyRequest_RevertTransaction(t *testing.T) {
	t.Parallel()

	t.Run("fetches original postings for revert transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, attrs := createTestAdmission(t, store)

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
		// Store TransactionState to link transaction ID to its creating log
		_, err = attrs.Transaction.Set(batch, domain.TransactionKey{Ledger: testLedgerName, ID: 1}.Bytes(), &commonpb.TransactionState{
			CreatedByLog: 1,
		})
		require.NoError(t, err)
		require.NoError(t, state.SetAppliedIndex(batch, 1))
		require.NoError(t, batch.Commit())

		// Now convert a revert request
		applyRequest := &servicepb.LedgerApplyRequest{
			Ledger: testLedgerName,
			Action: &servicepb.LedgerAction{
				Data: &servicepb.LedgerAction_RevertTransaction{
					RevertTransaction: &servicepb.RevertTransactionPayload{
						TransactionId:   1,
						Force:           false,
						AtEffectiveDate: true,
					},
				},
			},
		}

		order, err := admission.convertApplyRequest(t.Context(), applyRequest)
		require.NoError(t, err)
		require.NotNil(t, order)

		// Verify the order contains the original postings
		revertOrder := order.GetData().(*raftcmdpb.LedgerApplyOrder_RevertTransaction).RevertTransaction
		require.NotNil(t, revertOrder)
		require.Equal(t, uint64(1), revertOrder.GetTransactionId())
		require.False(t, revertOrder.GetForce())
		require.True(t, revertOrder.GetAtEffectiveDate())
		require.Len(t, revertOrder.GetOriginalPostings(), 1)
		require.Equal(t, "world", revertOrder.GetOriginalPostings()[0].GetSource())
		require.Equal(t, "user:alice", revertOrder.GetOriginalPostings()[0].GetDestination())
		require.Equal(t, "USD", revertOrder.GetOriginalPostings()[0].GetAsset())
	})

	t.Run("returns error when transaction to revert does not exist", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		applyRequest := &servicepb.LedgerApplyRequest{
			Ledger: testLedgerName,
			Action: &servicepb.LedgerAction{
				Data: &servicepb.LedgerAction_RevertTransaction{
					RevertTransaction: &servicepb.RevertTransactionPayload{
						TransactionId: 999,
					},
				},
			},
		}

		_, err := admission.convertApplyRequest(t.Context(), applyRequest)
		require.Error(t, err)
		require.Contains(t, err.Error(), "getting original transaction postings")
	})
}

func TestExtractNeededVolumes_Force(t *testing.T) {
	t.Parallel()

	t.Run("extracts volumes even when force is true for create transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
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
					},
				},
			},
		}

		needs, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		volumes := needs.Volumes

		// Should have 2 volume keys: both source and destination are always preloaded
		require.Len(t, volumes, 2, "force=true should still extract all volumes")

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
		require.True(t, hasAlice, "should have source volume")
		require.True(t, hasBob, "should have destination volume")
	})

	t.Run("extracts volumes when force is false for create transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

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

		needs, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		volumes := needs.Volumes

		// Should have 2 volume keys: both source and destination are always preloaded
		require.Len(t, volumes, 2, "force=false should extract all volumes")

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

	t.Run("mixed orders: all volumes extracted regardless of force flag", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			// First order with force=true - volumes are still extracted
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
			// Second order with force=false - volumes are also extracted
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

		needs, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		volumes := needs.Volumes

		// Should have 4 volume keys: source+dest from both orders
		require.Len(t, volumes, 4)

		// Verify force=true volumes ARE present
		forceSourceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "users:force_source"},
			Asset:      "USD",
		}
		forceDestKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "users:force_dest"},
			Asset:      "USD",
		}
		_, hasForceSource := volumes[forceSourceKey]
		_, hasForceDest := volumes[forceDestKey]
		require.True(t, hasForceSource, "force=true order should have source volumes extracted")
		require.True(t, hasForceDest, "force=true order should have destination volumes extracted")

		// Verify force=false volumes are present
		normalSourceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "users:normal_source"},
			Asset:      "EUR",
		}
		normalDestKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "users:normal_dest"},
			Asset:      "EUR",
		}
		_, hasNormalSource := volumes[normalSourceKey]
		_, hasNormalDest := volumes[normalDestKey]
		require.True(t, hasNormalSource, "force=false order should have source volumes extracted")
		require.True(t, hasNormalDest, "force=false order should have destination volumes extracted")
	})

	t.Run("force on revert still extracts volumes", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		// force=true on revert still preloads all volumes
		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: testLedgerName,
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
							RevertTransaction: &raftcmdpb.RevertTransactionOrder{
								TransactionId: 1,
								Force:         true,
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

		needs, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		volumes := needs.Volumes

		// Revert reverses postings: alice->world. Both source (alice) and destination (world) preloaded.
		require.Len(t, volumes, 2, "revert with force=true should still extract all volumes")

		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "user:alice"},
			Asset:      "USD",
		}
		worldKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "world"},
			Asset:      "USD",
		}

		_, hasAlice := volumes[aliceKey]
		_, hasWorld := volumes[worldKey]
		require.True(t, hasAlice, "should have alice volume key (source in revert)")
		require.True(t, hasWorld, "should have world volume key (destination in revert)")
	})
}

func TestConvertApplyRequest_CreateTransaction_Force(t *testing.T) {
	t.Parallel()

	t.Run("propagates force flag to order", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		applyRequest := &servicepb.LedgerApplyRequest{
			Ledger: testLedgerName,
			Action: &servicepb.LedgerAction{
				Data: &servicepb.LedgerAction_CreateTransaction{
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
			},
		}

		order, err := admission.convertApplyRequest(t.Context(), applyRequest)
		require.NoError(t, err)
		require.NotNil(t, order)

		createOrder := order.GetData().(*raftcmdpb.LedgerApplyOrder_CreateTransaction).CreateTransaction
		require.True(t, createOrder.GetForce(), "force flag should be propagated to order")
	})
}

func TestRequestToOrder_RevertTransaction(t *testing.T) {
	t.Parallel()

	t.Run("converts revert request with original postings", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, attrs := createTestAdmission(t, store)

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
		// Store TransactionState to link transaction ID to its creating log
		_, err = attrs.Transaction.Set(batch, domain.TransactionKey{Ledger: testLedgerName, ID: 42}.Bytes(), &commonpb.TransactionState{
			CreatedByLog: 1,
		})
		require.NoError(t, err)
		require.NoError(t, state.SetAppliedIndex(batch, 1))
		require.NoError(t, batch.Commit())

		// Create revert request
		request := &servicepb.Request{
			IdempotencyKey: "revert-tx-42",
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: testLedgerName,
					Action: &servicepb.LedgerAction{
						Data: &servicepb.LedgerAction_RevertTransaction{
							RevertTransaction: &servicepb.RevertTransactionPayload{
								TransactionId: 42,
								Force:         true,
							},
						},
					},
				},
			},
		}

		order, err := admission.requestToOrder(t.Context(), request, newBulkOverlay())
		require.NoError(t, err)
		require.NotNil(t, order)
		require.NotNil(t, order.GetIdempotency())
		require.Equal(t, "revert-tx-42", order.GetIdempotency().GetKey())

		applyOrder := order.GetType().(*raftcmdpb.Order_Apply).Apply
		require.Equal(t, testLedgerName, applyOrder.GetLedger())

		revertOrder := applyOrder.GetData().(*raftcmdpb.LedgerApplyOrder_RevertTransaction).RevertTransaction
		require.Equal(t, uint64(42), revertOrder.GetTransactionId())
		require.True(t, revertOrder.GetForce())
		require.Len(t, revertOrder.GetOriginalPostings(), 1)
		require.Equal(t, "bank", revertOrder.GetOriginalPostings()[0].GetSource())
		require.Equal(t, "user:charlie", revertOrder.GetOriginalPostings()[0].GetDestination())
		require.Equal(t, "EUR", revertOrder.GetOriginalPostings()[0].GetAsset())
	})
}

func TestExtractNeededVolumes_Numscript(t *testing.T) {
	t.Parallel()

	t.Run("discovers volumes from numscript script", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

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

		overlay := newBulkOverlay()
		needs, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		require.NoError(t, admission.resolveScriptsAndEnrichNeeds(context.Background(), orders, overlay, needs))
		volumes := needs.Volumes

		// Both source and destination volumes are preloaded from numscript
		require.Len(t, volumes, 2, "numscript emulation should discover all volumes")

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
		require.True(t, hasBob, "should preload destination account from numscript")
	})

	t.Run("extracts numscript volumes even when force is true", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

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

		overlay := newBulkOverlay()
		needs, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		require.NoError(t, admission.resolveScriptsAndEnrichNeeds(context.Background(), orders, overlay, needs))
		volumes := needs.Volumes

		// Force=true no longer skips volume extraction - all volumes are preloaded
		require.Len(t, volumes, 2, "force=true should still extract numscript volumes")

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
		require.True(t, hasAlice, "should have source volume from numscript")
		require.True(t, hasBob, "should have destination volume from numscript")
	})

	t.Run("falls back to postings when script has explicit postings", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

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

		needs, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		volumes := needs.Volumes

		// Should use explicit postings, not numscript emulation; both source and destination preloaded
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
		adm, _ := createTestAdmission(t, store)

		req := &servicepb.Request{
			IdempotencyKey: "valid-key-123",
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name: "test",
				},
			},
		}

		order, err := adm.requestToOrder(t.Context(), req, newBulkOverlay())
		require.NoError(t, err)
		require.NotNil(t, order)
		require.NotNil(t, order.GetIdempotency())
		require.Equal(t, "valid-key-123", order.GetIdempotency().GetKey())
	})

	t.Run("accepts idempotency key at exactly max length", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm, _ := createTestAdmission(t, store)

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

		order, err := adm.requestToOrder(t.Context(), req, newBulkOverlay())
		require.NoError(t, err)
		require.NotNil(t, order)
		require.NotNil(t, order.GetIdempotency())
		require.Equal(t, key, order.GetIdempotency().GetKey())
	})

	t.Run("rejects idempotency key exceeding max length", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm, _ := createTestAdmission(t, store)

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

		_, err := adm.requestToOrder(t.Context(), req, newBulkOverlay())
		require.Error(t, err)
		require.ErrorIs(t, err, ErrIdempotencyKeyTooLong)
	})

	t.Run("no idempotency key is accepted", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm, _ := createTestAdmission(t, store)

		req := &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name: "test",
				},
			},
		}

		order, err := adm.requestToOrder(t.Context(), req, newBulkOverlay())
		require.NoError(t, err)
		require.NotNil(t, order)
		require.Nil(t, order.GetIdempotency())
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
		adm, _ := createTestAdmission(t, store)

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
		require.Nil(t, result[0].GetSignature(), "bootstrap request should have no signature")
	})

	t.Run("unsigned RegisterSigningKey rejected when keys exist", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm, _ := createTestAdmission(t, store)

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
		adm, _ := createTestAdmission(t, store)

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
		adm, _ := createTestAdmission(t, store)

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
		adm, _ := createTestAdmission(t, store)

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
		require.NotNil(t, result[0].GetSignature(), "signature should be preserved for propagation")
	})

	t.Run("unsigned regular request allowed when requireSignatures is false", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm, _ := createTestAdmission(t, store)

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
		adm, _ := createTestAdmission(t, store)

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
		adm, _ := createTestAdmission(t, store)

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
		require.NotNil(t, result[0].GetSignature())
	})

	t.Run("unknown key ID rejected", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm, _ := createTestAdmission(t, store)

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
		adm, _ := createTestAdmission(t, store)

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
