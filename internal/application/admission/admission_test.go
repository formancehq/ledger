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

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
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

	// Register test ledger in both global zone (for GetLedgerByName queries)
	// and attribute zone (for preloader's bloom → cache → Pebble resolution).
	testAttrs := attributes.New()
	info := &commonpb.LedgerInfo{
		Name:      testLedgerName,
		Id:        1,
		CreatedAt: commonpb.NewTimestamp(time.Now()),
	}
	batch := s.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, info))
	_, err = testAttrs.Ledger.Set(batch, domain.LedgerKey{Name: testLedgerName}.Bytes(), info)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	return s
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
	testPreloader := plan.NewBuilder(node.NewIndexTracker(1), testCache, attrs, store, nil, logger, 0)

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

func TestExtractNeededVolumes(t *testing.T) {
	t.Parallel()

	t.Run("extracts volumes for create transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
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
				},
			},
		}

		needs, _, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		// Should have 2 volume keys: both source (world) and destination (user:alice) are preloaded
		require.Equal(t, 2, needs.Count(dal.SubAttrVolume))

		worldKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "world"},
			Asset:      "USD",
		}
		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "user:alice"},
			Asset:      "USD",
		}

		require.True(t, needs.Has(dal.SubAttrVolume, worldKey.Bytes()), "should have world volume key")
		require.True(t, needs.Has(dal.SubAttrVolume, aliceKey.Bytes()), "should have alice volume key")
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
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
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
				},
			},
		}

		needs, _, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		// Should have 2 volume keys: both the new source (alice) and new destination (world) are preloaded
		require.Equal(t, 2, needs.Count(dal.SubAttrVolume))

		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "user:alice"},
			Asset:      "USD",
		}
		worldKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "world"},
			Asset:      "USD",
		}

		require.True(t, needs.Has(dal.SubAttrVolume, aliceKey.Bytes()), "should have alice volume key (source in revert)")
		require.True(t, needs.Has(dal.SubAttrVolume, worldKey.Bytes()), "should have world volume key (destination in revert)")
	})

	t.Run("extracts volumes for multiple postings in revert", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
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
				},
			},
		}

		needs, _, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		// Should have 3 volume keys: alice, bob (original destinations become sources in revert)
		// AND world (original source becomes destination in revert) - all volumes preloaded
		require.Equal(t, 3, needs.Count(dal.SubAttrVolume))

		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "user:alice"},
			Asset:      "USD",
		}
		bobKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "user:bob"},
			Asset:      "USD",
		}
		worldKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "world"},
			Asset:      "USD",
		}

		require.True(t, needs.Has(dal.SubAttrVolume, aliceKey.Bytes()))
		require.True(t, needs.Has(dal.SubAttrVolume, bobKey.Bytes()))
		require.True(t, needs.Has(dal.SubAttrVolume, worldKey.Bytes()))
	})

	t.Run("preloads transaction state when add_metadata target uses id", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
								AddMetadata: &raftcmdpb.SaveMetadataOrder{
									Target: &commonpb.Target{
										Target: &commonpb.Target_TransactionId{TransactionId: 7},
									},
									Metadata: map[string]*commonpb.MetadataValue{
										"status": commonpb.NewStringValue("paid"),
									},
								},
							},
							},
						},
					},
				},
			},
		}

		needs, _, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		require.True(t, needs.Has(dal.SubAttrTransaction, domain.TransactionKey{LedgerName: "test-ledger", ID: 7}.Bytes()), "transaction key should be preloaded when id is used")
		require.Zero(t, needs.Count(dal.SubAttrReference), "reference key should not be preloaded when id is used")
	})
}

func TestConvertApplyRequest_RevertTransaction(t *testing.T) {
	t.Parallel()

	t.Run("attaches original postings for volume coverage", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, attrs := createTestAdmission(t, store)

		// Setup: persist a TxState with Postings — the shape a post-EN-1242
		// FSM would produce after applying a CreateTransaction.
		expectedPostings := []*commonpb.Posting{
			{
				Source:      "world",
				Destination: "user:alice",
				Amount:      commonpb.NewUint256FromUint64(100),
				Asset:       "USD",
			},
		}

		batch := store.OpenWriteSession()
		_, err := attrs.Transaction.Set(batch, domain.TransactionKey{LedgerName: testLedgerName, ID: 1}.Bytes(), &commonpb.TransactionState{
			CreatedByLog: 1,
			Postings:     expectedPostings,
		})
		require.NoError(t, err)
		require.NoError(t, batch.Commit())

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

		revertOrder := order.GetData().(*raftcmdpb.LedgerApplyOrder_RevertTransaction).RevertTransaction
		require.NotNil(t, revertOrder)
		require.Equal(t, uint64(1), revertOrder.GetTransactionId())
		require.Len(t, revertOrder.GetOriginalPostings(), 1,
			"admission reads TxState.Postings directly and attaches them to declare volume coverage (invariant #9)")
		require.Equal(t, "world", revertOrder.GetOriginalPostings()[0].GetSource())
		require.Equal(t, "user:alice", revertOrder.GetOriginalPostings()[0].GetDestination())
	})

	t.Run("passes revert of non-existent transaction through to FSM (audited)", func(t *testing.T) {
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

		// A revert on a non-existent transaction must NOT fail-fast at
		// admission: invariant #8 requires business decisions to be
		// hash-chained in the audit, and only the FSM apply writes audit
		// entries. Admission emits an order with OriginalPostings=nil;
		// the FSM's processRevertTransaction returns ErrTransactionNotFound
		// (via `txID >= boundaries.GetNextTransactionId()`) BEFORE touching
		// volumes — that error lands in the audit chain.
		order, err := admission.convertApplyRequest(t.Context(), applyRequest)
		require.NoError(t, err)
		require.NotNil(t, order)

		revert, ok := order.GetData().(*raftcmdpb.LedgerApplyOrder_RevertTransaction)
		require.True(t, ok)
		require.Empty(t, revert.RevertTransaction.GetOriginalPostings(),
			"admission must pass through with nil postings when the source tx is absent")
	})

	t.Run("returns error when revert payload has no identifier", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		applyRequest := &servicepb.LedgerApplyRequest{
			Ledger: testLedgerName,
			Action: &servicepb.LedgerAction{
				Data: &servicepb.LedgerAction_RevertTransaction{
					RevertTransaction: &servicepb.RevertTransactionPayload{},
				},
			},
		}

		_, err := admission.convertApplyRequest(t.Context(), applyRequest)
		require.ErrorIs(t, err, domain.ErrTransactionTargetMissing)
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
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
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
				},
			},
		}

		needs, _, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		// Should have 2 volume keys: both source and destination are always preloaded
		require.Equal(t, 2, needs.Count(dal.SubAttrVolume), "force=true should still extract all volumes")

		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"},
			Asset:      "USD",
		}
		bobKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:bob"},
			Asset:      "USD",
		}

		require.True(t, needs.Has(dal.SubAttrVolume, aliceKey.Bytes()), "should have source volume")
		require.True(t, needs.Has(dal.SubAttrVolume, bobKey.Bytes()), "should have destination volume")
	})

	t.Run("extracts volumes when force is false for create transaction", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
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
				},
			},
		}

		needs, _, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		// Should have 2 volume keys: both source and destination are always preloaded
		require.Equal(t, 2, needs.Count(dal.SubAttrVolume), "force=false should extract all volumes")

		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"},
			Asset:      "USD",
		}
		bobKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:bob"},
			Asset:      "USD",
		}

		require.True(t, needs.Has(dal.SubAttrVolume, aliceKey.Bytes()))
		require.True(t, needs.Has(dal.SubAttrVolume, bobKey.Bytes()))
	})

	t.Run("mixed orders: all volumes extracted regardless of force flag", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			// First order with force=true - volumes are still extracted
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
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
				},
			},
			// Second order with force=false - volumes are also extracted
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
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
				},
			},
		}

		needs, _, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		// Should have 4 volume keys: source+dest from both orders
		require.Equal(t, 4, needs.Count(dal.SubAttrVolume))

		// Verify force=true volumes ARE present
		forceSourceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:force_source"},
			Asset:      "USD",
		}
		forceDestKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:force_dest"},
			Asset:      "USD",
		}
		require.True(t, needs.Has(dal.SubAttrVolume, forceSourceKey.Bytes()), "force=true order should have source volumes extracted")
		require.True(t, needs.Has(dal.SubAttrVolume, forceDestKey.Bytes()), "force=true order should have destination volumes extracted")

		// Verify force=false volumes are present
		normalSourceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:normal_source"},
			Asset:      "EUR",
		}
		normalDestKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:normal_dest"},
			Asset:      "EUR",
		}
		require.True(t, needs.Has(dal.SubAttrVolume, normalSourceKey.Bytes()), "force=false order should have source volumes extracted")
		require.True(t, needs.Has(dal.SubAttrVolume, normalDestKey.Bytes()), "force=false order should have destination volumes extracted")
	})

	t.Run("force on revert still extracts volumes", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		// force=true on revert still preloads all volumes
		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
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
				},
			},
		}

		needs, _, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		// Revert reverses postings: alice->world. Both source (alice) and destination (world) preloaded.
		require.Equal(t, 2, needs.Count(dal.SubAttrVolume), "revert with force=true should still extract all volumes")

		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "user:alice"},
			Asset:      "USD",
		}
		worldKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "world"},
			Asset:      "USD",
		}

		require.True(t, needs.Has(dal.SubAttrVolume, aliceKey.Bytes()), "should have alice volume key (source in revert)")
		require.True(t, needs.Has(dal.SubAttrVolume, worldKey.Bytes()), "should have world volume key (destination in revert)")
	})
}

// TestExtractPreloadNeeds_AccountMetadata_ScriptBacked guards against a
// regression class exposed by the camelCase decode fix in #452. Before
// the decode fix, `accountMetadata` was silently dropped on script-backed
// requests so the bug never surfaced. With decoding fixed, the
// previously-correct posting-only branch shape would have caused
// processCreateTransaction's previousAccountMetadata capture to read an
// un-preloaded key for any inline script / scriptReference combined with
// accountMetadata. Caller-supplied accountMetadata keys must reach the
// preload set in all three shapes (postings-only, inline-script,
// scriptReference).
func TestExtractPreloadNeeds_AccountMetadata_ScriptBacked(t *testing.T) {
	t.Parallel()

	makeAccountMetadata := func() map[string]*commonpb.MetadataMap {
		return map[string]*commonpb.MetadataMap{
			"users:alice": {Values: map[string]*commonpb.MetadataValue{
				"vip": commonpb.NewStringValue("yes"),
			}},
		}
	}

	cases := []struct {
		name string
		ct   *raftcmdpb.CreateTransactionOrder
	}{
		{
			name: "postings + accountMetadata",
			ct: &raftcmdpb.CreateTransactionOrder{
				Postings: []*commonpb.Posting{{
					Source:      "world",
					Destination: "users:alice",
					Amount:      commonpb.NewUint256FromUint64(1),
					Asset:       "USD",
				}},
				AccountMetadata: makeAccountMetadata(),
			},
		},
		{
			name: "inline script + accountMetadata",
			ct: &raftcmdpb.CreateTransactionOrder{
				Script:          &commonpb.Script{Plain: "send [USD 1] (source = @world destination = @users:alice)"},
				AccountMetadata: makeAccountMetadata(),
			},
		},
		{
			name: "scriptReference + accountMetadata",
			ct: &raftcmdpb.CreateTransactionOrder{
				NumscriptReference: &raftcmdpb.NumscriptReference{Name: "payment", Version: "1.0.0"},
				AccountMetadata:    makeAccountMetadata(),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := createTestStore(t)
			admission, _ := createTestAdmission(t, store)

			orders := []*raftcmdpb.Order{
				{
					Type: &raftcmdpb.Order_LedgerScoped{
						LedgerScoped: &raftcmdpb.LedgerScopedOrder{
							Ledger: testLedgerName,
							Payload: &raftcmdpb.LedgerScopedOrder_Apply{
								Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
									CreateTransaction: tc.ct,
								},
								},
							},
						},
					},
				},
			}

			needs, _, err := admission.extractPreloadNeeds(context.Background(), orders)
			require.NoError(t, err)

			key := domain.MetadataKey{
				AccountKey: domain.AccountKey{LedgerName: testLedgerName, Account: "users:alice"},
				Key:        "vip",
			}
			require.True(t, needs.Has(dal.SubAttrMetadata, key.Bytes()),
				"caller-supplied accountMetadata key must reach the preload set so the FSM can capture previousAccountMetadata",
			)
		})
	}
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

	t.Run("converts revert request with empty OriginalPostings", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		// Non-receipt reverts leave OriginalPostings nil on the wire; the FSM
		// reads TxState.Postings authoritatively at apply time.
		request := &servicepb.Request{
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

		order, err := admission.requestToOrder(t.Context(), request, nil, newBulkOverlay())
		require.NoError(t, err)
		require.NotNil(t, order)

		ls := order.GetLedgerScoped()
		require.NotNil(t, ls)
		require.Equal(t, testLedgerName, ls.GetLedger())
		applyOrder := ls.GetPayload().(*raftcmdpb.LedgerScopedOrder_Apply).Apply

		revertOrder := applyOrder.GetData().(*raftcmdpb.LedgerApplyOrder_RevertTransaction).RevertTransaction
		require.Equal(t, uint64(42), revertOrder.GetTransactionId())
		require.True(t, revertOrder.GetForce())
		require.Empty(t, revertOrder.GetOriginalPostings())
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
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
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
				},
			},
		}

		overlay := newBulkOverlay()
		needs, perOrderNeeds, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		require.NoError(t, admission.resolveScriptsAndEnrichNeeds(context.Background(), orders, overlay, needs, perOrderNeeds, false))

		// Both source and destination volumes are preloaded from numscript
		require.Equal(t, 2, needs.Count(dal.SubAttrVolume), "numscript emulation should discover all volumes")

		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"},
			Asset:      "USD/2",
		}
		bobKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:bob"},
			Asset:      "USD/2",
		}

		require.True(t, needs.Has(dal.SubAttrVolume, aliceKey.Bytes()), "should discover source account from numscript")
		require.True(t, needs.Has(dal.SubAttrVolume, bobKey.Bytes()), "should preload destination account from numscript")
	})

	t.Run("extracts numscript volumes even when force is true", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
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
				},
			},
		}

		overlay := newBulkOverlay()
		needs, perOrderNeeds, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		require.NoError(t, admission.resolveScriptsAndEnrichNeeds(context.Background(), orders, overlay, needs, perOrderNeeds, false))

		// Force=true no longer skips volume extraction - all volumes are preloaded
		require.Equal(t, 2, needs.Count(dal.SubAttrVolume), "force=true should still extract numscript volumes")

		aliceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"},
			Asset:      "USD/2",
		}
		bobKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:bob"},
			Asset:      "USD/2",
		}

		require.True(t, needs.Has(dal.SubAttrVolume, aliceKey.Bytes()), "should have source volume from numscript")
		require.True(t, needs.Has(dal.SubAttrVolume, bobKey.Bytes()), "should have destination volume from numscript")
	})

	t.Run("discovers volumes from numscript reference vars", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{
									NumscriptReference: &raftcmdpb.NumscriptReference{
										Name: "pay",
										Vars: map[string]string{
											"account": "users:alice",
											"amount":  "USD/2 1000",
										},
									},
								},
							},
							},
						},
					},
				},
			},
		}

		overlay := newBulkOverlay()
		overlay.recordNumscriptSave(testLedgerName, "pay", "v1", `
#![feature("experimental-account-interpolation")]
				vars {
					string $account
					monetary $amount
				}
				send $amount (
					source = @world
					destination = @$account
				)
			`)

		needs, perOrderNeeds, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)
		require.NoError(t, admission.resolveScriptsAndEnrichNeeds(context.Background(), orders, overlay, needs, perOrderNeeds, false))

		require.True(t, needs.Has(dal.SubAttrVolume, domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"},
			Asset:      "USD/2",
		}.Bytes()), "should discover destination account from reference vars")

		// Admission no longer rewrites the reference — the audited order keeps the
		// client's selector (empty = latest); only planning/discovery resolves it.
		ref := orders[0].GetLedgerScoped().GetApply().GetCreateTransaction().GetNumscriptReference()
		require.Equal(t, "", ref.GetVersion())
		require.Equal(t, "users:alice", ref.GetVars()["account"])
	})

	t.Run("rejects numscript dependency discovery failures", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{
									Script: &commonpb.Script{
										Plain: `send [USD/2 invalid] ( source = @world destination = @users:alice )`,
									},
								},
							},
							},
						},
					},
				},
			},
		}

		overlay := newBulkOverlay()
		needs, perOrderNeeds, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		err = admission.resolveScriptsAndEnrichNeeds(context.Background(), orders, overlay, needs, perOrderNeeds, false)
		require.Error(t, err)

		var businessErr *domain.BusinessError
		require.ErrorAs(t, err, &businessErr)

		// A parse error is definitive (deterministic KindValidation): it surfaces
		// as the real ErrNumscriptParse, NOT wrapped as the retryable
		// ErrDependencyDiscoveryFailed — no retry could ever make it parse, so
		// forwarding/retry semantics would spin forever.
		var discoveryErr *domain.ErrDependencyDiscoveryFailed
		require.NotErrorAs(t, err, &discoveryErr)

		var parseErr *domain.ErrNumscriptParse
		require.ErrorAs(t, err, &parseErr)
		require.Zero(t, needs.Count(dal.SubAttrVolume))
	})

	t.Run("rejects numscript emulation failures during dependency discovery", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{
									Script: &commonpb.Script{
										Plain: `
										vars {
											monetary $amount
										}
										send $amount (
											source = @world
											destination = @users:alice
										)
									`,
									},
								},
							},
							},
						},
					},
				},
			},
		}

		overlay := newBulkOverlay()
		needs, perOrderNeeds, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		err = admission.resolveScriptsAndEnrichNeeds(context.Background(), orders, overlay, needs, perOrderNeeds, false)
		require.Error(t, err)

		var businessErr *domain.BusinessError
		require.ErrorAs(t, err, &businessErr)

		// EN-1557: an inline script with a deterministic no-read failure (undeclared
		// $amount — resolution fails before any balance/metadata lookup) surfaces
		// the real numscript cause directly. It is NOT wrapped in the retryable
		// ErrDependencyDiscoveryFailed: it can never succeed on retry, so it must
		// terminate rather than invite a retry loop.
		var runtimeErr *domain.ErrNumscriptRuntime
		require.ErrorAs(t, err, &runtimeErr)

		var discoveryErr *domain.ErrDependencyDiscoveryFailed
		require.NotErrorAs(t, err, &discoveryErr,
			"a deterministic no-read failure surfaces the cause, not the retryable discovery error")
		require.Zero(t, needs.Count(dal.SubAttrVolume))
	})

	t.Run("forwards discovery failure (marks preload_unavailable) when an idempotency key is present", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		// A STATE-DEPENDENT failure: meta(@cfg,"dest") attempts a metadata read and
		// then fails because the key is absent against current state. That is
		// exactly the class an idempotency key must still forward (EN-1557): the
		// batch may be a replay of a frozen outcome, so admission marks the order
		// preload_unavailable and forwards it for the FSM to decide. (A deterministic
		// no-read failure — e.g. undeclared $amount — now terminates instead; see
		// TestForwardOrFail_ProvenanceClassification.)
		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{
									Script: &commonpb.Script{
										Plain: `
										vars {
											account $dst = meta(@cfg, "dest")
										}
										send [USD/2 1] (
											source = @world
											destination = $dst
										)
									`,
									},
								},
							},
							},
						},
					},
				},
			},
		}

		overlay := newBulkOverlay()
		needs, perOrderNeeds, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		// hasIdempotencyKey = true → forward instead of fail-fast.
		err = admission.resolveScriptsAndEnrichNeeds(context.Background(), orders, overlay, needs, perOrderNeeds, true)
		require.NoError(t, err)
		require.True(t, orders[0].GetTechnical().GetPreloadUnavailable(),
			"a forwarded order must be marked preload_unavailable for the FSM guard")
	})

	t.Run("does NOT forward a definitive parse error even when an idempotency key is present", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		// A parse error is definitive and deterministic: no retry can make it
		// parse, so there is no frozen outcome to replay. Even WITH an idempotency
		// key it must surface the real terminal error, NOT be forwarded as a
		// retryable preload_unavailable (which would spin forever). Regression for
		// the terminal-classification review finding.
		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{
									Script: &commonpb.Script{
										Plain: `send [USD/2 invalid] ( source = @world destination = @users:alice )`,
									},
								},
							},
							},
						},
					},
				},
			},
		}

		overlay := newBulkOverlay()
		needs, perOrderNeeds, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		// hasIdempotencyKey = true, but the cause is definitive → terminal.
		err = admission.resolveScriptsAndEnrichNeeds(context.Background(), orders, overlay, needs, perOrderNeeds, true)
		require.Error(t, err)

		var parseErr *domain.ErrNumscriptParse
		require.ErrorAs(t, err, &parseErr, "definitive parse error must surface, not be forwarded")
		require.False(t, orders[0].GetTechnical().GetPreloadUnavailable(),
			"a definitive error must NOT be marked preload_unavailable")
	})

	t.Run("falls back to postings when script has explicit postings", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		admission, _ := createTestAdmission(t, store)

		// When both Script and Postings are present, explicit Postings take precedence
		orders := []*raftcmdpb.Order{
			{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: testLedgerName,
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
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
				},
			},
		}

		needs, _, err := admission.extractPreloadNeeds(context.Background(), orders)
		require.NoError(t, err)

		// Should use explicit postings, not numscript emulation; both source and destination preloaded
		require.Equal(t, 2, needs.Count(dal.SubAttrVolume))

		bankKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "bank"},
			Asset:      "EUR",
		}
		merchantKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "merchant"},
			Asset:      "EUR",
		}

		require.True(t, needs.Has(dal.SubAttrVolume, bankKey.Bytes()), "should use explicit posting source")
		require.True(t, needs.Has(dal.SubAttrVolume, merchantKey.Bytes()), "should use explicit posting destination")
	})
}

// TestValidateIdempotencyKey covers the batch-level idempotency key bounds.
// The key now lives on the ApplyBatch and is validated once per proposal.
func TestValidateIdempotencyKey(t *testing.T) {
	t.Parallel()

	t.Run("accepts idempotency key within max length", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, validateIdempotencyKey("valid-key-123"))
	})

	t.Run("accepts idempotency key at exactly max length", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, validateIdempotencyKey(strings.Repeat("a", 256)))
	})

	t.Run("rejects idempotency key exceeding max length", func(t *testing.T) {
		t.Parallel()
		err := validateIdempotencyKey(strings.Repeat("a", 257))
		require.Error(t, err)
		require.ErrorIs(t, err, ErrIdempotencyKeyTooLong)
	})

	t.Run("no idempotency key is accepted", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, validateIdempotencyKey(""))
	})
}

func TestRequestsToOrders_CheckpointOrderPosition(t *testing.T) {
	t.Parallel()

	applyReq := func() *servicepb.Request {
		return &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{Name: "ledger-" + t.Name()},
			},
		}
	}
	checkpointReq := func() *servicepb.Request {
		return &servicepb.Request{
			Type: &servicepb.Request_CreateQueryCheckpoint{
				CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{},
			},
		}
	}
	closeChapterReq := func() *servicepb.Request {
		return &servicepb.Request{
			Type: &servicepb.Request_CloseChapter{
				CloseChapter: &servicepb.CloseChapterRequest{},
			},
		}
	}

	cases := []struct {
		name    string
		reqs    []*servicepb.Request
		wantErr error
	}{
		{"empty batch", nil, nil},
		{"single apply", []*servicepb.Request{applyReq()}, nil},
		{"checkpoint alone", []*servicepb.Request{checkpointReq()}, nil},
		{"close chapter alone", []*servicepb.Request{closeChapterReq()}, nil},
		{"apply then checkpoint", []*servicepb.Request{applyReq(), checkpointReq()}, nil},
		{"apply then close chapter", []*servicepb.Request{applyReq(), closeChapterReq()}, nil},
		{"checkpoint then apply", []*servicepb.Request{checkpointReq(), applyReq()}, ErrCheckpointOrderNotLast},
		{"close chapter then apply", []*servicepb.Request{closeChapterReq(), applyReq()}, ErrCheckpointOrderNotLast},
		{"checkpoint mid-batch", []*servicepb.Request{applyReq(), checkpointReq(), applyReq()}, ErrCheckpointOrderNotLast},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := createTestStore(t)
			adm, _ := createTestAdmission(t, store)

			_, _, err := adm.requestsToOrders(t.Context(), tc.reqs, nil)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// generateTestKeyPair generates an Ed25519 key pair for testing.
func generateTestKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()

	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	return pubKey, privKey
}

// signedBatchRequest signs a single-request ApplyBatch and wraps it into the
// signed ApplyRequest the Admit RPC carries.
func signedBatchRequest(t *testing.T, req *servicepb.Request, keyID string, privKey ed25519.PrivateKey) *servicepb.ApplyRequest {
	t.Helper()

	sb, err := signing.Sign(&servicepb.ApplyBatch{Requests: []*servicepb.Request{req}}, keyID, privKey)
	require.NoError(t, err)

	return servicepb.SignedApplyRequest(sb)
}

func TestResolveBatch(t *testing.T) {
	t.Parallel()

	t.Run("bootstrap: unsigned RegisterSigningKey allowed when no keys exist", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm, _ := createTestAdmission(t, store)

		pubKey, _ := generateTestKeyPair(t)

		req := servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_RegisterSigningKey{
				RegisterSigningKey: &servicepb.RegisterSigningKeyRequest{
					KeyId:     "first-key",
					PublicKey: []byte(pubKey),
				},
			},
		})

		result, err := adm.resolveBatch(req)
		require.NoError(t, err)
		require.Len(t, result.requests, 1)
		require.Nil(t, result.sig, "bootstrap request should have no signature")
	})

	t.Run("unsigned RegisterSigningKey rejected when keys exist", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm, _ := createTestAdmission(t, store)

		existingPubKey, _ := generateTestKeyPair(t)
		adm.keyStore.AddPublicKey("existing", existingPubKey, "")

		newPubKey, _ := generateTestKeyPair(t)

		req := servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_RegisterSigningKey{
				RegisterSigningKey: &servicepb.RegisterSigningKeyRequest{
					KeyId:     "new-key",
					PublicKey: []byte(newPubKey),
				},
			},
		})

		_, err := adm.resolveBatch(req)
		require.ErrorIs(t, err, signing.ErrMissingSignature)
	})

	t.Run("unsigned RevokeSigningKey rejected when keys exist", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm, _ := createTestAdmission(t, store)

		pubKey, _ := generateTestKeyPair(t)
		adm.keyStore.AddPublicKey("my-key", pubKey, "")

		req := servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_RevokeSigningKey{
				RevokeSigningKey: &servicepb.RevokeSigningKeyRequest{
					KeyId: "my-key",
				},
			},
		})

		_, err := adm.resolveBatch(req)
		require.ErrorIs(t, err, signing.ErrMissingSignature)
	})

	t.Run("unsigned SetSigningConfig rejected when keys exist", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm, _ := createTestAdmission(t, store)

		pubKey, _ := generateTestKeyPair(t)
		adm.keyStore.AddPublicKey("my-key", pubKey, "")

		req := servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_SetSigningConfig{
				SetSigningConfig: &servicepb.SetSigningConfigRequest{
					RequireSignatures: true,
				},
			},
		})

		_, err := adm.resolveBatch(req)
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

		result, err := adm.resolveBatch(signedBatchRequest(t, req, "existing", existingPrivKey))
		require.NoError(t, err)
		require.Len(t, result.requests, 1)
		require.NotNil(t, result.sig, "signature should be preserved for propagation")
	})

	t.Run("unsigned regular request allowed when requireSignatures is false", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm, _ := createTestAdmission(t, store)

		req := servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name: "test-ledger",
				},
			},
		})

		result, err := adm.resolveBatch(req)
		require.NoError(t, err)
		require.Len(t, result.requests, 1)
	})

	t.Run("unsigned regular request rejected when requireSignatures is true", func(t *testing.T) {
		t.Parallel()
		store := createTestStore(t)
		adm, _ := createTestAdmission(t, store)

		adm.sharedState.SetRequireSignatures(true)

		req := servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{
					Name: "test-ledger",
				},
			},
		})

		_, err := adm.resolveBatch(req)
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

		result, err := adm.resolveBatch(signedBatchRequest(t, req, "my-key", privKey))
		require.NoError(t, err)
		require.Len(t, result.requests, 1)
		require.NotNil(t, result.sig)
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

		_, err := adm.resolveBatch(signedBatchRequest(t, req, "unknown-key", privKey))
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
		_, err := adm.resolveBatch(signedBatchRequest(t, req, "my-key", otherPrivKey))
		require.ErrorIs(t, err, signing.ErrInvalidSignature)
	})
}
