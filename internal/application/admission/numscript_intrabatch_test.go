package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// scriptOrder builds a ledger-scoped inline-script CreateTransaction order.
func scriptOrder(ledger, plain string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Script: &commonpb.Script{Plain: plain},
							},
						},
					},
				},
			},
		},
	}
}

func createTxOf(order *raftcmdpb.Order) *raftcmdpb.CreateTransactionOrder {
	return order.GetLedgerScoped().
		GetPayload().(*raftcmdpb.LedgerScopedOrder_Apply).Apply.
		GetData().(*raftcmdpb.LedgerApplyOrder_CreateTransaction).CreateTransaction
}

// applyOrder wraps a LedgerApplyOrder in a ledger-scoped Order.
func applyOrder(ledger string, data *raftcmdpb.LedgerApplyOrder) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: data,
				},
			},
		},
	}
}

// revertOrder builds a revert whose original postings are supplied directly
// (admission normally reads them from the store; here we set them so the FSM's
// reversed-posting balance effect is deterministic).
func revertOrder(ledger string, txID uint64, original ...*commonpb.Posting) *raftcmdpb.Order {
	return applyOrder(ledger, &raftcmdpb.LedgerApplyOrder{
		Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
			RevertTransaction: &raftcmdpb.RevertTransactionOrder{
				TransactionId:    txID,
				OriginalPostings: original,
			},
		},
	})
}

// addAccountMetaOrder builds an account-targeted AddMetadata order.
func addAccountMetaOrder(ledger, account, key, value string) *raftcmdpb.Order {
	return applyOrder(ledger, &raftcmdpb.LedgerApplyOrder{
		Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
			AddMetadata: &raftcmdpb.SaveMetadataOrder{
				Target: &commonpb.Target{
					Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: account}},
				},
				Metadata: map[string]*commonpb.MetadataValue{key: commonpb.NewStringValue(value)},
			},
		},
	})
}

// deleteAccountMetaOrder builds an account-targeted DeleteMetadata order.
func deleteAccountMetaOrder(ledger, account, key string) *raftcmdpb.Order {
	return applyOrder(ledger, &raftcmdpb.LedgerApplyOrder{
		Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
			DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
				Target: &commonpb.Target{
					Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: account}},
				},
				Key: key,
			},
		},
	})
}

// posting is a small helper for a world->account original posting.
func posting(source, dest, asset string, amount uint64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      source,
		Destination: dest,
		Asset:       asset,
		Amount:      commonpb.NewUint256FromUint64(amount),
	}
}

// writeVolume seeds a volume (input/output) directly in Pebble.
func writeVolume(t *testing.T, admission *Admission, ledger, account, asset string, input, output uint64) {
	t.Helper()

	key := domain.NewVolumeKey(ledger, account, asset)
	batch := admission.store.OpenWriteSession()
	_, err := admission.attrs.Volume.Set(batch, key.Bytes(), &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(input),
		Output: commonpb.NewUint256FromUint64(output),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

func resolveHashFor(t *testing.T, admission *Admission, orders []*raftcmdpb.Order, idx int) []byte {
	t.Helper()

	needs, perOrder, err := admission.extractPreloadNeeds(context.Background(), orders)
	require.NoError(t, err)
	require.NoError(t, admission.resolveScriptsAndEnrichNeeds(context.Background(), orders, newBulkOverlay(), needs, perOrder))

	return createTxOf(orders[idx]).GetInputsResolutionHash()
}

// TestResolveScripts_IntraBatchBalanceDependency pins EN-1406 P1-1: within one
// atomic batch, an order whose balance() depends on an EARLIER order in the same
// batch must resolve against that earlier order's effect. Admission layers the
// preceding order's net balance delta into the value source, so the dependent
// order's resolution hash matches the state the FSM will see (the FSM applies
// batch orders sequentially against a mutated WriteSet). Without the fix the
// dependent order resolves against the pre-batch snapshot (source balance 0) and
// the hash diverges from the FSM's — a permanent STALE_INPUTS_RESOLUTION.
func TestResolveScripts_IntraBatchBalanceDependency(t *testing.T) {
	t.Parallel()

	// Batch: order 0 deposits 100 into bulk:source from world; order 1 sends
	// balance(@bulk:source) onward. Order 1 depends on order 0's deposit.
	batch := []*raftcmdpb.Order{
		scriptOrder(testLedgerName, `send [USD/2 100] (source = @world destination = @bulk:source)`),
		scriptOrder(testLedgerName, `
vars {
  monetary $all = balance(@bulk:source, USD/2)
}
send $all (source = @bulk:source destination = @bulk:dest)
`),
	}

	storeBatch := createTestStore(t)
	admissionBatch, _ := createTestAdmission(t, storeBatch)
	batchHash := resolveHashFor(t, admissionBatch, batch, 1)
	require.NotEmpty(t, batchHash, "order 1 reads a balance, so it must carry a resolution hash")

	// Reference: the same order 1 resolved standalone against a store where
	// bulk:source ALREADY holds 100 (the post-order-0 state). Its hash is what
	// the FSM will compute once order 0 has applied. The intra-batch hash must
	// equal this — proving admission resolved order 1 against order 0's effect.
	storeRef := createTestStore(t)
	admissionRef, _ := createTestAdmission(t, storeRef)
	writeVolume(t, admissionRef, testLedgerName, "bulk:source", "USD/2", 100, 0)
	refHash := resolveHashFor(t, admissionRef, []*raftcmdpb.Order{
		scriptOrder(testLedgerName, `
vars {
  monetary $all = balance(@bulk:source, USD/2)
}
send $all (source = @bulk:source destination = @bulk:dest)
`),
	}, 0)

	require.Equal(t, refHash, batchHash,
		"the dependent order's resolution hash must reflect the preceding batch order's deposit")

	// Sanity: resolving the dependent order alone against an EMPTY store (source
	// balance 0) yields a different hash — confirming the overlay actually moved
	// the resolved balance.
	storeEmpty := createTestStore(t)
	admissionEmpty, _ := createTestAdmission(t, storeEmpty)
	emptyHash := resolveHashFor(t, admissionEmpty, []*raftcmdpb.Order{
		scriptOrder(testLedgerName, `
vars {
  monetary $all = balance(@bulk:source, USD/2)
}
send $all (source = @bulk:source destination = @bulk:dest)
`),
	}, 0)
	require.NotEqual(t, emptyHash, batchHash,
		"without the deposit the resolved balance (0) differs, so the hash must differ")
}

// TestResolveScripts_IntraBatchMetadataDependency pins the same for metadata: an
// order's meta() must see a set_account_meta a preceding batch order wrote.
func TestResolveScripts_IntraBatchMetadataDependency(t *testing.T) {
	t.Parallel()

	batch := []*raftcmdpb.Order{
		scriptOrder(testLedgerName, `
set_account_meta(@ib:cfg, "dest", "ib:resolved")
send [USD/2 1] (source = @world destination = @ib:cfg)
`),
		scriptOrder(testLedgerName, `
vars {
  account $dst = meta(@ib:cfg, "dest")
}
send [USD/2 5] (source = @world destination = $dst)
`),
	}

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)

	needs, perOrder, err := admission.extractPreloadNeeds(context.Background(), batch)
	require.NoError(t, err)
	require.NoError(t, admission.resolveScriptsAndEnrichNeeds(context.Background(), batch, newBulkOverlay(), needs, perOrder))

	// The dependent order's meta() resolved to ib:resolved, so that destination
	// volume must have been preloaded — proving admission saw order 0's write.
	destKey := domain.NewVolumeKey(testLedgerName, "ib:resolved", "USD/2")
	require.True(t, needs.Has(dal.SubAttrVolume, destKey.Bytes()),
		"meta()-resolved destination from an earlier batch order must be discovered")
}

// TestResolveScripts_IntraBatchRevertBalanceDependency pins that a preceding
// RevertTransaction's balance effect is folded into the overlay (flemzord P1
// follow-up). A revert applies the original postings reversed, so a later
// balance() on a reverted account must resolve against the post-revert balance,
// not the pre-batch snapshot — otherwise the hash diverges from the FSM's and
// the dependent order is permanently rejected as STALE_INPUTS_RESOLUTION.
func TestResolveScripts_IntraBatchRevertBalanceDependency(t *testing.T) {
	t.Parallel()

	// Pre-batch: rev:acct holds 250 (input 250, output 0). Order 0 reverts a tx
	// whose original posting sent 100 world->rev:acct; the reversed posting
	// sends 100 rev:acct->world, so rev:acct ends at 150. Order 1 sends
	// balance(@rev:acct) onward and must resolve against 150.
	batch := []*raftcmdpb.Order{
		revertOrder(testLedgerName, 1, posting("world", "rev:acct", "USD/2", 100)),
		scriptOrder(testLedgerName, `
vars {
  monetary $all = balance(@rev:acct, USD/2)
}
send $all (source = @rev:acct destination = @rev:dest)
`),
	}

	storeBatch := createTestStore(t)
	admissionBatch, _ := createTestAdmission(t, storeBatch)
	writeVolume(t, admissionBatch, testLedgerName, "rev:acct", "USD/2", 250, 0)
	batchHash := resolveHashFor(t, admissionBatch, batch, 1)
	require.NotEmpty(t, batchHash, "order 1 reads a balance, so it must carry a resolution hash")

	// Reference: order 1 resolved standalone against a store where rev:acct
	// already holds the post-revert 150. This is the state the FSM sees once the
	// revert has applied.
	storeRef := createTestStore(t)
	admissionRef, _ := createTestAdmission(t, storeRef)
	writeVolume(t, admissionRef, testLedgerName, "rev:acct", "USD/2", 150, 0)
	refHash := resolveHashFor(t, admissionRef, []*raftcmdpb.Order{
		scriptOrder(testLedgerName, `
vars {
  monetary $all = balance(@rev:acct, USD/2)
}
send $all (source = @rev:acct destination = @rev:dest)
`),
	}, 0)

	require.Equal(t, refHash, batchHash,
		"the dependent order's hash must reflect the preceding revert's balance effect (150)")

	// Sanity: resolving against the un-reverted pre-batch balance (250) differs.
	storePre := createTestStore(t)
	admissionPre, _ := createTestAdmission(t, storePre)
	writeVolume(t, admissionPre, testLedgerName, "rev:acct", "USD/2", 250, 0)
	preHash := resolveHashFor(t, admissionPre, []*raftcmdpb.Order{
		scriptOrder(testLedgerName, `
vars {
  monetary $all = balance(@rev:acct, USD/2)
}
send $all (source = @rev:acct destination = @rev:dest)
`),
	}, 0)
	require.NotEqual(t, preHash, batchHash,
		"without folding the revert, the resolved balance (250) differs, so the hash must differ")
}

// TestResolveScripts_IntraBatchAddMetadataDependency pins that a preceding
// account-targeted AddMetadata order is folded into the overlay: a later meta()
// must see the value it wrote (flemzord P1 follow-up).
func TestResolveScripts_IntraBatchAddMetadataDependency(t *testing.T) {
	t.Parallel()

	batch := []*raftcmdpb.Order{
		addAccountMetaOrder(testLedgerName, "am:cfg", "dest", "am:resolved"),
		scriptOrder(testLedgerName, `
vars {
  account $dst = meta(@am:cfg, "dest")
}
send [USD/2 5] (source = @world destination = $dst)
`),
	}

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)

	needs, perOrder, err := admission.extractPreloadNeeds(context.Background(), batch)
	require.NoError(t, err)
	require.NoError(t, admission.resolveScriptsAndEnrichNeeds(context.Background(), batch, newBulkOverlay(), needs, perOrder))

	// meta() resolved to am:resolved, so that destination volume must have been
	// discovered — proving admission saw the preceding AddMetadata write.
	destKey := domain.NewVolumeKey(testLedgerName, "am:resolved", "USD/2")
	require.True(t, needs.Has(dal.SubAttrVolume, destKey.Bytes()),
		"meta()-resolved destination from a preceding AddMetadata order must be discovered")
}

// TestResolveScripts_IntraBatchDeleteMetadataTombstone pins that a preceding
// account-targeted DeleteMetadata tombstones the key end to end: a later
// meta() resolves ABSENT even though the pre-batch snapshot holds a value
// (flemzord P1 follow-up on deletion tombstones). meta() on an absent key is a
// Numscript runtime error, so the batch resolution surfaces
// ErrDependencyDiscoveryFailed — proving the delete order was folded. Without
// the tombstone the pre-batch value would resolve fine.
func TestResolveScripts_IntraBatchDeleteMetadataTombstone(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)

	// Pre-batch snapshot holds dm:cfg/dest = "dm:old" — a valid meta() target.
	writeAccountMetadata(t, admission, testLedgerName, "dm:cfg", "dest", commonpb.NewStringValue("dm:old"))

	// Sanity: without any preceding delete, the script resolves against the
	// pre-batch value.
	okBatch := []*raftcmdpb.Order{
		scriptOrder(testLedgerName, `
vars {
  account $dst = meta(@dm:cfg, "dest")
}
send [USD/2 1] (source = @world destination = $dst)
`),
	}
	needs, perOrder, err := admission.extractPreloadNeeds(context.Background(), okBatch)
	require.NoError(t, err)
	require.NoError(t, admission.resolveScriptsAndEnrichNeeds(context.Background(), okBatch, newBulkOverlay(), needs, perOrder))

	// A preceding DeleteMetadata tombstones the key: the dependent meta() now
	// resolves absent and discovery fails.
	delBatch := []*raftcmdpb.Order{
		deleteAccountMetaOrder(testLedgerName, "dm:cfg", "dest"),
		scriptOrder(testLedgerName, `
vars {
  account $dst = meta(@dm:cfg, "dest")
}
send [USD/2 1] (source = @world destination = $dst)
`),
	}
	needs, perOrder, err = admission.extractPreloadNeeds(context.Background(), delBatch)
	require.NoError(t, err)

	err = admission.resolveScriptsAndEnrichNeeds(context.Background(), delBatch, newBulkOverlay(), needs, perOrder)
	require.Error(t, err, "a preceding same-batch delete must make the dependent meta() resolve absent and fail discovery")

	var businessErr *domain.BusinessError
	require.ErrorAs(t, err, &businessErr)
}
