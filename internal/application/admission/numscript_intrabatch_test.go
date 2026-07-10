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
