package admission

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestCheckMirrorPromotionsStopsBeforeSnapshotOnCancellation(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	a, _ := createTestAdmission(t, store)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := a.checkMirrorPromotions(ctx, []*raftcmdpb.Order{promotionOrder("missing")}, "")
	require.True(t, errors.Is(err, context.Canceled), "cancellation must win before the ledger read")
}

func TestMirrorPromotionBatchPlanCardinality(t *testing.T) {
	t.Parallel()

	orders := make([]*raftcmdpb.Order, 0, 1032)
	for i := range 32 {
		orders = append(orders, promotionOrder(fmt.Sprintf("mirror-%d", i)))
	}
	for i := range 1000 {
		orders = append(orders, creationOrder(fmt.Sprintf("other-%d", i)))
	}
	// Creates are collected regardless of their position relative to a
	// promotion. The admission gate must still reject a create-then-promote
	// batch after a previously deleted ledger left a tombstone.
	orders = append(orders, creationOrder("mirror-0"))

	promotions, created := mirrorPromotionBatch(orders)
	require.Len(t, promotions, 32)
	require.Len(t, created, 1001)
	require.Equal(t, "mirror-0", promotions[0].GetLedger())
	require.Equal(t, "mirror-31", promotions[31].GetLedger())
	require.Contains(t, created, "mirror-0")
	require.NotContains(t, created, "mirror-31")

	noPromotions, noCreatedMap := mirrorPromotionBatch(orders[32:1032])
	require.Empty(t, noPromotions)
	require.Nil(t, noCreatedMap, "batches without promotion need no create-name map")
}

func BenchmarkCheckMirrorPromotionsBatchSize(b *testing.B) {
	for _, tc := range []struct {
		name       string
		creates    int
		promotions int
	}{
		{name: "one_promotion", promotions: 1},
		{name: "one_promotion_1000_creates", creates: 1000, promotions: 1},
		{name: "32_promotions_1000_creates", creates: 1000, promotions: 32},
	} {
		b.Run(tc.name, func(b *testing.B) {
			store := createTestStore(b)
			a, attrs := createTestAdmission(b, store)
			batch := store.OpenWriteSession()
			orders := make([]*raftcmdpb.Order, 0, tc.creates+tc.promotions)
			for i := range tc.creates {
				orders = append(orders, creationOrder(fmt.Sprintf("other-%d", i)))
			}
			for i := range tc.promotions {
				name := fmt.Sprintf("mirror-%d", i)
				require.NoError(b, state.SaveLedger(batch, name, &commonpb.LedgerInfo{
					Name: name, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
				}))
				_, err := attrs.Boundary.Set(batch, []byte(name), &raftcmdpb.LedgerBoundaries{LastMirrorV2LogId: 1})
				require.NoError(b, err)
				require.NoError(b, state.SetMirrorSourceHead(batch, name, 1))
				orders = append(orders, promotionOrder(name))
			}
			require.NoError(b, batch.Commit())

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := a.checkMirrorPromotions(b.Context(), orders, ""); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func promotionOrder(name string) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: name, Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}},
	}}}
}

func creationOrder(name string) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: name, Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{
			Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
		}},
	}}}
}
