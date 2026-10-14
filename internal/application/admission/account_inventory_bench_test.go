package admission

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

var benchCoverageSink *plan.Coverage

func benchmarkAccountLifecycleCoverage(b *testing.B, ephemeral bool, assets int) {
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	store, err := dal.NewStore(b.TempDir(), logger, noop.NewMeterProvider().Meter("bench"), dal.DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	attrs := attributes.New()
	info := &commonpb.LedgerInfo{Name: "bench"}
	if ephemeral {
		info.AccountTypes = map[string]*commonpb.AccountType{"account": {Name: "account", Pattern: "account:{id}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL}}
	}
	account := domain.AccountKey{LedgerName: "bench", Account: "account:1"}
	batch := store.OpenWriteSession()
	if _, err := attrs.Ledger.Set(batch, domain.LedgerKey{Name: "bench"}.Bytes(), info); err != nil {
		b.Fatal(err)
	}
	for i := range assets {
		key := domain.NewVolumeKey("bench", account.Account, fmt.Sprintf("ASSET%d", i), "")
		if _, err := attrs.Volume.Set(batch, key.Bytes(), &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(1)}); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := attrs.AccountInventory.Set(batch, account.Bytes(), &raftcmdpb.AccountInventoryState{VolumeCount: uint32(assets)}); err != nil {
		b.Fatal(err)
	}
	if err := batch.Commit(); err != nil {
		b.Fatal(err)
	}
	c, err := cache.New(100, nil)
	if err != nil {
		b.Fatal(err)
	}
	a := &Admission{store: store, attrs: attrs, builder: plan.NewBuilder(node.NewIndexTracker(1), c, attrs, store, nil, logger, 0)}
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: "bench", Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
			Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{}},
		}},
	}}}
	key := domain.NewVolumeKey("bench", account.Account, "ASSET0", "")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		aggregate := plan.NewCoverage()
		perOrder := plan.NewCoverage()
		perOrder.Add(dal.SubAttrVolume, key.Bytes())
		aggregate.Merge(perOrder)
		release, err := a.expandAccountLifecycleCoverage(context.Background(), aggregate, []*plan.Coverage{perOrder}, []*raftcmdpb.Order{order})
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}

func BenchmarkAccountLifecycleCoverageNormal1(b *testing.B) {
	benchmarkAccountLifecycleCoverage(b, false, 1)
}
func BenchmarkAccountLifecycleCoverageEphemeral1(b *testing.B) {
	benchmarkAccountLifecycleCoverage(b, true, 1)
}
func BenchmarkAccountLifecycleCoverageEphemeral4(b *testing.B) {
	benchmarkAccountLifecycleCoverage(b, true, 4)
}

func BenchmarkAccountLifecycleCoveragePreExpansion(b *testing.B) {
	key := domain.NewVolumeKey("bench", "account:1", "ASSET0", "")
	b.ReportAllocs()
	for range b.N {
		perOrder := plan.NewCoverage()
		perOrder.Add(dal.SubAttrVolume, key.Bytes())
		aggregate := plan.NewCoverage()
		aggregate.Merge(perOrder)
		benchCoverageSink = aggregate
	}
}
