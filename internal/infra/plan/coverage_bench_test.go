package plan

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// benchOrder describes a synthetic ledger-scoped Order: a ledger name plus
// N posting rows. Every posting adds two volume keys (source + destination)
// to per-order Coverage, plus the shared ledger + boundary keys — the
// shape a real CreateTransaction produces at admission.
type benchOrder struct {
	ledger   string
	postings int
}

// buildPerOrderCoverage mirrors what extractLedgerScopedNeeds does for a
// CreateTransaction: add the ledger + boundary + N*(source volume,
// destination volume) keys onto an order-scoped Coverage.
func buildPerOrderCoverage(o benchOrder, orderIdx int) *Coverage {
	c := NewCoverage()

	ledgerBytes := domain.LedgerKey{Name: o.ledger}.Bytes()
	c.Add(dal.SubAttrLedger, ledgerBytes)
	c.Add(dal.SubAttrBoundary, ledgerBytes)

	for i := 0; i < o.postings; i++ {
		src := fmt.Sprintf("bench:src:%d:%d", orderIdx, i)
		dst := fmt.Sprintf("bench:dst:%d:%d", orderIdx, i)

		c.Add(dal.SubAttrVolume, domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: o.ledger, Account: src},
			Asset:      "USD/2",
		}.Bytes())
		c.Add(dal.SubAttrVolume, domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: o.ledger, Account: dst},
			Asset:      "USD/2",
		}.Bytes())
	}

	return c
}

// benchmarkPipeline exercises the coverage pipeline shape used by admission:
//
//	extractPreloadNeeds → Build → applyBits
//
// It avoids proto marshalling and Raft altogether; the point is to measure
// the allocation cost of the coverage construction path in isolation.
func benchmarkPipeline(b *testing.B, orders int, postingsPerOrder int) {
	b.Helper()

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("bench")

	store, err := dal.NewStore(b.TempDir(), logger, meter, dal.DefaultConfig())
	if err != nil {
		b.Fatalf("dal.NewStore: %v", err)
	}
	b.Cleanup(func() { _ = store.Close() })

	attrs := attributes.New()
	c, err := cache.New(1_000_000, meter)
	if err != nil {
		b.Fatalf("cache.New: %v", err)
	}

	tracker := node.NewIndexTracker(1)
	p := NewBuilder(tracker, c, attrs, store, nil, logger, 0)

	// Pre-seed every key into the cache so buildPreloadsAt takes the
	// CacheHit → coverage-only branch (no Pebble reads, no goroutine
	// fan-out) — the shared hot path we want to characterize.
	specs := make([]benchOrder, orders)
	for i := range specs {
		specs[i] = benchOrder{
			ledger:   "bench-ledger",
			postings: postingsPerOrder,
		}
	}

	ops := make([]WriteOperation, orders)
	targets := make([][]byte, orders)

	// Warm the cache with the exact key set every iteration exercises.
	for orderIdx, spec := range specs {
		ledgerID, _ := attributes.MakeKey(domain.LedgerKey{Name: spec.ledger}.Bytes())
		c.Ledgers.Put(ledgerID, attributes.Entry[*commonpb.LedgerInfo]{Data: &commonpb.LedgerInfo{}})
		c.Boundaries.Put(ledgerID, attributes.Entry[*raftcmdpb.LedgerBoundaries]{Data: &raftcmdpb.LedgerBoundaries{}})

		for i := 0; i < spec.postings; i++ {
			src := fmt.Sprintf("bench:src:%d:%d", orderIdx, i)
			dst := fmt.Sprintf("bench:dst:%d:%d", orderIdx, i)

			for _, acct := range []string{src, dst} {
				id, _ := attributes.MakeKey(domain.VolumeKey{
					AccountKey: domain.AccountKey{LedgerName: spec.ledger, Account: acct},
					Asset:      "USD/2",
				}.Bytes())
				c.Volumes.Put(id, attributes.Entry[*raftcmdpb.VolumePair]{Data: &raftcmdpb.VolumePair{}})
			}
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		// Step 1: extractPreloadNeeds-equivalent — per-order Coverage +
		// aggregate.
		aggregate := NewCoverage()
		for orderIdx, spec := range specs {
			c := buildPerOrderCoverage(spec, orderIdx)

			idx := orderIdx
			ops[orderIdx] = WriteOperation{
				Coverage: c,
				SetCoverage: func(bits []byte) {
					targets[idx] = bits
				},
			}
			aggregate.Merge(c)
		}
		_ = aggregate

		// Step 2: Build → resolves every attribute cache in parallel.
		build, err := p.Build(ops)
		if err != nil {
			b.Fatalf("Build: %v", err)
		}

		// Step 3: applyBits — assign the per-operation coverage bitset.
		build.applyBits(&raftcmdpb.Proposal{}, build.ExecutionPlan.GetAttributes())

		build.ReleaseLoaders()
	}
}

func BenchmarkAdmissionCoverage_10Orders_1Posting(b *testing.B) {
	benchmarkPipeline(b, 10, 1)
}

func BenchmarkAdmissionCoverage_50Orders_2Postings(b *testing.B) {
	benchmarkPipeline(b, 50, 2)
}

func BenchmarkAdmissionCoverage_100Orders_5Postings(b *testing.B) {
	benchmarkPipeline(b, 100, 5)
}
