package query_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// countingReader wraps a dal.PebbleReader and counts physical Pebble iterator
// opens (NewIter) so tests and benchmarks can assert the physical scan count
// independently of the compiled-iterator tree recorded in query.QueryProfile.
// profile.Root tracks only compiled filters; a fast path that silently opened
// extra volume iterators would leave it nil while still doing per-account
// scans, which is exactly what this counter pins down.
type countingReader struct {
	inner dal.PebbleReader
	iters int
}

var _ dal.PebbleReader = (*countingReader)(nil)

func (c *countingReader) Get(key []byte) ([]byte, io.Closer, error) {
	return c.inner.Get(key)
}

func (c *countingReader) NewIter(o *pebble.IterOptions) (*pebble.Iterator, error) {
	c.iters++

	return c.inner.NewIter(o)
}

// The unfiltered fast path delegates the whole aggregation to
// AggregateAllVolumes, which must read every ledger volume through exactly one
// physical iterator — not one per account. This is the physical-scan-count
// assertion EN-1970 requires; it would catch the fast path opening extra,
// untracked iterators that profile.Root == nil cannot see.
func TestAggregateAllVolumes_OpensSinglePhysicalIterator(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")

	attrs := attributes.New()
	seedVolumes(t, store, attrs, "l",
		seededVolume{account: "a", asset: "USD/2", input: 100},
		seededVolume{account: "b", asset: "EUR/4", input: 20, output: 3},
		seededVolume{account: "c", asset: "USD/4", color: "RED", input: 4},
		seededVolume{account: "d", asset: "GOLD", input: 1, output: 2},
	)

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	cr := &countingReader{inner: handle}
	result, err := query.AggregateAllVolumes(cr, attrs.Volume, "l", query.AggregateOptions{})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, cr.iters,
		"unfiltered aggregation must read all volumes through a single physical iterator")
}

// The per-account strategy AggregateAllVolumes replaces opens one physical
// volume iterator per matched account. Pinning that N here documents exactly
// what the fast path avoids and guards the counting reader against silently
// passing both sides.
func TestAggregateVolumes_OpensOneIteratorPerAccount(t *testing.T) {
	t.Parallel()

	const accounts = 6

	seed := make([]seededVolume, 0, accounts)
	for i := range accounts {
		seed = append(seed, seededVolume{
			account: fmt.Sprintf("acct:%d", i),
			asset:   "USD/2",
			input:   uint64(i),
			output:  1,
		})
	}

	store := newTestStore(t)
	registerLedger(t, store, "l")

	attrs := attributes.New()
	seedVolumes(t, store, attrs, "l", seed...)

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// The account iterator is built against the real handle so only the
	// per-account volume iterators are attributed to the counting reader.
	accountIter, err := readstore.NewPebbleAccountIterator(handle, "l")
	require.NoError(t, err)
	defer accountIter.Close()

	cr := &countingReader{inner: handle}
	result, err := query.AggregateVolumes(cr, attrs.Volume, "l", accountIter, query.AggregateOptions{})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, accounts, cr.iters,
		"the per-account strategy opens one physical volume iterator per account")
}

type aggBenchFixture struct {
	store  *dal.Store
	rs     *readstore.Store
	attrs  *attributes.Attributes
	handle *dal.ReadHandle
	req    *servicepb.ExecutePreparedQueryRequest
}

// newAggBenchFixture seeds a ledger with `accounts` accounts, each holding a
// varied number of volume rows (1..3) across varied asset precisions, so the
// benchmark mirrors the "varied volumes per account" distribution EN-1970
// asks to measure. Seeding happens before the sub-benchmark timer starts.
func newAggBenchFixture(b *testing.B, accounts int) *aggBenchFixture {
	b.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("bench")

	store, err := dal.NewStore(b.TempDir(), logger, meter, dal.DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })

	rs, err := readstore.New(b.TempDir(), logging.NopZap(), readstore.DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = rs.Close() })

	attrs := attributes.New()

	batch := store.OpenWriteSession()
	if err := state.SaveLedger(batch, "l", &commonpb.LedgerInfo{
		Name:      "l",
		CreatedAt: commonpb.NewTimestamp(libtime.Now()),
	}); err != nil {
		b.Fatal(err)
	}
	if _, err := attrs.PreparedQuery.Set(batch, domain.PreparedQueryKey{LedgerName: "l", Name: "q"}.Bytes(), &commonpb.PreparedQuery{
		Name:   "q",
		Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		Filter: nil,
	}); err != nil {
		b.Fatal(err)
	}

	for i := range accounts {
		account := fmt.Sprintf("acct:%d", i)
		n := (i % 3) + 1 // 1..3 volume rows per account
		for j := range n {
			asset := fmt.Sprintf("USD/%d", (i+j)%3+2) // precision varies 2..4
			if _, err := attrs.Volume.Set(batch, domain.NewVolumeKey("l", account, asset, "").Bytes(), &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(uint64(i + j)),
				Output: commonpb.NewUint256FromUint64(uint64(j)),
			}); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := batch.Commit(); err != nil {
		b.Fatal(err)
	}

	handle, err := store.NewReadHandle()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = handle.Close() })

	return &aggBenchFixture{
		store:  store,
		rs:     rs,
		attrs:  attrs,
		handle: handle,
		req: &servicepb.ExecutePreparedQueryRequest{
			Ledger:    "l",
			QueryName: "q",
			Mode:      commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES,
		},
	}
}

// BenchmarkAggregate_ScanCount reports the physical iterator count, latency and
// allocations for the shared unfiltered fast path versus the per-account
// strategy it replaces, at the 1k and 100k account scales EN-1970 requires.
//
// Run with -benchmem to also collect B/op and allocs/op. The iters/op metric
// is the physical scan count: 1 for the fast path, accounts+1 for the
// per-account strategy.
func BenchmarkAggregate_ScanCount(b *testing.B) {
	for _, accounts := range []int{1000, 100000} {
		fx := newAggBenchFixture(b, accounts)

		b.Run(fmt.Sprintf("fast_path/accounts=%d", accounts), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				cr := &countingReader{inner: fx.handle}
				res, err := query.AggregateAllVolumes(cr, fx.attrs.Volume, "l", query.AggregateOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(res.GetVolumes()) == 0 {
					b.Fatal("expected aggregated volumes")
				}
				b.ReportMetric(float64(cr.iters), "iters/op")
			}
		})

		b.Run(fmt.Sprintf("per_account/accounts=%d", accounts), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				accountIter, err := readstore.NewPebbleAccountIterator(fx.handle, "l")
				if err != nil {
					b.Fatal(err)
				}

				cr := &countingReader{inner: fx.handle}
				res, err := query.AggregateVolumes(cr, fx.attrs.Volume, "l", accountIter, query.AggregateOptions{})
				accountIter.Close()
				if err != nil {
					b.Fatal(err)
				}
				if len(res.GetVolumes()) == 0 {
					b.Fatal("expected aggregated volumes")
				}
				b.ReportMetric(float64(cr.iters), "iters/op")
			}
		})
	}
}

// BenchmarkExecute_UnfilteredAggregate measures the end-to-end latency of the
// prepared nil-filter aggregate through the executor, i.e. the exact path the
// PR adds the fast-path branch to, at both required account scales.
func BenchmarkExecute_UnfilteredAggregate(b *testing.B) {
	for _, accounts := range []int{1000, 100000} {
		fx := newAggBenchFixture(b, accounts)

		b.Run(fmt.Sprintf("accounts=%d", accounts), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				resp, err := query.Execute(context.Background(), fx.rs, fx.store, fx.attrs.Volume, fx.attrs.PreparedQuery, fx.attrs.Index, fx.req, &query.QueryProfile{}, nil)
				if err != nil {
					b.Fatal(err)
				}
				if resp.GetAggregate() == nil {
					b.Fatal("expected aggregate response")
				}
			}
		})
	}
}
