package wal

import (
	"fmt"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// newBenchWAL creates a DefaultWAL on a benchmark-scoped temp directory.
func newBenchWAL(b *testing.B) *DefaultWAL {
	b.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("bench")

	w, err := New(b.TempDir(), logger, meter)
	if err != nil {
		b.Fatalf("creating benchmark WAL: %v", err)
	}
	b.Cleanup(func() { _ = w.Close() })

	return w
}

// makeEntries builds the contiguous entry range [start, end].
func makeEntries(start, end uint64) []*raftpb.Entry {
	entries := make([]*raftpb.Entry, 0, end-start+1)
	for i := start; i <= end; i++ {
		entries = append(entries, ent(i, 1, []byte("x")))
	}

	return entries
}

// BenchmarkAppendCacheMerge measures the in-memory entry-cache merge inside
// Append. The contiguous mode hits the EN-1964 fast path (amortized contiguous
// growth); the overlap mode is the retained truncation-copy baseline the ticket
// requires to stay. Both modes perform the same fsync-backed etcd WAL Save,
// which is O(batch) and identical across modes, so the B/op and allocs/op
// difference at growing retained sizes is the full-prefix copy the fast path
// eliminates. These are local diagnostic numbers, not an end-to-end Raft TPS
// forecast.
func BenchmarkAppendCacheMerge(b *testing.B) {
	for _, retained := range []int{1_000, 10_000, 100_000} {
		for _, batch := range []int{1, 64, 512} {
			b.Run(fmt.Sprintf("contiguous/retained=%d/batch=%d", retained, batch), func(b *testing.B) {
				benchmarkContiguousAppend(b, retained, batch)
			})
			b.Run(fmt.Sprintf("overlap/retained=%d/batch=%d", retained, batch), func(b *testing.B) {
				benchmarkOverlapAppend(b, retained, batch)
			})
		}
	}
}

func benchmarkContiguousAppend(b *testing.B, retained, batch int) {
	b.Helper()

	w := newBenchWAL(b)

	// Seed the retained window in a single contiguous Append (one fsync), then
	// measure repeated contiguous growth. The first measured iterations cross a
	// geometric capacity growth boundary; subsequent iterations reuse spare
	// capacity, so the reported figure is the amortized append cost including
	// capacity growth rather than an O(retained) copy on every call.
	if err := w.Append(hs(1, 1, uint64(retained)), makeEntries(1, uint64(retained))); err != nil {
		b.Fatalf("seeding retained window: %v", err)
	}

	next := uint64(retained + 1)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		start := next
		end := next + uint64(batch) - 1
		if err := w.Append(hs(1, 1, end), makeEntries(start, end)); err != nil {
			b.Fatalf("contiguous append: %v", err)
		}
		next = end + 1
	}
}

func benchmarkOverlapAppend(b *testing.B, retained, batch int) {
	b.Helper()

	w := newBenchWAL(b)

	if err := w.Append(hs(1, 1, uint64(retained)), makeEntries(1, uint64(retained))); err != nil {
		b.Fatalf("seeding retained window: %v", err)
	}

	if batch > retained {
		b.Skip("batch larger than retained window; cannot form an overlap")
	}

	// Overlap the tail of the window so every iteration truncates and copies
	// the retained prefix — the O(retained) merge the fast path avoids.
	start := uint64(retained - batch + 1)
	end := uint64(retained)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		if err := w.Append(hs(1, 1, end), makeEntries(start, end)); err != nil {
			b.Fatalf("overlap append: %v", err)
		}
	}
}
