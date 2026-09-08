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
// Append at a fixed retained-window size, isolating three contiguous fast-path
// behaviors plus the retained truncation-copy baseline:
//
//   - contiguous-amortized: the unrestricted growing-window loop from EN-1964
//     (capacity growth is amortized over the expanding window);
//   - contiguous-spare: the retained window stays fixed and always has batch
//     spare slots, so every append grows in place with no capacity growth;
//   - contiguous-full: the retained window stays fixed with len == cap, so
//     every append reallocates and copies the full retained prefix;
//   - overlap: the retained truncation-copy baseline the ticket requires to
//     stay.
//
// All modes perform the same fsync-backed etcd WAL Save, which is O(batch) and
// identical across modes, so the B/op and allocs/op differences between the
// spare and full cases attribute the fast-path win to capacity reuse versus
// reallocation at a controlled retained size. These are local diagnostic
// numbers, not an end-to-end Raft TPS forecast.
func BenchmarkAppendCacheMerge(b *testing.B) {
	for _, retained := range []int{1_000, 10_000, 100_000} {
		for _, batch := range []int{1, 64, 512} {
			b.Run(fmt.Sprintf("contiguous-amortized/retained=%d/batch=%d", retained, batch), func(b *testing.B) {
				benchmarkContiguousAmortizedAppend(b, retained, batch)
			})
			b.Run(fmt.Sprintf("contiguous-spare/retained=%d/batch=%d", retained, batch), func(b *testing.B) {
				benchmarkContiguousSpareAppend(b, retained, batch)
			})
			b.Run(fmt.Sprintf("contiguous-full/retained=%d/batch=%d", retained, batch), func(b *testing.B) {
				benchmarkContiguousFullAppend(b, retained, batch)
			})
			b.Run(fmt.Sprintf("overlap/retained=%d/batch=%d", retained, batch), func(b *testing.B) {
				benchmarkOverlapAppend(b, retained, batch)
			})
		}
	}
}

func benchmarkContiguousAmortizedAppend(b *testing.B, retained, batch int) {
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

// benchmarkContiguousSpareAppend isolates capacity reuse: the retained window
// stays fixed at `retained` entries and always carries exactly `batch` spare
// slots, so every measured append grows in place without reallocating the
// backing array. Reported allocation here is the per-batch incoming slice and
// fsync path, not an O(retained) copy.
func benchmarkContiguousSpareAppend(b *testing.B, retained, batch int) {
	b.Helper()

	w := newBenchWAL(b)

	if err := w.Append(hs(1, 1, uint64(retained)), makeEntries(1, uint64(retained))); err != nil {
		b.Fatalf("seeding retained window: %v", err)
	}

	// Rebuild the cache with exactly batch spare slots (len == retained,
	// cap == retained+batch) so the contiguous fast path absorbs every
	// measured append without triggering capacity growth.
	spare := make([]*raftpb.Entry, retained, retained+batch)
	copy(spare, w.entries)
	w.entries = spare

	start := uint64(retained + 1)
	end := start + uint64(batch) - 1

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		if err := w.Append(hs(1, 1, end), makeEntries(start, end)); err != nil {
			b.Fatalf("spare-capacity contiguous append: %v", err)
		}
		// Roll the cache back to the fixed retained window so the next
		// iteration reuses the same spare slots and the retained size stays
		// controlled.
		w.entries = w.entries[:retained]
	}
}

// benchmarkContiguousFullAppend isolates reallocation: the retained window
// stays fixed at `retained` entries with len == cap, so every measured append
// must grow the backing array and copy the full retained prefix. Comparing this
// with the spare-capacity case isolates the O(retained) copy the fast path
// eliminates, at a controlled retained size.
func benchmarkContiguousFullAppend(b *testing.B, retained, batch int) {
	b.Helper()

	w := newBenchWAL(b)

	if err := w.Append(hs(1, 1, uint64(retained)), makeEntries(1, uint64(retained))); err != nil {
		b.Fatalf("seeding retained window: %v", err)
	}

	// Force len == cap so every append must reallocate a new backing array.
	w.entries = w.entries[:retained:retained]

	start := uint64(retained + 1)
	end := start + uint64(batch) - 1

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		if err := w.Append(hs(1, 1, end), makeEntries(start, end)); err != nil {
			b.Fatalf("full-capacity contiguous append: %v", err)
		}
		// Restore the full (len == cap) window so the next append must grow
		// the backing array again.
		w.entries = w.entries[:retained:retained]
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
