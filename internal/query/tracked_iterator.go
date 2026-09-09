package query

import (
	"time"

	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// TrackedIterator wraps an Iterator and records per-iterator stats (call
// counters, inclusive wall-clock duration, emitted-rows counter) into the
// associated IteratorStats. Overhead is a single time.Now()/time.Since pair
// plus two int64 increments per call.
//
// Profiling is direction-agnostic: it counts calls and rows, neither of which
// depends on which way the iterator walks, so one implementation covers both
// directions and a reverse plan renders in the iterator tree exactly like an
// ascending one (EN-1966).
type TrackedIterator[D readstore.Direction] struct {
	inner readstore.Iterator[D]
	stats *IteratorStats
}

// NewTrackedIterator wraps an ascending iterator with profiling counters.
func NewTrackedIterator(inner readstore.EntityIterator, stats *IteratorStats) *TrackedIterator[readstore.Asc] {
	return &TrackedIterator[readstore.Asc]{inner: inner, stats: stats}
}

func (t *TrackedIterator[D]) Next() bool {
	start := time.Now()
	ok := t.inner.Next()
	t.stats.Duration += time.Since(start)
	t.stats.NextCalls++

	if ok {
		t.stats.ItemsEmitted++
	}

	return ok
}

func (t *TrackedIterator[D]) Current() []byte {
	return t.inner.Current()
}

func (t *TrackedIterator[D]) Seek(target []byte) bool {
	start := time.Now()
	ok := t.inner.Seek(target)
	t.stats.Duration += time.Since(start)
	t.stats.SeekCalls++

	return ok
}

func (t *TrackedIterator[D]) Err() error {
	return t.inner.Err()
}

func (t *TrackedIterator[D]) Close() {
	t.inner.Close()
}

// Direction is the compile-time direction witness; see readstore.Iterator.
func (t *TrackedIterator[D]) Direction() (d D) { return }

var _ readstore.EntityIterator = (*TrackedIterator[readstore.Asc])(nil)
