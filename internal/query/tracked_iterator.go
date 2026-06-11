package query

import (
	"time"

	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// TrackedIterator wraps an EntityIterator and records per-iterator stats
// (call counters, inclusive wall-clock duration, emitted-rows counter) into
// the associated IteratorStats. Overhead is a single time.Now()/time.Since
// pair plus two int64 increments per call.
type TrackedIterator struct {
	inner readstore.EntityIterator
	stats *IteratorStats
}

// NewTrackedIterator wraps an iterator with profiling counters.
func NewTrackedIterator(inner readstore.EntityIterator, stats *IteratorStats) *TrackedIterator {
	return &TrackedIterator{inner: inner, stats: stats}
}

func (t *TrackedIterator) Next() bool {
	start := time.Now()
	ok := t.inner.Next()
	t.stats.Duration += time.Since(start)
	t.stats.NextCalls++

	if ok {
		t.stats.ItemsEmitted++
	}

	return ok
}

func (t *TrackedIterator) Current() []byte {
	return t.inner.Current()
}

func (t *TrackedIterator) SeekGE(target []byte) bool {
	start := time.Now()
	ok := t.inner.SeekGE(target)
	t.stats.Duration += time.Since(start)
	t.stats.SeekCalls++

	return ok
}

func (t *TrackedIterator) Err() error {
	return t.inner.Err()
}

func (t *TrackedIterator) Close() {
	t.inner.Close()
}

var _ readstore.EntityIterator = (*TrackedIterator)(nil)
