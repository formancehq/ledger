package query

import "github.com/formancehq/ledger-v3-poc/internal/storage/readstore"

// TrackedIterator wraps an EntityIterator and counts Next/SeekGE calls
// in the associated IteratorStats. The overhead is a single int64 increment
// per call, which is negligible compared to bbolt cursor operations.
type TrackedIterator struct {
	inner readstore.EntityIterator
	stats *IteratorStats
}

// NewTrackedIterator wraps an iterator with profiling counters.
func NewTrackedIterator(inner readstore.EntityIterator, stats *IteratorStats) *TrackedIterator {
	return &TrackedIterator{inner: inner, stats: stats}
}

func (t *TrackedIterator) Next() bool {
	t.stats.NextCalls++

	return t.inner.Next()
}

func (t *TrackedIterator) Current() []byte {
	return t.inner.Current()
}

func (t *TrackedIterator) SeekGE(target []byte) bool {
	t.stats.SeekCalls++

	return t.inner.SeekGE(target)
}

func (t *TrackedIterator) Close() {
	t.inner.Close()
}

var _ readstore.EntityIterator = (*TrackedIterator)(nil)
