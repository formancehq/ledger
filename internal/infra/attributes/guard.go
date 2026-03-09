package attributes

import (
	"math"
	"sync"
)

// IndexGuard tracks the minimum raft index used by active readers.
// The attribute cleaner uses this to avoid deleting Pebble entries
// that concurrent queries still need.
type IndexGuard struct {
	mu     sync.Mutex
	active []uint64
}

// NewIndexGuard creates a new IndexGuard.
func NewIndexGuard() *IndexGuard {
	return &IndexGuard{}
}

// Hold registers a reader at the given raft index. The returned function
// must be called when the reader is done (typically via defer).
// Call Hold AFTER reading the raft index from bbolt, BEFORE scanning Pebble.
func (g *IndexGuard) Hold(raftIndex uint64) func() {
	g.mu.Lock()
	g.active = append(g.active, raftIndex)
	g.mu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()

			for i, v := range g.active {
				if v == raftIndex {
					// Swap-remove: replace with last element and shrink.
					g.active[i] = g.active[len(g.active)-1]
					g.active = g.active[:len(g.active)-1]

					return
				}
			}
		})
	}
}

// Min returns the minimum raft index among active readers,
// or math.MaxUint64 if there are no active readers.
func (g *IndexGuard) Min() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	result := uint64(math.MaxUint64)

	for _, v := range g.active {
		if v < result {
			result = v
		}
	}

	return result
}
