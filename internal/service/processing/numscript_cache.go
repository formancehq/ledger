package processing

import (
	"container/list"
	"context"
	"sync"

	"github.com/formancehq/numscript"
	"github.com/zeebo/blake3"
	"go.opentelemetry.io/otel/metric"
)

// NumscriptCache stores parsed Numscript programs keyed by their content hash.
// It uses an LRU eviction policy bounded by maxSize to prevent unbounded memory growth.
// The cache is safe for concurrent use via a mutex on the hasher;
// all other state is accessed single-threaded from the FSM apply path.
type NumscriptCache struct {
	cache    map[[32]byte]*list.Element
	order    *list.List
	maxSize  int
	hasher   *blake3.Hasher
	hasherMu sync.Mutex
	hashBuf  [32]byte // pre-allocated buffer for hash output

	// Metrics (nil if not initialized)
	sizeGauge metric.Int64Gauge
}

// lruEntry holds the cache key and value for an LRU list element.
type lruEntry struct {
	hash   [32]byte
	script parsedScript
}

// parsedScript wraps a parsed Numscript program with any parsing errors.
type parsedScript struct {
	program numscript.ParseResult
	err     error
}

// newNumscriptCache creates a new NumscriptCache with the given maximum size.
// If maxSize <= 0, it defaults to 1024.
func newNumscriptCache(maxSize int) *NumscriptCache {
	if maxSize <= 0 {
		maxSize = 1024
	}
	return &NumscriptCache{
		cache:   make(map[[32]byte]*list.Element, maxSize),
		order:   list.New(),
		maxSize: maxSize,
		hasher:  blake3.New(),
	}
}

// computeHash computes the blake3 hash of the script content.
// It reuses the internal hasher to avoid allocations.
func (c *NumscriptCache) computeHash(script string) [32]byte {
	c.hasherMu.Lock()
	defer c.hasherMu.Unlock()

	c.hasher.Reset()
	_, _ = c.hasher.WriteString(script)
	sum := c.hasher.Sum(c.hashBuf[:0])

	var result [32]byte
	copy(result[:], sum)
	return result
}

// GetOrParse retrieves a parsed script from the cache or parses it if not found.
// On cache hit the entry is moved to the front (most recently used).
// On cache miss the script is parsed, added to the front, and the least recently
// used entry is evicted if the cache is at capacity.
func (c *NumscriptCache) GetOrParse(script string) (numscript.ParseResult, error) {
	hash := c.computeHash(script)

	// Try to get from cache
	if elem, ok := c.cache[hash]; ok {
		c.order.MoveToFront(elem)
		entry := elem.Value.(*lruEntry)
		return entry.script.program, entry.script.err
	}

	// Parse the script
	parsed := numscript.Parse(script)

	// Check for parsing errors
	var parseErr error
	if errs := parsed.GetParsingErrors(); len(errs) > 0 {
		parseErr = &ErrNumscriptParse{
			Details: numscript.ParseErrorsToString(errs, parsed.GetSource()),
		}
	}

	// Evict least recently used if at capacity
	if c.order.Len() >= c.maxSize {
		back := c.order.Back()
		if back != nil {
			evicted := c.order.Remove(back).(*lruEntry)
			delete(c.cache, evicted.hash)
		}
	}

	// Add new entry to front
	entry := &lruEntry{
		hash: hash,
		script: parsedScript{
			program: parsed,
			err:     parseErr,
		},
	}
	elem := c.order.PushFront(entry)
	c.cache[hash] = elem

	c.recordSize(int64(c.order.Len()))

	return parsed, parseErr
}

// initCacheMetrics initializes the cache metrics on the NumscriptCache.
func (c *NumscriptCache) initCacheMetrics(m metric.Meter) error {
	size, err := m.Int64Gauge(
		"numscript.cache.size",
		metric.WithDescription("Number of scripts in the Numscript cache"),
	)
	if err != nil {
		return err
	}

	c.sizeGauge = size
	return nil
}

// recordSize records the current cache size.
func (c *NumscriptCache) recordSize(size int64) {
	if c.sizeGauge == nil {
		return
	}
	c.sizeGauge.Record(context.Background(), size)
}
