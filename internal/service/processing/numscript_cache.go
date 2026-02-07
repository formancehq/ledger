package processing

import (
	"context"
	"fmt"
	"sync"

	"github.com/formancehq/numscript"
	"github.com/zeebo/blake3"
	"go.opentelemetry.io/otel/metric"
)

// NumscriptCache stores parsed Numscript programs keyed by their content hash.
// This avoids re-parsing the same script multiple times.
type NumscriptCache struct {
	cache    sync.Map
	size     int64
	sizeMu   sync.Mutex
	hasher   *blake3.Hasher
	hasherMu sync.Mutex
	hashBuf  [32]byte // pre-allocated buffer for hash output

	// Metrics (nil if not initialized)
	sizeGauge metric.Int64Gauge
}

// parsedScript wraps a parsed Numscript program with any parsing errors.
type parsedScript struct {
	program numscript.ParseResult
	err     error
}

// newNumscriptCache creates a new NumscriptCache without metrics (for tests).
func newNumscriptCache() *NumscriptCache {
	return &NumscriptCache{
		hasher: blake3.New(),
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
// The cache key is a blake3 hash of the script content.
func (c *NumscriptCache) GetOrParse(script string) (numscript.ParseResult, error) {
	// Compute hash of the script content
	hash := c.computeHash(script)

	// Try to get from cache
	if cached, ok := c.cache.Load(hash); ok {
		ps := cached.(*parsedScript)
		return ps.program, ps.err
	}

	// Parse the script
	parsed := numscript.Parse(script)

	// Check for parsing errors
	var parseErr error
	if errs := parsed.GetParsingErrors(); len(errs) > 0 {
		parseErr = fmt.Errorf("numscript parse error: %s", numscript.ParseErrorsToString(errs, parsed.GetSource()))
	}

	// Store in cache (even if there are errors, to avoid re-parsing invalid scripts)
	c.cache.Store(hash, &parsedScript{
		program: parsed,
		err:     parseErr,
	})

	// Update size
	c.sizeMu.Lock()
	c.size++
	c.recordSize(c.size)
	c.sizeMu.Unlock()

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
