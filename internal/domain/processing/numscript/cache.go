package numscript

import (
	"container/list"
	"context"
	"sync"

	"github.com/zeebo/blake3"
	"go.opentelemetry.io/otel/metric"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// NumscriptCache stores parsed Numscript programs keyed by their content hash.
// It uses an LRU eviction policy bounded by maxSize to prevent unbounded memory growth.
// Thread-safe: an RWMutex allows concurrent cache hits without contention.
// LRU reordering is approximate — read hits do not call MoveToFront to avoid
// write-locking on the hot path.
type NumscriptCache struct {
	mu      sync.RWMutex
	cache   map[[32]byte]*list.Element
	order   *list.List
	maxSize int

	// Metrics (nil if not initialized)
	sizeGauge metric.Int64Gauge
}

// lruEntry holds the cache key and value for an LRU list element.
type lruEntry struct {
	hash   [32]byte
	script parsedScript
}

// parsedScript wraps a parsed Numscript program with any parsing errors, plus
// lazily-populated bytecode-compilation artifacts (the VM execution path). The
// vars encoder and the machine are only produced on the first GetOrCompileVM
// call for a given script and memoized for reuse, mirroring how the parsed AST
// is memoized for the interpreter path.
type parsedScript struct {
	program numscriptlib.ParseResult
	err     domain.Describable

	compiledDone bool
	encoder      numscriptlib.VarsEncoder
	compiledErr  domain.Describable
	// vm is a reusable machine bound to the compiled program (nil when
	// compilation failed). It holds mutable per-run register banks + runstate
	// that numscriptlib.ExecVm resets on every call, so it may be reused across
	// executions — but MUST NOT be executed concurrently (see GetOrCompileVM).
	vm *numscriptlib.Vm
}

// NewNumscriptCache creates a new NumscriptCache with the given maximum size.
// If maxSize <= 0, it defaults to 1024.
func NewNumscriptCache(maxSize int) *NumscriptCache {
	if maxSize <= 0 {
		maxSize = 1024
	}

	return &NumscriptCache{
		cache:   make(map[[32]byte]*list.Element, maxSize),
		order:   list.New(),
		maxSize: maxSize,
	}
}

// hashScript computes the blake3 hash of the script content.
// Lock-free: allocates a hasher per call (blake3.New is cheap).
func HashScript(script string) [32]byte {
	h := blake3.New()
	_, _ = h.WriteString(script)

	var result [32]byte

	h.Sum(result[:0])

	return result
}

// GetOrParse retrieves a parsed script from the cache or parses it if not found.
// On cache hit the lookup uses a read lock for zero contention under concurrent reads.
// On cache miss the script is parsed outside the lock, then inserted with a write lock.
// LRU ordering is approximate: read hits do not reorder to avoid write contention.
func (c *NumscriptCache) GetOrParse(script string) (numscriptlib.ParseResult, domain.Describable) {
	hash := HashScript(script)

	// Fast path: read lock for cache hits (no contention between readers).
	c.mu.RLock()
	if elem, ok := c.cache[hash]; ok {
		entry, _ := elem.Value.(*lruEntry)
		c.mu.RUnlock()

		return entry.script.program, entry.script.err
	}

	c.mu.RUnlock()

	// Parse the script outside the lock — this is the expensive operation.
	parsed := numscriptlib.Parse(script)

	var parseErr domain.Describable
	if errs := parsed.GetParsingErrors(); len(errs) > 0 {
		parseErr = &domain.ErrNumscriptParse{
			Details: numscriptlib.ParseErrorsToString(errs, parsed.GetSource()),
		}
	}

	// Acquire write lock to insert into cache.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check: another goroutine may have inserted this entry while we parsed.
	if elem, ok := c.cache[hash]; ok {
		entry, _ := elem.Value.(*lruEntry)

		return entry.script.program, entry.script.err
	}

	// Evict least recently used if at capacity
	if c.order.Len() >= c.maxSize {
		back := c.order.Back()
		if back != nil {
			evicted, _ := c.order.Remove(back).(*lruEntry)
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

// GetOrCompileVM returns the vars encoder and a reusable bytecode VM for a
// script, compiling the program and building the machine once and memoizing
// both. Compilation subsumes parsing, so a parse error is surfaced here too.
// The compile + VM build happen outside the write lock; the result is stored on
// the shared cache entry so subsequent runs of the same script skip both.
//
// The returned *Vm is SHARED and holds mutable per-run state (register banks +
// runstate); numscriptlib.ExecVm resets it on every call, so it may be reused
// across executions but MUST NOT be executed concurrently. In the ledger this
// is safe because numscript VM execution only happens on the single-threaded
// FSM apply path — admission dependency discovery uses the interpreter, not the
// VM. Reuse avoids reallocating the machine's register banks
// ([256]big.Int/big.Rat/monetary/string — the dominant per-transaction
// allocation) on every call.
func (c *NumscriptCache) GetOrCompileVM(script string) (numscriptlib.VarsEncoder, *numscriptlib.Vm, domain.Describable) {
	// Ensure the script is parsed and cached first; a parse failure is terminal.
	parsed, parseErr := c.GetOrParse(script)
	if parseErr != nil {
		return numscriptlib.VarsEncoder{}, nil, parseErr
	}

	hash := HashScript(script)

	// Fast path: encoder + machine already memoized on the entry.
	c.mu.RLock()
	if elem, ok := c.cache[hash]; ok {
		if entry, _ := elem.Value.(*lruEntry); entry.script.compiledDone {
			enc, machine, cErr := entry.script.encoder, entry.script.vm, entry.script.compiledErr
			c.mu.RUnlock()

			return enc, machine, cErr
		}
	}
	c.mu.RUnlock()

	// Compile + build the machine outside the lock — the expensive operations.
	enc, prog, err := parsed.Compile()

	var (
		compileErr domain.Describable
		machine    *numscriptlib.Vm
	)

	if err != nil {
		compileErr = &domain.ErrNumscriptParse{Details: err.Error()}
	} else {
		machine = numscriptlib.NewVm(prog)
	}

	// Store the artifacts on the shared entry (if it still exists).
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[hash]; ok {
		entry, _ := elem.Value.(*lruEntry)
		if !entry.script.compiledDone {
			entry.script.encoder = enc
			entry.script.compiledErr = compileErr
			entry.script.vm = machine
			entry.script.compiledDone = true
		}

		return entry.script.encoder, entry.script.vm, entry.script.compiledErr
	}

	// The entry was evicted between parse and compile; return the fresh result.
	return enc, machine, compileErr
}

// InitCacheMetrics initializes the cache metrics on the NumscriptCache.
func (c *NumscriptCache) InitCacheMetrics(m metric.Meter) error {
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
