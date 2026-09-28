package numscript

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/zeebo/blake3"
	"go.opentelemetry.io/otel/metric"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// NumscriptCache stores parsed Numscript programs keyed by their content hash,
// and decoded+verified VM artifacts — each as one warm VM instance — keyed by
// the artifact bytes' hash. Both sides use an LRU eviction policy bounded by
// maxSize to prevent unbounded memory growth.
// Thread-safe: an RWMutex allows concurrent cache hits without contention.
// LRU reordering is approximate — read hits do not call MoveToFront to avoid
// write-locking on the hot path.
type NumscriptCache struct {
	mu      sync.RWMutex
	cache   map[[32]byte]*list.Element
	order   *list.List
	maxSize int

	compiledMu    sync.RWMutex
	compiledCache map[[32]byte]*list.Element
	compiledOrder *list.List

	// Metrics (nil if not initialized)
	sizeGauge metric.Int64Gauge
}

// lruEntry holds the cache key and value for an LRU list element. The
// admission-side compile of the script hangs off the same entry (compileOnce /
// compiled): it is computed at most once per cached script, shared by every
// order carrying that script, and evicted together with the parse. See
// compileParsed.
type lruEntry struct {
	hash   [32]byte
	script parsedScript

	compileOnce sync.Once
	compiled    *compiledProgram
}

// compiledProgram is the script-dependent half of an admission compile: the
// encoded bytecode and the encoder that binds an order's vars to the program's
// variable layout. Neither depends on any order's values, so one instance
// serves every order of the script. program is shared and never mutated —
// compileScript hands each order its own copy.
type compiledProgram struct {
	varsEncoder numscriptlib.VarsEncoder
	program     []byte
}

// parsedScript wraps a parsed Numscript program with any parsing errors.
type parsedScript struct {
	program numscriptlib.ParseResult
	err     domain.SerializableError
}

// compiledLruEntry holds one decoded, verified VM artifact as a single warm VM
// instance (which embeds the program). Every apply of this artifact reuses the
// instance: a "dirty" one is always safe — registers are write-before-read
// (verified), the runstate resets on each exec, and the program is immutable —
// even after a recovered panic. The one hard rule is that the same instance
// must never execute concurrently; the FSM apply path is single-threaded (see
// RequestProcessor), which guarantees it.
//
// nStr/nInt are the vars pool sizes the verification ran against; they are a
// property of the program's own variable layout (the encoder appends one slot
// per declaration), so every order carrying this artifact presents the same
// sizes and the verification outcome is reusable.
type compiledLruEntry struct {
	hash       [32]byte
	vm         *numscriptlib.Vm
	nStr, nInt int
}

// NewNumscriptCache creates a new NumscriptCache with the given maximum size.
// If maxSize <= 0, it defaults to 1024.
func NewNumscriptCache(maxSize int) *NumscriptCache {
	if maxSize <= 0 {
		maxSize = 1024
	}

	return &NumscriptCache{
		cache:         make(map[[32]byte]*list.Element, maxSize),
		order:         list.New(),
		maxSize:       maxSize,
		compiledCache: make(map[[32]byte]*list.Element, maxSize),
		compiledOrder: list.New(),
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
func (c *NumscriptCache) GetOrParse(script string) (numscriptlib.ParseResult, domain.SerializableError) {
	entry := c.getOrParseEntry(script)

	return entry.script.program, entry.script.err
}

// getOrParseEntry is GetOrParse returning the cache entry itself, for callers
// in this package that also need what hangs off it: the script's hash and its
// once-per-script compile.
// On cache hit the lookup uses a read lock for zero contention under concurrent reads.
// On cache miss the script is parsed outside the lock, then inserted with a write lock.
// LRU ordering is approximate: read hits do not reorder to avoid write contention.
func (c *NumscriptCache) getOrParseEntry(script string) *lruEntry {
	hash := HashScript(script)

	// Fast path: read lock for cache hits (no contention between readers).
	c.mu.RLock()
	if elem, ok := c.cache[hash]; ok {
		entry, _ := elem.Value.(*lruEntry)
		c.mu.RUnlock()

		return entry
	}

	c.mu.RUnlock()

	// Parse the script outside the lock — this is the expensive operation.
	parsed := numscriptlib.Parse(script)

	var parseErr domain.SerializableError
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

		return entry
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

	return entry
}

// compileParsed compiles the entry's script at most once and shares the result
// with every later caller. nil means the compiler cannot lower the script (or
// panicked on it) — as stable a property of the text as a parse error, and
// cached the same way so an uncompilable script does not re-run the compiler
// on every order. Only the script-dependent half is computed here; binding an
// order's vars (VarsEncoder.Encode) is per-order and stays with the caller.
// The compile runs outside the cache locks; concurrent callers for the same
// script block on the Once and share the single result.
func (e *lruEntry) compileParsed() *compiledProgram {
	e.compileOnce.Do(func() {
		defer func() {
			if recover() != nil {
				e.compiled = nil
			}
		}()

		if e.script.err != nil {
			return
		}

		varsEncoder, program, err := e.script.program.Compile()
		if err != nil {
			return
		}

		e.compiled = &compiledProgram{varsEncoder: varsEncoder, program: program.Encode()}
	})

	return e.compiled
}

// getOrDecodeCompiled returns the cache entry holding the decoded, verified VM
// program — as one warm VM instance — for an admission-compiled artifact,
// decoding and verifying on the first sighting and serving every later apply
// from cache. The verifier is a whole-program static pass far more expensive
// than execution, so running it per apply would cost more than interpreting;
// running it once per artifact keeps its guarantee (ExecVm may assume
// well-formed bytecode) at parse-cache prices.
//
// A program that does not decode, or that does not carry exactly the bundled
// library's bytecode version (numscriptlib.CurrentBytecodeVersion — see
// SafeExecCompiled for why), is rejected loudly before verification and never
// inserted, so every cached entry holds a current-version program and the hit
// path needs no version check.
//
// vars is only consulted for its pool sizes, which VerifyWithVars checks
// LoadVar indices against. The sizes are fixed by the program's own variable
// layout, so a cached artifact is valid for every order that carries it; a
// size mismatch means the artifact and vars were produced by different
// compilations (a "should not happen") and is re-verified against the actual
// pools so it fails with the verifier's own error, loudly.
func (c *NumscriptCache) getOrDecodeCompiled(programBytes []byte, vars *numscriptlib.Vars) (*compiledLruEntry, domain.SerializableError) {
	hash := blake3.Sum256(programBytes)

	c.compiledMu.RLock()
	if elem, ok := c.compiledCache[hash]; ok {
		entry, _ := elem.Value.(*compiledLruEntry)
		c.compiledMu.RUnlock()

		if entry.nStr == len(vars.StringsPool) && entry.nInt == len(vars.IntsPool) {
			return entry, nil
		}

		if verifyErr := numscriptlib.VerifyCompiledProgramWithVars(entry.vm.Program, vars); verifyErr != nil {
			return nil, &domain.ErrNumscriptRuntime{
				Detail: "verifying compiled numscript program: " + verifyErr.Error(),
			}
		}

		return entry, nil
	}

	c.compiledMu.RUnlock()

	// Decode and verify outside the lock — the expensive part.
	program, decErr := numscriptlib.DecodeCompiledProgram(programBytes)
	if decErr != nil {
		return nil, &domain.ErrNumscriptRuntime{
			Detail: "decoding compiled numscript program: " + decErr.Error(),
		}
	}

	if program.Version != numscriptlib.CurrentBytecodeVersion {
		return nil, &domain.ErrNumscriptRuntime{
			Detail: fmt.Sprintf(
				"compiled numscript program encoded with bytecode version %s; this binary executes %s only",
				program.Version, numscriptlib.CurrentBytecodeVersion,
			),
		}
	}

	if verifyErr := numscriptlib.VerifyCompiledProgramWithVars(program, vars); verifyErr != nil {
		return nil, &domain.ErrNumscriptRuntime{
			Detail: "verifying compiled numscript program: " + verifyErr.Error(),
		}
	}

	c.compiledMu.Lock()
	defer c.compiledMu.Unlock()

	// Double-check: another goroutine may have inserted while we decoded.
	if elem, ok := c.compiledCache[hash]; ok {
		entry, _ := elem.Value.(*compiledLruEntry)

		return entry, nil
	}

	if c.compiledOrder.Len() >= c.maxSize {
		back := c.compiledOrder.Back()
		if back != nil {
			evicted, _ := c.compiledOrder.Remove(back).(*compiledLruEntry)
			delete(c.compiledCache, evicted.hash)
		}
	}

	entry := &compiledLruEntry{
		hash: hash,
		vm:   numscriptlib.NewVm(program),
		nStr: len(vars.StringsPool),
		nInt: len(vars.IntsPool),
	}
	c.compiledCache[hash] = c.compiledOrder.PushFront(entry)

	return entry, nil
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
