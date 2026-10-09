package numscript

import (
	"container/list"
	"context"
	"sync"

	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// NumscriptCache stores parsed scripts and verified, locally compiled VM programs
// keyed by the script's content hash. Each side has a bounded LRU. Cache
// residency changes only the amount of work needed to execute a script, never
// the business input or the result. Read hits do not reorder the LRU.
//
// Admission and the FSM apply path each construct their own NumscriptCache
// instance (see internal/bootstrap/module.go and
// processing.NewRequestProcessor) and share no state. The compiled side is
// populated during FSM execution; admission executes its locally compiled
// program on a fresh VM instance.
type NumscriptCache struct {
	mu      sync.RWMutex
	cache   map[[16]byte]*list.Element
	order   *list.List
	maxSize int

	compiledMu    sync.RWMutex
	compiledCache map[[16]byte]*list.Element
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
	hash   [16]byte
	script parsedScript

	compileOnce sync.Once
	compiled    *compiledProgram
	compileErr  domain.SerializableError
}

// compiledProgram is the script-dependent half of a local compile: the VM
// program and the encoder for an order's variables. It is
// immutable and shared by all uses of the same parsed script.
type compiledProgram struct {
	varsEncoder numscriptlib.VarsEncoder
	program     numscriptlib.CompiledProgram
}

// parsedScript wraps a parsed Numscript program with any parsing errors.
type parsedScript struct {
	program numscriptlib.ParseResult
	err     domain.SerializableError
}

// compiledLruEntry holds one verified local program as a warm VM instance.
// Every apply of this program reuses the
// instance: a "dirty" one is always safe — registers are write-before-read
// (verified), the runstate resets on each exec, and the program is immutable —
// even after a recovered panic. The one hard rule is that the same instance
// must never execute concurrently; the FSM apply path is single-threaded (see
// RequestProcessor), which guarantees it.
//
// verified records the variable layout checked by the library. Its result is
// reusable across orders with different values of the same shape. program
// identifies the exact local compilation used to construct this VM; a cache
// hit cannot reuse a VM built from another compilation of the same script.
type compiledLruEntry struct {
	hash     [16]byte
	program  *compiledProgram
	vm       *numscriptlib.Vm
	verified numscriptlib.VerifiedVarsInfo
}

// verifyVars checks an order's vars against the entry's program: O(1) through
// the verification record when the shape is one it reports sufficient,
// otherwise the full static pass against the actual vars, so a failure carries
// the verifier's own error (a "should not happen", see getOrCreateVM).
func (e *compiledLruEntry) verifyVars(vars *numscriptlib.Vars) domain.SerializableError {
	if e.verified.CheckVars(vars) {
		return nil
	}

	if _, verifyErr := numscriptlib.VerifyCompiledProgramWithVars(e.vm.Program, vars); verifyErr != nil {
		return &domain.ErrNumscriptRuntime{
			Detail: "verifying compiled numscript program: " + verifyErr.Error(),
		}
	}

	return nil
}

// NewNumscriptCache creates a new NumscriptCache with the given maximum size.
// If maxSize <= 0, it defaults to 1024.
func NewNumscriptCache(maxSize int) *NumscriptCache {
	if maxSize <= 0 {
		maxSize = 1024
	}

	return &NumscriptCache{
		cache:         make(map[[16]byte]*list.Element, maxSize),
		order:         list.New(),
		maxSize:       maxSize,
		compiledCache: make(map[[16]byte]*list.Element, maxSize),
		compiledOrder: list.New(),
	}
}

// HashScript computes the XXH3-128 hash of the script content: the cache key
// for the parsed script and compiled VM program. It runs on the FSM apply
// path for every scripted order. Like the attribute keys, it is not
// collision-resistant against chosen inputs; every writer of a cluster is
// trusted with every ledger, so a crafted collision gains nothing a direct
// write could not.
func HashScript(script string) [16]byte {
	return xxh3.HashString128(script).Bytes()
}

// GetOrParse retrieves a parsed script from the cache or parses it if not found.
func (c *NumscriptCache) GetOrParse(script string) (numscriptlib.ParseResult, domain.SerializableError) {
	return c.GetOrParseHashed(HashScript(script), script)
}

// GetOrParseHashed is GetOrParse for a caller that already holds
// HashScript(script), so the text is not hashed twice. hash must be exactly
// HashScript(script): the cache is keyed by it, and a mismatched hash would
// cache the parse under another script's key.
func (c *NumscriptCache) GetOrParseHashed(hash [16]byte, script string) (numscriptlib.ParseResult, domain.SerializableError) {
	entry := c.getOrParseEntryHashed(hash, script)

	return entry.script.program, entry.script.err
}

// getOrParseEntry is GetOrParse returning the cache entry itself, for callers
// in this package that also need what hangs off it: the script's hash and its
// once-per-script compile.
// On cache hit the lookup uses a read lock for zero contention under concurrent reads.
// On cache miss the script is parsed outside the lock, then inserted with a write lock.
// LRU ordering is approximate: read hits do not reorder to avoid write contention.
func (c *NumscriptCache) getOrParseEntry(script string) *lruEntry {
	return c.getOrParseEntryHashed(HashScript(script), script)
}

// getOrParseEntryHashed is getOrParseEntry with hash = HashScript(script)
// supplied by the caller.
func (c *NumscriptCache) getOrParseEntryHashed(hash [16]byte, script string) *lruEntry {
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

	c.recordSize(cacheSideParsed, int64(c.order.Len()))

	return entry
}

// compileParsed compiles the entry's script at most once and shares the result
// with every later caller. A compile failure is as stable a property of the
// text as a parse error and is cached the same way, so an uncompilable script
// does not re-run the compiler on every order: ErrNumscriptCompile when the
// compiler rejects the script, a panicError (IsPanic) when it panics. Callers
// only reach here for a script that parsed. Only the script-dependent half is
// computed here; binding an order's vars (VarsEncoder.Encode) is per-order and
// stays with the caller. The compile runs outside the cache locks; concurrent
// callers for the same script block on the Once and share the single result.
func (e *lruEntry) compileParsed() (program *compiledProgram, err domain.SerializableError) {
	e.compileOnce.Do(func() {
		defer func() {
			if panicErr := numscriptPanicToDescribable(recover()); panicErr != nil {
				e.compiled = nil
				e.compileErr = panicErr
			}
		}()

		varsEncoder, program, err := e.script.program.Compile()
		if err != nil {
			e.compileErr = &domain.ErrNumscriptCompile{Detail: err.Error()}

			return
		}

		e.compiled = &compiledProgram{
			varsEncoder: varsEncoder,
			program:     program,
		}
	})

	return e.compiled, e.compileErr
}

// getOrCreateVM returns the cached VM for a locally compiled program,
// verifying on first use. Verification runs once per compilation;
// ExecVm may then assume well-formed bytecode.
//
// A program that does not verify is rejected and never inserted.
//
// Entries are keyed by HashScript(text), and a hit also compares the local
// compilation's identity. A mismatch replaces the entry after successful
// verification. A rejected program leaves the current entry in place.
//
// vars is only consulted through the entry's VerifiedVarsInfo: the vars shape
// is fixed by the program's own variable layout, so a cached VM is valid
// for every order that carries it. A shape the library does not report
// sufficient means the program and vars were produced by different
// compilations (a "should not happen") and is re-verified against the actual
// vars so it fails with the verifier's own error, loudly.
func (c *NumscriptCache) getOrCreateVM(hash [16]byte, program *compiledProgram, vars *numscriptlib.Vars) (*compiledLruEntry, domain.SerializableError) {
	if entry, ok := c.lookupCompiled(hash); ok && entry.program == program {
		if verifyErr := entry.verifyVars(vars); verifyErr != nil {
			return nil, verifyErr
		}

		return entry, nil
	}

	// Verify outside the lock — the expensive part.
	verified, verifyErr := numscriptlib.VerifyCompiledProgramWithVars(program.program, vars)
	if verifyErr != nil {
		return nil, &domain.ErrNumscriptRuntime{
			Detail: "verifying compiled numscript program: " + verifyErr.Error(),
		}
	}

	c.compiledMu.Lock()
	defer c.compiledMu.Unlock()

	// Another goroutine may have inserted while we verified: reuse its entry
	// only for this compilation, otherwise replace it with the one just verified.
	if elem, ok := c.compiledCache[hash]; ok {
		existing, _ := elem.Value.(*compiledLruEntry)
		if existing.program == program {
			return existing, nil
		}

		c.compiledOrder.Remove(elem)
		delete(c.compiledCache, hash)
	}

	if c.compiledOrder.Len() >= c.maxSize {
		back := c.compiledOrder.Back()
		if back != nil {
			evicted, _ := c.compiledOrder.Remove(back).(*compiledLruEntry)
			delete(c.compiledCache, evicted.hash)
		}
	}

	entry := &compiledLruEntry{
		hash:     hash,
		program:  program,
		vm:       numscriptlib.NewVm(program.program),
		verified: verified,
	}
	c.compiledCache[hash] = c.compiledOrder.PushFront(entry)

	c.recordSize(cacheSideCompiled, int64(c.compiledOrder.Len()))

	return entry, nil
}

// lookupCompiled returns the compiled-side entry cached under scriptHash, if
// any. Read-locked only: a hit never reorders the LRU (see NumscriptCache).
func (c *NumscriptCache) lookupCompiled(scriptHash [16]byte) (*compiledLruEntry, bool) {
	c.compiledMu.RLock()
	elem, ok := c.compiledCache[scriptHash]
	c.compiledMu.RUnlock()

	if !ok {
		return nil, false
	}

	entry, _ := elem.Value.(*compiledLruEntry)

	return entry, true
}

// InitCacheMetrics initializes the cache metrics on the NumscriptCache.
func (c *NumscriptCache) InitCacheMetrics(m metric.Meter) error {
	size, err := m.Int64Gauge(
		"numscript.cache.size",
		metric.WithDescription("Number of entries in the Numscript cache, per side (parsed scripts, warm VMs)"),
	)
	if err != nil {
		return err
	}

	c.sizeGauge = size

	return nil
}

// Values of the numscript.cache.size "cache" attribute: the two LRUs are
// bounded independently, so each side reports its own size.
const (
	cacheSideParsed   = "parsed"
	cacheSideCompiled = "compiled"
)

// recordSize records the current size of one cache side.
func (c *NumscriptCache) recordSize(side string, size int64) {
	if c.sizeGauge == nil {
		return
	}

	c.sizeGauge.Record(context.Background(), size, metric.WithAttributes(attribute.String("cache", side)))
}
