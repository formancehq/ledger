package numscript

import (
	"bytes"
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
// the same script-content hash and served only for identical program bytes
// (see getOrDecodeCompiled). Both sides use an
// LRU eviction policy bounded by maxSize to prevent unbounded memory growth.
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
	compileErr  domain.SerializableError
}

// compiledProgram is the script-dependent half of an admission compile: the
// program, its encoded bytecode, and the encoder that binds an order's vars to
// the program's variable layout. None depends on any order's values, so one
// instance serves every order of the script. It is shared and never mutated:
// compileScript hands each order its own copy of the bytes, and admission's
// effects run builds its own VM instance over the (immutable) program.
type compiledProgram struct {
	varsEncoder numscriptlib.VarsEncoder
	program     numscriptlib.CompiledProgram
	encoded     []byte
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
// verified is the library's record of what the verification checked against
// the vars it ran with. The vars shape is a property of the program's own
// variable layout (the encoder appends one slot per declaration), so every
// order carrying this artifact presents a shape it reports sufficient, and the
// verification outcome is reusable without re-running the static pass.
//
// program is the exact encoded bytes that were decoded and verified. A hit
// requires the order's bytes to be identical, so the node always executes the
// committed artifact, never another compilation of the same script.
type compiledLruEntry struct {
	hash     [32]byte
	program  []byte
	vm       *numscriptlib.Vm
	verified numscriptlib.VerifiedVarsInfo
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
// with every later caller. A compile failure is as stable a property of the
// text as a parse error and is cached the same way, so an uncompilable script
// does not re-run the compiler on every order: ErrNumscriptCompile when the
// compiler rejects the script, a panicError (IsPanic) when it panics. Callers
// only reach here for a script that parsed. Only the script-dependent half is
// computed here; binding an order's vars (VarsEncoder.Encode) is per-order and
// stays with the caller. The compile runs outside the cache locks; concurrent
// callers for the same script block on the Once and share the single result.
func (e *lruEntry) compileParsed() (*compiledProgram, domain.SerializableError) {
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

		e.compiled = &compiledProgram{varsEncoder: varsEncoder, program: program, encoded: program.Encode()}
	})

	return e.compiled, e.compileErr
}

// getOrDecodeCompiled returns the cache entry holding the decoded, verified VM
// program — as one warm VM instance — for an admission-compiled artifact,
// decoding and verifying on the first sighting and serving every later apply
// from cache. Verification runs once per artifact and its guarantee (ExecVm
// may assume well-formed bytecode) holds for every later apply.
//
// A program that does not decode, or whose bytecode version the bundled
// library cannot read (numscriptlib.CurrentBytecodeVersion.CanRead — see
// SafeExecCompiled for why), is rejected loudly before verification and never
// inserted, so every cached entry holds a program this binary can run.
//
// Entries are keyed by scriptHash — the order's HashScript(text), already
// checked against the resolved text by the caller — and hold the exact program
// bytes they verified. A hit requires those bytes to equal the order's:
// compilation is not assumed to be deterministic, so two compilations of the
// same script (another leader, a rolling upgrade, a parse-cache eviction on the
// leader) may produce different bytes. On mismatch the order's bytes take the
// cold path and, once verified, replace the entry: the node always executes
// the committed bytes, and only pays the decode+verify again when the bytes
// change. A rejected artifact (undecodable, foreign version, unverifiable) is
// never inserted, so it leaves the current entry in place. The cache itself is
// in-memory, so a binary upgrade restarts with it empty.
//
// vars is only consulted through the entry's VerifiedVarsInfo: the vars shape
// is fixed by the program's own variable layout, so a cached artifact is valid
// for every order that carries it. A shape the library does not report
// sufficient means the artifact and vars were produced by different
// compilations (a "should not happen") and is re-verified against the actual
// vars so it fails with the verifier's own error, loudly.
func (c *NumscriptCache) getOrDecodeCompiled(scriptHash, programBytes []byte, vars *numscriptlib.Vars) (*compiledLruEntry, domain.SerializableError) {
	if len(scriptHash) != len([32]byte{}) {
		return nil, &domain.ErrNumscriptRuntime{
			Detail: fmt.Sprintf("compiled numscript artifact: script hash has %d bytes, want 32", len(scriptHash)),
		}
	}

	hash := [32]byte(scriptHash)

	c.compiledMu.RLock()
	elem, ok := c.compiledCache[hash]
	c.compiledMu.RUnlock()

	var entry *compiledLruEntry
	if ok {
		entry, _ = elem.Value.(*compiledLruEntry)
		ok = bytes.Equal(entry.program, programBytes)
	}

	if ok {
		if entry.verified.CheckVars(vars) {
			return entry, nil
		}

		if _, verifyErr := numscriptlib.VerifyCompiledProgramWithVars(entry.vm.Program, vars); verifyErr != nil {
			return nil, &domain.ErrNumscriptRuntime{
				Detail: "verifying compiled numscript program: " + verifyErr.Error(),
			}
		}

		return entry, nil
	}

	// Decode and verify outside the lock — the expensive part.
	program, decErr := numscriptlib.DecodeCompiledProgram(programBytes)
	if decErr != nil {
		return nil, &domain.ErrNumscriptRuntime{
			Detail: "decoding compiled numscript program: " + decErr.Error(),
		}
	}

	if !numscriptlib.CurrentBytecodeVersion.CanRead(program.Version) {
		return nil, &domain.ErrNumscriptRuntime{
			Detail: fmt.Sprintf(
				"compiled numscript program encoded with bytecode version %s; this binary executes %d.0 through %s",
				program.Version, numscriptlib.CurrentBytecodeVersion.Major, numscriptlib.CurrentBytecodeVersion,
			),
		}
	}

	verified, verifyErr := numscriptlib.VerifyCompiledProgramWithVars(program, vars)
	if verifyErr != nil {
		return nil, &domain.ErrNumscriptRuntime{
			Detail: "verifying compiled numscript program: " + verifyErr.Error(),
		}
	}

	c.compiledMu.Lock()
	defer c.compiledMu.Unlock()

	// Another goroutine may have inserted while we decoded: reuse its entry
	// only for the same bytes, otherwise replace it with the ones just verified.
	if elem, ok := c.compiledCache[hash]; ok {
		existing, _ := elem.Value.(*compiledLruEntry)
		if bytes.Equal(existing.program, programBytes) {
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

	entry = &compiledLruEntry{
		hash: hash,
		// Own copy: programBytes aliases the committed order.
		program:  bytes.Clone(programBytes),
		vm:       numscriptlib.NewVm(program),
		verified: verified,
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
