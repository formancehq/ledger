package numscript

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// NumscriptCache stores parsed Numscript programs keyed by their content hash,
// and decoded+verified VM artifacts — each as one warm VM instance — keyed by
// the same script-content hash and served only for the program bytes an order
// commits to: identical bytes when the program travels by value (see
// getOrDecodeCompiled), bytes with the committed hash when it travels by
// reference (see resolveByReference). Both sides use an LRU eviction policy
// bounded by maxSize to prevent unbounded memory growth. Thread-safe: an
// RWMutex allows concurrent cache hits without contention. LRU reordering is
// approximate — read hits do not call MoveToFront to avoid write-locking on
// the hot path.
//
// Admission and the FSM apply path each construct their own NumscriptCache
// instance (see internal/bootstrap/module.go and
// processing.NewRequestProcessor) — they share no state. The compiled side
// (compiledCache) is populated only by getOrDecodeCompiled, reached only from
// the FSM apply path (SafeExecCompiled, SafeExecCommitted), so it is never
// warm on admission's instance; admission's parsed side remembers, per entry,
// whether it has compiled the script before (lruEntry.compiledBefore).
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
	// compiledBefore reports, to the caller of compileParsed, whether an
	// earlier call on this exact entry already ran the compile (true) or this
	// call is the one doing it (false) — see compileParsed.
	compiledBefore atomic.Bool
}

// compiledProgram is the script-dependent half of an admission compile: the
// program, its encoded bytecode and that bytecode's hash (HashProgram, the
// order's compiled_program_hash when the bytes travel by reference), and the
// encoder that binds an order's vars to the program's variable layout. None
// depends on any order's values, so one instance serves every order of the
// script. It is shared and never mutated: compileScript hands each order its
// own copy of the bytes, and admission's effects run builds its own VM
// instance over the (immutable) program.
type compiledProgram struct {
	varsEncoder numscriptlib.VarsEncoder
	program     numscriptlib.CompiledProgram
	encoded     []byte
	encodedHash [16]byte
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
// program is the exact encoded bytes that were decoded and verified, and
// programHash their HashProgram. A hit requires the order's bytes to be
// identical (by value) or to have the order's hash (by reference), so the node
// always executes the artifact admission compiled, never another compilation
// of the same script.
type compiledLruEntry struct {
	hash        [16]byte
	program     []byte
	programHash [16]byte
	vm          *numscriptlib.Vm
	verified    numscriptlib.VerifiedVarsInfo
}

// verifyVars checks an order's vars against the entry's program: O(1) through
// the verification record when the shape is one it reports sufficient,
// otherwise the full static pass against the actual vars, so a failure carries
// the verifier's own error (a "should not happen", see getOrDecodeCompiled).
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
// for the parsed script and compiled artifact, and the order's
// compiled_script_hash binding the artifact to its text. It runs on the FSM
// apply path for every scripted order. Like the attribute keys, it is not
// collision-resistant against chosen inputs; every writer of a cluster is
// trusted with every ledger, so a crafted collision gains nothing a direct
// write could not.
func HashScript(script string) [16]byte {
	return xxh3.HashString128(script).Bytes()
}

// HashProgram computes the XXH3-128 hash of encoded program bytes: the order's
// compiled_program_hash when the program travels by reference, and what the
// FSM checks a cached or recompiled program against before running it in
// place of the committed bytes (see SafeExecCommitted). Same collision
// caveat as HashScript.
func HashProgram(program []byte) [16]byte {
	return xxh3.Hash128(program).Bytes()
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
//
// alreadyCompiled reports whether an earlier call on this exact entry already
// claimed the compile: false for exactly one call per entry (the atomic swap
// below), true for every later one — including concurrent first callers that
// then block on the Once for the result, and callers after a cached compile
// failure. Admission reads it as "this instance has sent the bytecode for
// this script before" and sends the program by reference from then on (see
// compileScript's callers); the FSM tolerates the signal being wrong in
// either direction (see SafeExecCommitted). An entry evicted and later
// recreated starts this at false again, which is the conservative answer:
// this incarnation has not compiled it yet, regardless of an older
// incarnation having done so before eviction.
func (e *lruEntry) compileParsed() (program *compiledProgram, err domain.SerializableError, alreadyCompiled bool) {
	alreadyCompiled = e.compiledBefore.Swap(true)

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

		encoded := program.Encode()
		e.compiled = &compiledProgram{
			varsEncoder: varsEncoder,
			program:     program,
			encoded:     encoded,
			encodedHash: HashProgram(encoded),
		}
	})

	return e.compiled, e.compileErr, alreadyCompiled
}

// getOrDecodeCompiled returns the cache entry holding the decoded, verified VM
// program — as one warm VM instance — for an admission-compiled artifact,
// decoding and verifying on the first sighting and serving every later apply
// from cache. Verification runs once per artifact and its guarantee (ExecVm
// may assume well-formed bytecode) holds for every later apply.
//
// A program that does not decode — including one with an invalid header or of
// a bytecode version the bundled library cannot read, which the decoder
// refuses — is rejected loudly before verification and never inserted, so
// every cached entry holds a program this binary can run. (SafeExecCommitted
// routes an artifact of a version this library cannot read to the script text
// before it gets here; see there.)
//
// Entries are keyed by scriptHash — the order's HashScript(text), already
// checked against the resolved text by the caller — and hold the exact program
// bytes they verified. A hit requires those bytes to equal the order's:
// compiling the same text under the same bundled library is deterministic —
// byte-identical, always — but this binary is not the only one that could
// have produced the committed bytes: a replica on another library version
// (a rolling upgrade) compiles the same text to other bytes, and may have
// cached its own compile of the script (SafeExecFromText) before a by-value
// order of the leader's bytes arrives. On a mismatch the order's bytes take
// the cold path and, once verified, replace the entry: the node always
// executes the committed bytes, and only pays the decode+verify again when
// the bytes change. A rejected artifact (undecodable, unverifiable) is never
// inserted, so it leaves the current entry in place. The cache itself is
// in-memory, so a binary upgrade restarts with it empty.
//
// vars is only consulted through the entry's VerifiedVarsInfo: the vars shape
// is fixed by the program's own variable layout, so a cached artifact is valid
// for every order that carries it. A shape the library does not report
// sufficient means the artifact and vars were produced by different
// compilations (a "should not happen") and is re-verified against the actual
// vars so it fails with the verifier's own error, loudly.
func (c *NumscriptCache) getOrDecodeCompiled(scriptHash, programBytes []byte, vars *numscriptlib.Vars) (*compiledLruEntry, domain.SerializableError) {
	if len(scriptHash) != len([16]byte{}) {
		return nil, &domain.ErrNumscriptRuntime{
			Detail: fmt.Sprintf("compiled numscript artifact: script hash has %d bytes, want 16", len(scriptHash)),
		}
	}

	hash := [16]byte(scriptHash)

	if entry, ok := c.lookupCompiled(hash); ok && bytes.Equal(entry.program, programBytes) {
		if verifyErr := entry.verifyVars(vars); verifyErr != nil {
			return nil, verifyErr
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

	entry := &compiledLruEntry{
		hash: hash,
		// Own copy: programBytes aliases the committed order.
		program:     bytes.Clone(programBytes),
		programHash: HashProgram(programBytes),
		vm:          numscriptlib.NewVm(program),
		verified:    verified,
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

// resolveByReference returns the compiled-side entry holding the bytes with
// programHash for the script keyed by scriptHash, for an order that carries
// its program by reference (see SafeExecCommitted): the cached entry when its
// bytes have that hash, otherwise this binary's own compile of script —
// decoded, verified against vars and cached, exactly as a by-value program
// would be, but only if it reproduces the hash. reproduced is false when it
// does not: another library version compiled the committed bytes, and the
// caller must derive the order from the text instead. A parse or compile
// failure of the text is returned as is, since deriving from the text would
// fail the same way. scriptHash must be HashScript(script).
func (c *NumscriptCache) resolveByReference(scriptHash, programHash [16]byte, script string, vars *numscriptlib.Vars) (entry *compiledLruEntry, reproduced bool, err domain.SerializableError) {
	if cached, ok := c.lookupCompiled(scriptHash); ok && cached.programHash == programHash {
		if verifyErr := cached.verifyVars(vars); verifyErr != nil {
			return nil, true, verifyErr
		}

		return cached, true, nil
	}

	parsed := c.getOrParseEntryHashed(scriptHash, script)
	if parsed.script.err != nil {
		return nil, true, parsed.script.err
	}

	compiled, compileErr, _ := parsed.compileParsed()
	if compileErr != nil {
		return nil, true, compileErr
	}

	if compiled.encodedHash != programHash {
		return nil, false, nil
	}

	entry, err = c.getOrDecodeCompiled(scriptHash[:], compiled.encoded, vars)
	if err != nil {
		return nil, true, err
	}

	return entry, true, nil
}

// InitCacheMetrics initializes the cache metrics on the NumscriptCache.
func (c *NumscriptCache) InitCacheMetrics(m metric.Meter) error {
	size, err := m.Int64Gauge(
		"numscript.cache.size",
		metric.WithDescription("Number of entries in the Numscript cache, per side (parsed scripts, compiled VM artifacts)"),
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
