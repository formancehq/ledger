package numscript

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/big"
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

type mapValueSource struct {
	balances map[string]*big.Int // "account\x00asset\x00color"
	metadata map[string]string   // "account\x00key"
}

func (s mapValueSource) Balance(account, asset, color string) (*big.Int, error) {
	if b, ok := s.balances[account+"\x00"+asset+"\x00"+color]; ok {
		return new(big.Int).Set(b), nil
	}

	return new(big.Int), nil
}

func (s mapValueSource) Metadata(account, key string) (string, bool, error) {
	v, ok := s.metadata[account+"\x00"+key]

	return v, ok, nil
}

// mustEntry parses script through a fresh cache and returns its entry — what
// compileScript takes on the admission path.
func mustEntry(t *testing.T, script string) *lruEntry {
	t.Helper()

	entry := NewNumscriptCache(16).getOrParseEntry(script)
	require.Nil(t, entry.script.err)

	return entry
}

// mustCompile compiles script with vars the way admission does and fails the
// test on any compile error.
func mustCompile(t *testing.T, entry *lruEntry, vars map[string]string) *CompiledScript {
	t.Helper()

	compiled, err := compileScript(entry, vars)
	require.Nil(t, err)
	require.NotNil(t, compiled)

	return compiled
}

// withArtifactVersion returns a copy of an encoded program or vars blob with
// its bytecode version header rewritten. Both blobs share the library's header
// layout — a 4-byte magic, then major and minor as two little-endian uint16 —
// which is the one part of the format that has to stay put for any versioning
// to work at all; nothing past the header is touched.
func withArtifactVersion(t *testing.T, encoded []byte, version numscriptlib.BytecodeVersion) []byte {
	t.Helper()

	require.GreaterOrEqual(t, len(encoded), 8)

	patched := bytes.Clone(encoded)
	binary.LittleEndian.PutUint16(patched[4:], version.Major)
	binary.LittleEndian.PutUint16(patched[6:], version.Minor)

	return patched
}

// unreadableBytecodeVersions returns the bytecode versions around the bundled
// one that it cannot read: another major or a newer minor always, and under an
// unstable (0.x) bundled version an older minor too.
func unreadableBytecodeVersions(t *testing.T) map[string]numscriptlib.BytecodeVersion {
	t.Helper()

	current := numscriptlib.CurrentBytecodeVersion
	candidates := map[string]numscriptlib.BytecodeVersion{
		"newer major": {Major: current.Major + 1},
		"newer minor": {Major: current.Major, Minor: current.Minor + 1},
	}

	if current.Major > 0 {
		candidates["older major"] = numscriptlib.BytecodeVersion{Major: current.Major - 1, Minor: current.Minor}
	}

	if current.Minor > 0 {
		candidates["older minor"] = numscriptlib.BytecodeVersion{Major: current.Major, Minor: current.Minor - 1}
	}

	for name, v := range candidates {
		if current.CanRead(v) {
			delete(candidates, name)
		}
	}

	require.Contains(t, candidates, "newer major")
	require.Contains(t, candidates, "newer minor")

	return candidates
}

// TestCompileScript_ArtifactRoundTrips: a compilable script yields an artifact
// whose program and vars decode and execute.
func TestCompileScript_ArtifactRoundTrips(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := mustCompile(t, mustEntry(t, script), nil)

	hash := HashScript(script)
	require.Equal(t, hash[:], compiled.ScriptHash)

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}

	result, err := SafeExecCompiled(NewNumscriptCache(16), compiled.ScriptHash, compiled.Program, compiled.Vars, NewVMStore(source, false))
	require.Nil(t, err)
	require.Len(t, result.Postings, 1)
	require.Equal(t, "src", result.Postings[0].Source)
	require.Equal(t, "dst", result.Postings[0].Destination)
	require.Equal(t, int64(30), result.Postings[0].Amount.Int64())
}

// TestCompileScript_UnsupportedFeatureRejected: a script the compiler cannot
// lower is an admission rejection — the VM is the only engine, so there is no
// other way to run it. ErrNumscriptCompile is a freezable validation failure.
func TestCompileScript_UnsupportedFeatureRejected(t *testing.T) {
	t.Parallel()

	script := `#![feature("experimental-asset-scaling")]
send [COIN/2 100] (
  source = @src with scaling through @swap
  destination = @dst
)`
	compiled, err := compileScript(mustEntry(t, script), nil)
	require.Nil(t, compiled)
	requireCompileError(t, err)
	require.False(t, IsPanic(err))
}

// TestCompileScript_BadVarValueRejected: a var value that does not bind to the
// program's variable layout is an admission rejection, with the encoder's
// reason in the detail.
func TestCompileScript_BadVarValueRejected(t *testing.T) {
	t.Parallel()

	script := `vars {
  monetary $amt
}

send $amt (
  source = @src
  destination = @dst
)`
	compiled, err := compileScript(mustEntry(t, script), map[string]string{"amt": "not-a-monetary"})
	require.Nil(t, compiled)
	require.Contains(t, requireCompileError(t, err), "amt")
}

// requireCompileError asserts err is a freezable ErrNumscriptCompile and
// returns its detail.
func requireCompileError(t *testing.T, err domain.SerializableError) string {
	t.Helper()

	require.NotNil(t, err)

	var compileErr *domain.ErrNumscriptCompile
	require.ErrorAs(t, err, &compileErr)
	require.Equal(t, domain.KindValidation, compileErr.Kind())
	require.True(t, domain.IsFreezableFailure(compileErr.Kind()))

	return compileErr.Detail
}

// TestCompileScript_CompilesOncePerCachedScript: the script-dependent half of
// an admission compile is computed once per cached script and shared by every
// order carrying it, while the vars are bound per order and each order gets
// its own copy of the program bytes.
func TestCompileScript_CompilesOncePerCachedScript(t *testing.T) {
	t.Parallel()

	// Several registers die on the same instruction here, the case where
	// register allocation used to depend on map iteration order.
	script := `vars {
  number $a
  number $b
  number $c
  number $d
  number $e
  number $f
}

send [COIN ($a + $b) + ($c + $d) + ($e + $f)] (
  source = @world
  destination = @dst
)`
	cache := NewNumscriptCache(16)

	entry := cache.getOrParseEntry(script)
	require.Nil(t, entry.script.err)
	firstCompile, err := entry.compileParsed()
	require.Nil(t, err)
	secondCompile, err := entry.compileParsed()
	require.Nil(t, err)
	require.Same(t, firstCompile, secondCompile)
	require.Same(t, entry, cache.getOrParseEntry(script))

	first := mustCompile(t, cache.getOrParseEntry(script), map[string]string{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "f": "6"})

	second := mustCompile(t, cache.getOrParseEntry(script), map[string]string{"a": "10", "b": "20", "c": "30", "d": "40", "e": "50", "f": "60"})

	require.Equal(t, first.Program, second.Program)
	require.NotSame(t, &first.Program[0], &second.Program[0], "each order carries its own copy of the shared program bytes")
	require.Equal(t, first.ScriptHash, second.ScriptHash)
	require.NotEqual(t, first.Vars, second.Vars, "vars are bound per order")

	// An uncompilable script caches its outcome the same way.
	scaling := cache.getOrParseEntry(`#![feature("experimental-asset-scaling")]
send [COIN/2 100] (
  source = @src with scaling through @swap
  destination = @dst
)`)
	scalingCompiled, firstErr := scaling.compileParsed()
	require.Nil(t, scalingCompiled)
	requireCompileError(t, firstErr)
	_, secondErr := scaling.compileParsed()
	require.Same(t, firstErr, secondErr, "the compile failure is cached, not recomputed")
	_, err = compileScript(scaling, nil)
	require.Same(t, firstErr, err)
}

// TestVMStore_ForceReturnsUnlimitedBalance mirrors the resolver-facing
// Store's force semantics: balances are unlimited, metadata reads stay real.
func TestVMStore_ForceReturnsUnlimitedBalance(t *testing.T) {
	t.Parallel()

	source := mapValueSource{
		balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(1)},
		metadata: map[string]string{"src\x00k": "v"},
	}
	store := NewVMStore(source, true)

	balance, err := store.GetBalance(context.Background(), "src", "", "COIN", "")
	require.NoError(t, err)
	require.Equal(t, MaxForceBalance, balance)

	value, present, err := store.GetMetadata(context.Background(), "src", "", "k")
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, "v", value)
}

// TestVMStore_ScopedReadsRejected mirrors the resolver-facing Store: a scope
// view would collapse onto the single volume / metadata key (EN-1406 P1-2).
func TestVMStore_ScopedReadsRejected(t *testing.T) {
	t.Parallel()

	store := NewVMStore(mapValueSource{}, false)

	_, err := store.GetBalance(context.Background(), "src", "scope1", "COIN", "")
	require.ErrorIs(t, err, domain.ErrScopedBalanceUnsupported)

	_, _, err = store.GetMetadata(context.Background(), "src", "scope1", "k")
	require.ErrorIs(t, err, domain.ErrScopedBalanceUnsupported)
}

// TestSafeExecCompiled_WarmInstanceReuse: repeated applies of the same artifact
// through one cache run on the entry's single warm VM instance. Reuse must be
// invisible in results: each run sees only its own vars and store (no state
// leaks across runs, including from a failed run), and a result handed out
// earlier stays intact after later runs (postings are copied out of the VM,
// never aliased to its reusable buffers).
func TestSafeExecCompiled_WarmInstanceReuse(t *testing.T) {
	t.Parallel()

	script := `vars {
  monetary $amt
}

send $amt (
  source = @src
  destination = @dst
)`
	entry := mustEntry(t, script)

	first := mustCompile(t, entry, map[string]string{"amt": "COIN 30"})

	second := mustCompile(t, entry, map[string]string{"amt": "COIN 40"})

	// Identical program bytes — the script is compiled once per cache entry —
	// so both executions resolve to the same apply-side cache entry and the
	// later runs execute on the same warm instance as the first.
	require.Equal(t, first.Program, second.Program)

	cache := NewNumscriptCache(16)
	store := NewVMStore(mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}, false)

	firstResult, err := SafeExecCompiled(cache, first.ScriptHash, first.Program, first.Vars, store)
	require.Nil(t, err)
	require.Len(t, firstResult.Postings, 1)
	require.Equal(t, int64(30), firstResult.Postings[0].Amount.Int64())

	// A run that fails normally (missing funds against an empty store) leaves
	// the instance reusable for the next apply.
	_, err = SafeExecCompiled(cache, second.ScriptHash, second.Program, second.Vars, NewVMStore(mapValueSource{}, false))
	require.NotNil(t, err)

	secondResult, err := SafeExecCompiled(cache, second.ScriptHash, second.Program, second.Vars, store)
	require.Nil(t, err)
	require.Len(t, secondResult.Postings, 1)
	require.Equal(t, int64(40), secondResult.Postings[0].Amount.Int64())

	// The warm runs above must not have mutated the result handed out first.
	require.Len(t, firstResult.Postings, 1)
	require.Equal(t, int64(30), firstResult.Postings[0].Amount.Int64())
}

// panicValueSource makes the store panic mid-run, standing in for a library or
// adapter bug surfacing while a run has already mutated the VM instance.
type panicValueSource struct{}

func (panicValueSource) Balance(string, string, string) (*big.Int, error) {
	panic("store panic mid-run")
}

func (panicValueSource) Metadata(string, string) (string, bool, error) {
	return "", false, nil
}

// TestSafeExecCompiled_PanicLeavesInstanceReusable: a panicking run is
// recovered into the runtime-error contract, and the same warm instance —
// dirty from the aborted run — executes the next apply correctly (registers
// are write-before-read, the runstate resets on each exec, the program is
// immutable).
func TestSafeExecCompiled_PanicLeavesInstanceReusable(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := mustCompile(t, mustEntry(t, script), nil)

	cache := NewNumscriptCache(16)

	_, err := SafeExecCompiled(cache, compiled.ScriptHash, compiled.Program, compiled.Vars, NewVMStore(panicValueSource{}, false))
	require.NotNil(t, err)
	require.True(t, IsPanic(err))

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}

	result, err := SafeExecCompiled(cache, compiled.ScriptHash, compiled.Program, compiled.Vars, NewVMStore(source, false))
	require.Nil(t, err)
	require.Len(t, result.Postings, 1)
	require.Equal(t, int64(30), result.Postings[0].Amount.Int64())
}

// panickingValueSource is panicValueSource with a heap identity, so a weak
// pointer can observe whether a run retains it. The pointer field keeps it off
// the tiny allocator: a pointer-free object under 16 bytes shares its block
// with unrelated allocations and stays alive as long as any of them does.
type panickingValueSource struct {
	_ *byte
}

func (*panickingValueSource) Balance(string, string, string) (*big.Int, error) {
	panic("store panic mid-run")
}

func (*panickingValueSource) Metadata(string, string) (string, bool, error) {
	return "", false, nil
}

// weakSource returns source with a liveness probe that holds it only weakly.
func weakSource[T any, P interface {
	*T
	ValueSource
}](source P) (ValueSource, func() bool) {
	ref := weak.Make((*T)(source))

	return source, func() bool { return ref.Value() != nil }
}

// TestSafeExecCompiled_WarmInstanceReleasesSource: the cached warm instance
// outlives the run. On the apply path the source reaches the Scope and the
// proposal's whole coverage plan, so once SafeExecCompiled returns — on
// success, a normal failure or a recovered panic — the source must be
// collectable while the cache entry, and its instance, stay alive. The library
// releases the store when Exec returns; this pins that contract.
func TestSafeExecCompiled_WarmInstanceReleasesSource(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := mustCompile(t, mustEntry(t, script), nil)

	for _, tc := range []struct {
		name string
		// source returns a fresh source and reports whether it is still alive.
		source func() (ValueSource, func() bool)
		check  func(t *testing.T, err domain.SerializableError)
	}{
		{
			name: "success",
			source: func() (ValueSource, func() bool) {
				return weakSource(&mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}})
			},
			check: func(t *testing.T, err domain.SerializableError) {
				t.Helper()
				require.Nil(t, err)
			},
		},
		{
			name: "missing funds",
			source: func() (ValueSource, func() bool) {
				return weakSource(&mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(10)}})
			},
			check: func(t *testing.T, err domain.SerializableError) {
				t.Helper()
				var insufficientFunds *domain.ErrInsufficientFunds
				require.ErrorAs(t, err, &insufficientFunds)
			},
		},
		{
			name:   "panic",
			source: func() (ValueSource, func() bool) { return weakSource(&panickingValueSource{}) },
			check: func(t *testing.T, err domain.SerializableError) {
				t.Helper()
				require.True(t, IsPanic(err))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cache := NewNumscriptCache(16)

			// Run in its own frame so no local of this test keeps the source
			// reachable; only what the run left behind can.
			alive := func() func() bool {
				source, alive := tc.source()

				_, err := SafeExecCompiled(cache, compiled.ScriptHash, compiled.Program, compiled.Vars, NewVMStore(source, false))
				tc.check(t, err)

				return alive
			}()

			hash := [32]byte(compiled.ScriptHash)
			cache.compiledMu.RLock()
			_, cached := cache.compiledCache[hash]
			cache.compiledMu.RUnlock()
			require.True(t, cached, "the warm instance must stay cached")

			// Collect until the source is released; a reference the cached
			// instance holds never releases it.
			require.Eventually(t, func() bool {
				runtime.GC()

				return !alive()
			}, 5*time.Second, 10*time.Millisecond, "the cached warm instance still retains the run's source")

			runtime.KeepAlive(cache)
		})
	}
}

// TestSafeExecCompiled_ForeignBytecodeVersionRejected: an artifact whose
// program or vars carry a bytecode version the bundled library cannot read is
// rejected loudly (ErrNumscriptRuntime, not a panic) and never cached, for
// either half: the apply path hands every present artifact to SafeExecCompiled
// and never repairs one from the script text. A readable older minor of a
// stable major runs (TestSafeExecCompiled_OlderMinorRuns).
func TestSafeExecCompiled_ForeignBytecodeVersionRejected(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := mustCompile(t, mustEntry(t, script), nil)

	program, decErr := numscriptlib.DecodeCompiledProgram(compiled.Program)
	require.NoError(t, decErr)
	require.Equal(t, numscriptlib.CurrentBytecodeVersion, program.Version, "a fresh artifact carries the bundled bytecode version")

	const libraryRefusal = "not readable by this build" // the decoder's typed error, surfaced as a decode failure

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}

	for name, v := range unreadableBytecodeVersions(t) {
		for _, half := range []string{"program", "vars"} {
			t.Run(name+" "+half, func(t *testing.T) {
				t.Parallel()

				programBytes, varsBytes := compiled.Program, compiled.Vars
				if half == "program" {
					programBytes = withArtifactVersion(t, programBytes, v)
				} else {
					varsBytes = withArtifactVersion(t, varsBytes, v)
				}

				cache := NewNumscriptCache(16)

				_, err := SafeExecCompiled(cache, compiled.ScriptHash, programBytes, varsBytes, NewVMStore(source, false))
				require.NotNil(t, err)
				require.False(t, IsPanic(err))

				var runtimeErr *domain.ErrNumscriptRuntime
				require.ErrorAs(t, err, &runtimeErr)
				require.Contains(t, runtimeErr.Detail, "compiled numscript "+half)
				require.Contains(t, runtimeErr.Detail, libraryRefusal)
				require.Zero(t, cache.compiledOrder.Len(), "a rejected artifact must not be cached")

				// The rejection leaves the cache fit for the genuine artifact.
				result, err := SafeExecCompiled(cache, compiled.ScriptHash, compiled.Program, compiled.Vars, NewVMStore(source, false))
				require.Nil(t, err)
				require.Len(t, result.Postings, 1)
			})
		}
	}
}

// TestSafeExecCompiled_UndecodableArtifactIsLoud: bytes the bundled library
// cannot read at all are an internal error (our own codec wrote them), not a
// panic, not a client error — for either
// half of the artifact.
func TestSafeExecCompiled_UndecodableArtifactIsLoud(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := mustCompile(t, mustEntry(t, script), nil)

	garble := func(encoded []byte) []byte {
		out := bytes.Clone(encoded)
		for i := range out {
			out[i] ^= 0xA5
		}

		return out
	}

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}

	cases := map[string]struct {
		program, vars []byte
		detail        string
	}{
		"program": {garble(compiled.Program), compiled.Vars, "decoding compiled numscript program"},
		"vars":    {compiled.Program, garble(compiled.Vars), "decoding compiled numscript vars"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := SafeExecCompiled(NewNumscriptCache(16), compiled.ScriptHash, tc.program, tc.vars, NewVMStore(source, false))
			require.NotNil(t, err)
			require.False(t, IsPanic(err))

			var runtimeErr *domain.ErrNumscriptRuntime
			require.ErrorAs(t, err, &runtimeErr)
			require.Contains(t, runtimeErr.Detail, tc.detail)
		})
	}
}

// TestSafeExecCompiled_UnverifiableCurrentFormatIsLoud: bytecode in the bundled
// format that decodes but fails verification is an internal error — our own
// compiler produced it in this very format, so a malformed program means a
// compiler or verifier bug (invariant #7). Forged by re-encoding a decoded
// program with an opcode the VM does not have.
func TestSafeExecCompiled_UnverifiableCurrentFormatIsLoud(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := mustCompile(t, mustEntry(t, script), nil)

	program, decErr := numscriptlib.DecodeCompiledProgram(compiled.Program)
	require.NoError(t, decErr)
	require.NotEmpty(t, program.Instructions)

	program.Instructions[0].Opcode = 0xFF // no such opcode
	malformed := program.Encode()         // re-encoded in the bundled format

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}

	_, err := SafeExecCompiled(NewNumscriptCache(16), compiled.ScriptHash, malformed, compiled.Vars, NewVMStore(source, false))
	require.NotNil(t, err)
	require.False(t, IsPanic(err))

	var runtimeErr *domain.ErrNumscriptRuntime
	require.ErrorAs(t, err, &runtimeErr)
	require.Contains(t, runtimeErr.Detail, "verifying compiled numscript program")
}

// TestSafeExecCompiled_MissingFundsClassification: the VM's missing-funds error
// maps to ErrInsufficientFunds with ColorKnown unset.
func TestSafeExecCompiled_MissingFundsClassification(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := mustCompile(t, mustEntry(t, script), nil)

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(10)}}

	_, err := SafeExecCompiled(NewNumscriptCache(16), compiled.ScriptHash, compiled.Program, compiled.Vars, NewVMStore(source, false))
	require.NotNil(t, err)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "COIN", insufficientFunds.Asset)
	require.Equal(t, "30", insufficientFunds.Amount)
	require.Equal(t, "10", insufficientFunds.Balance)
}

// TestSafeExecCompiled_WarmHitForeignVersionRejected: the compiled cache is
// keyed by the script hash, so a warm entry must not serve an artifact of
// another bytecode version for the same script (a rolling upgrade: this node on
// the old binary, the artifact compiled by an upgraded leader). The bytes
// differ, so the lookup takes the cold path and rejects exactly as a node
// without the entry would — and, never inserted, the rejected artifact leaves
// the warm entry serving the genuine one.
func TestSafeExecCompiled_WarmHitForeignVersionRejected(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := mustCompile(t, mustEntry(t, script), nil)

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}
	cache := NewNumscriptCache(16)

	_, err := SafeExecCompiled(cache, compiled.ScriptHash, compiled.Program, compiled.Vars, NewVMStore(source, false))
	require.Nil(t, err)
	require.Equal(t, 1, cache.compiledOrder.Len())

	current := numscriptlib.CurrentBytecodeVersion
	foreign := withArtifactVersion(t, compiled.Program, numscriptlib.BytecodeVersion{Major: current.Major + 1})

	_, err = SafeExecCompiled(cache, compiled.ScriptHash, foreign, compiled.Vars, NewVMStore(source, false))
	require.NotNil(t, err)
	require.False(t, IsPanic(err))

	var runtimeErr *domain.ErrNumscriptRuntime
	require.ErrorAs(t, err, &runtimeErr)
	require.Contains(t, runtimeErr.Detail, "compiled numscript program")

	result, err := SafeExecCompiled(cache, compiled.ScriptHash, compiled.Program, compiled.Vars, NewVMStore(source, false))
	require.Nil(t, err)
	require.Len(t, result.Postings, 1)
}

// TestSafeExecCompiled_SameScriptDifferentBytesRunsCommittedBytes: compilation
// is not assumed to be deterministic, so the same script hash can arrive with
// different program bytes (a new leader, a rolling upgrade). A warm entry for
// that hash must not serve them: the node runs the committed bytes, which then
// replace the entry, and switching back re-verifies again. The two programs
// here differ observably (30 vs 40) so the test can tell which one ran.
func TestSafeExecCompiled_SameScriptDifferentBytesRunsCommittedBytes(t *testing.T) {
	t.Parallel()

	first := mustCompile(t, mustEntry(t, `send [COIN 30] (
  source = @src
  destination = @dst
)`), nil)
	second := mustCompile(t, mustEntry(t, `send [COIN 40] (
  source = @src
  destination = @dst
)`), nil)
	require.NotEqual(t, first.Program, second.Program)

	// Both artifacts claim the same script, as two compilations of it would.
	scriptHash := first.ScriptHash

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}
	cache := NewNumscriptCache(16)

	for _, step := range []struct {
		compiled *CompiledScript
		want     int64
	}{
		{first, 30},
		{second, 40},
		{first, 30},
	} {
		result, err := SafeExecCompiled(cache, scriptHash, step.compiled.Program, step.compiled.Vars, NewVMStore(source, false))
		require.Nil(t, err)
		require.Len(t, result.Postings, 1)
		require.Equal(t, step.want, result.Postings[0].Amount.Int64(), "the committed bytes must run, not the cached ones")
		require.Equal(t, 1, cache.compiledOrder.Len(), "new bytes replace the entry for the script")
	}
}

// TestSafeExecCompiled_OlderMinorRuns: under a stable bundled major, an
// artifact of an older minor keeps its meaning (a minor bump is additive), so
// it executes as-is — a node restarting on a newer binary still applies
// entries committed before the upgrade, with the same outcome as the replicas
// that applied them on the old one. Only exercisable once the bundled version
// is stable (an unstable 0.x reads only itself) with a minor above zero.
func TestSafeExecCompiled_OlderMinorRuns(t *testing.T) {
	t.Parallel()

	current := numscriptlib.CurrentBytecodeVersion
	if current.Major == 0 || current.Minor == 0 {
		t.Skipf("bundled bytecode version %s has no readable older minor", current)
	}

	compiled := mustCompile(t, mustEntry(t, `send [COIN 30] (
  source = @src
  destination = @dst
)`), nil)

	older := numscriptlib.BytecodeVersion{Major: current.Major, Minor: current.Minor - 1}
	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}

	result, err := SafeExecCompiled(NewNumscriptCache(16), compiled.ScriptHash,
		withArtifactVersion(t, compiled.Program, older), withArtifactVersion(t, compiled.Vars, older),
		NewVMStore(source, false))
	require.Nil(t, err)
	require.Len(t, result.Postings, 1)
	require.Equal(t, int64(30), result.Postings[0].Amount.Int64())
}

// TestSafeExecCompiled_NegativePortionRejected: a division portion that comes
// out negative at run time ($n/3 with n = -1) fails the order instead of
// sending the money to the other destinations.
func TestSafeExecCompiled_NegativePortionRejected(t *testing.T) {
	t.Parallel()

	compiled := mustCompile(t, mustEntry(t, `vars {
  number $n
}

send [COIN 90] (
  source = @world
  destination = {
    $n/3 to @acc1
    remaining to @acc2
  }
)`), map[string]string{"n": "-1"})

	result, err := SafeExecCompiled(NewNumscriptCache(16), compiled.ScriptHash, compiled.Program, compiled.Vars, NewVMStore(mapValueSource{}, false))
	require.NotNil(t, err, "a negative portion must fail the order, got postings %+v", result.Postings)
	require.False(t, IsPanic(err))

	var runtimeErr *domain.ErrNumscriptRuntime
	require.ErrorAs(t, err, &runtimeErr)
	require.Contains(t, runtimeErr.Detail, "cannot be negative")
}
