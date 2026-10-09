package numscript

import (
	"context"
	"math/big"
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"

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

// TestCompileScript_DirectExecution: a compilable script executes
// directly with the variables bound by admission.
func TestCompileScript_DirectExecution(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := mustCompile(t, mustEntry(t, script), nil)

	hash := HashScript(script)
	require.Equal(t, hash, compiled.scriptHash)

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}

	result, err := SafeExecCompiled(NewNumscriptCache(16), compiled, NewVMStore(source, false))
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

// TestCompileScript_CompilesOncePerCachedScript pins the shared program and
// per-order variable binding used by both admission and FSM apply.
func TestCompileScript_CompilesOncePerCachedScript(t *testing.T) {
	t.Parallel()
	script := `vars { number $amount } send [COIN $amount] (source = @world destination = @dst)`
	cache := NewNumscriptCache(16)
	entry := cache.getOrParseEntry(script)
	firstProgram, err := entry.compileParsed()
	require.Nil(t, err)
	secondProgram, err := entry.compileParsed()
	require.Nil(t, err)
	require.Same(t, firstProgram, secondProgram)
	first := mustCompile(t, entry, map[string]string{"amount": "1"})
	second := mustCompile(t, entry, map[string]string{"amount": "2"})
	require.Same(t, first.program, second.program)
	require.NotEqual(t, first.vars, second.vars)

	scaling := cache.getOrParseEntry(`#![feature("experimental-asset-scaling")]
 send [COIN/2 100] (source = @src with scaling through @swap destination = @dst)`)
	_, firstErr := scaling.compileParsed()
	requireCompileError(t, firstErr)
	_, secondErr := scaling.compileParsed()
	require.Same(t, firstErr, secondErr, "a compile failure is cached")
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

// TestSafeExecFromText_ColdWarmSameOutcome pins text-derived execution across
// independent cache histories. A warm apply must never depend on another
// order's vars or mutable VM state.
func TestSafeExecFromText_ColdWarmSameOutcome(t *testing.T) {
	t.Parallel()
	script := `vars { monetary $amt } send $amt (source = @src destination = @dst)`
	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}
	cache := NewNumscriptCache(16)
	vars := map[string]string{"amt": "COIN 30"}
	cold, err := SafeExecFromText(cache, script, vars, NewVMStore(source, false))
	require.Nil(t, err)
	warm, err := SafeExecFromText(cache, script, vars, NewVMStore(source, false))
	require.Nil(t, err)
	require.Equal(t, cold, warm)
	require.Len(t, warm.Postings, 1)
	require.Equal(t, "30", warm.Postings[0].Amount.String())
}

// TestSafeExecCompiled_WarmInstanceReuse: repeated applies of the same program
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

	// Both orders share the same compilation and resolve to one cache entry;
	// later runs execute on the same warm instance as the first.
	require.Same(t, first.program, second.program)

	cache := NewNumscriptCache(16)
	store := NewVMStore(mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}, false)

	firstResult, err := SafeExecCompiled(cache, first, store)
	require.Nil(t, err)
	require.Len(t, firstResult.Postings, 1)
	require.Equal(t, int64(30), firstResult.Postings[0].Amount.Int64())
	warmEntry, ok := cache.lookupCompiled(first.scriptHash)
	require.True(t, ok)

	// A run that fails normally (missing funds against an empty store) leaves
	// the instance reusable for the next apply.
	_, err = SafeExecCompiled(cache, second, NewVMStore(mapValueSource{}, false))
	require.NotNil(t, err)

	secondResult, err := SafeExecCompiled(cache, second, store)
	require.Nil(t, err)
	require.Len(t, secondResult.Postings, 1)
	require.Equal(t, int64(40), secondResult.Postings[0].Amount.Int64())
	reusedEntry, ok := cache.lookupCompiled(second.scriptHash)
	require.True(t, ok)
	require.Same(t, warmEntry.vm, reusedEntry.vm)

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

	_, err := SafeExecCompiled(cache, compiled, NewVMStore(panicValueSource{}, false))
	require.NotNil(t, err)
	require.True(t, IsPanic(err))

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}

	result, err := SafeExecCompiled(cache, compiled, NewVMStore(source, false))
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

				_, err := SafeExecCompiled(cache, compiled, NewVMStore(source, false))
				tc.check(t, err)

				return alive
			}()

			hash := compiled.scriptHash
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

	_, err := SafeExecCompiled(NewNumscriptCache(16), compiled, NewVMStore(source, false))
	require.NotNil(t, err)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "COIN", insufficientFunds.Asset)
	require.Equal(t, "30", insufficientFunds.Amount)
	require.Equal(t, "10", insufficientFunds.Balance)
}

// TestSafeExecCompiled_SameHashDifferentCompilation verifies that a warm VM
// is replaced when another local compilation is presented under the same hash.
// The scripts differ observably to expose any accidental reuse.
func TestSafeExecCompiled_SameHashDifferentCompilation(t *testing.T) {
	t.Parallel()

	first := mustCompile(t, mustEntry(t, `send [COIN 30] (source = @src destination = @dst)`), nil)
	second := mustCompile(t, mustEntry(t, `send [COIN 40] (source = @src destination = @dst)`), nil)
	second.scriptHash = first.scriptHash
	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}
	cache := NewNumscriptCache(16)

	for _, step := range []struct {
		compiled *CompiledScript
		want     int64
	}{{first, 30}, {second, 40}, {first, 30}} {
		result, err := SafeExecCompiled(cache, step.compiled, NewVMStore(source, false))
		require.Nil(t, err)
		require.Len(t, result.Postings, 1)
		require.Equal(t, step.want, result.Postings[0].Amount.Int64())
		require.Equal(t, 1, cache.compiledOrder.Len())
	}
}

// A parsed entry may be evicted without evicting its VM. Recompiling its text
// must replace that VM even though the script hash is unchanged.
func TestSafeExecFromText_ParsedEvictionReplacesVM(t *testing.T) {
	t.Parallel()

	const script = `send [COIN 30] (source = @src destination = @dst)`
	cache := NewNumscriptCache(1)
	store := NewVMStore(mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}, false)
	first, err := SafeExecFromText(cache, script, nil, store)
	require.Nil(t, err)
	warmEntry, ok := cache.lookupCompiled(HashScript(script))
	require.True(t, ok)

	cache.getOrParseEntry(`send [COIN 1] (source = @world destination = @other)`)
	second, err := SafeExecFromText(cache, script, nil, store)
	require.Nil(t, err)
	require.Equal(t, first, second)
	replacedEntry, ok := cache.lookupCompiled(HashScript(script))
	require.True(t, ok)
	require.NotSame(t, warmEntry.vm, replacedEntry.vm)
}

// TestSafeExecCompiled_UnverifiableLocalProgram ensures that a compiler or
// verifier bug is reported as an internal runtime error and is never cached.
func TestSafeExecCompiled_UnverifiableLocalProgram(t *testing.T) {
	t.Parallel()

	compiled := mustCompile(t, mustEntry(t, `send [COIN 30] (source = @src destination = @dst)`), nil)
	compiled.program.program.Instructions[0].Opcode = 0xff
	cache := NewNumscriptCache(16)

	_, err := SafeExecCompiled(cache, compiled, NewVMStore(mapValueSource{}, false))
	require.NotNil(t, err)
	require.False(t, IsPanic(err))
	var runtimeErr *domain.ErrNumscriptRuntime
	require.ErrorAs(t, err, &runtimeErr)
	require.Contains(t, runtimeErr.Detail, "verifying compiled numscript program")
	require.Zero(t, cache.compiledOrder.Len())
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

	result, err := SafeExecCompiled(NewNumscriptCache(16), compiled, NewVMStore(mapValueSource{}, false))
	require.NotNil(t, err, "a negative portion must fail the order, got postings %+v", result.Postings)
	require.False(t, IsPanic(err))

	var runtimeErr *domain.ErrNumscriptRuntime
	require.ErrorAs(t, err, &runtimeErr)
	require.Contains(t, runtimeErr.Detail, "cannot be negative")
}
