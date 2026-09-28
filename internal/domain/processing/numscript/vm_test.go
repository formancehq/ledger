package numscript

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/big"
	"testing"

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

func mustParse(t *testing.T, script string) numscriptlib.ParseResult {
	t.Helper()

	parsed := numscriptlib.Parse(script)
	require.Empty(t, parsed.GetParsingErrors())

	return parsed
}

// TestCompileScript_ArtifactRoundTrips: a compilable script yields an artifact
// whose program and vars decode and execute.
func TestCompileScript_ArtifactRoundTrips(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := compileScript(mustParse(t, script), script, nil)
	require.NotNil(t, compiled)

	hash := HashScript(script)
	require.Equal(t, hash[:], compiled.ScriptHash)

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}

	result, err := SafeExecCompiled(NewNumscriptCache(16), compiled.Program, compiled.Vars, NewVMStore(source, false))
	require.Nil(t, err)
	require.Len(t, result.Postings, 1)
	require.Equal(t, "src", result.Postings[0].Source)
	require.Equal(t, "dst", result.Postings[0].Destination)
	require.Equal(t, int64(30), result.Postings[0].Amount.Int64())
}

// TestCompileScript_UnsupportedFeatureFallsBack: a script the compiler cannot
// lower (asset scaling) produces no artifact — the FSM then runs the
// interpreter, which owns the authoritative outcome.
func TestCompileScript_UnsupportedFeatureFallsBack(t *testing.T) {
	t.Parallel()

	script := `#![feature("experimental-asset-scaling")]
send [COIN/2 100] (
  source = @src with scaling through @swap
  destination = @dst
)`
	require.Nil(t, compileScript(mustParse(t, script), script, nil))
}

// TestCompileScript_BadVarValueFallsBack: a var value the encoder rejects also
// yields no artifact, so the interpreter produces the canonical client error.
func TestCompileScript_BadVarValueFallsBack(t *testing.T) {
	t.Parallel()

	script := `vars {
  monetary $amt
}

send $amt (
  source = @src
  destination = @dst
)`
	require.Nil(t, compileScript(mustParse(t, script), script, map[string]string{"amt": "not-a-monetary"}))
}

// TestVMStore_ForceReturnsUnlimitedBalance mirrors the interpreter-facing
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

// TestVMStore_ScopedReadsRejected mirrors the interpreter-facing Store: a scope
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
// through one cache run on a warm VM instance recycled via the cache entry's
// pool. Reuse must be invisible in results: each run sees only its own vars and
// store (no state leaks across runs, including from a failed run), and a result
// handed out earlier stays intact after later runs (postings are copied out of
// the VM, never aliased to its reusable buffers).
func TestSafeExecCompiled_WarmInstanceReuse(t *testing.T) {
	t.Parallel()

	script := `vars {
  monetary $amt
}

send $amt (
  source = @src
  destination = @dst
)`
	parsed := mustParse(t, script)

	first := compileScript(parsed, script, map[string]string{"amt": "COIN 30"})
	require.NotNil(t, first)

	second := compileScript(parsed, script, map[string]string{"amt": "COIN 40"})
	require.NotNil(t, second)

	// Identical program bytes: both executions resolve to the same cache entry,
	// so the later runs execute on the instance the first run pooled.
	require.Equal(t, first.Program, second.Program)

	cache := NewNumscriptCache(16)
	store := NewVMStore(mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}, false)

	firstResult, err := SafeExecCompiled(cache, first.Program, first.Vars, store)
	require.Nil(t, err)
	require.Len(t, firstResult.Postings, 1)
	require.Equal(t, int64(30), firstResult.Postings[0].Amount.Int64())

	// A run that fails normally (missing funds against an empty store) still
	// returns its instance to the pool for the next apply.
	_, err = SafeExecCompiled(cache, second.Program, second.Vars, NewVMStore(mapValueSource{}, false))
	require.NotNil(t, err)

	secondResult, err := SafeExecCompiled(cache, second.Program, second.Vars, store)
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

// TestSafeExecCompiled_PanicDropsWarmInstance: a panicking run is recovered
// into the runtime-error contract and its possibly half-mutated VM instance is
// dropped rather than pooled; the next apply of the same artifact gets a fresh
// instance and succeeds.
func TestSafeExecCompiled_PanicDropsWarmInstance(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := compileScript(mustParse(t, script), script, nil)
	require.NotNil(t, compiled)

	cache := NewNumscriptCache(16)

	_, err := SafeExecCompiled(cache, compiled.Program, compiled.Vars, NewVMStore(panicValueSource{}, false))
	require.NotNil(t, err)
	require.True(t, IsPanic(err))

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}

	result, err := SafeExecCompiled(cache, compiled.Program, compiled.Vars, NewVMStore(source, false))
	require.Nil(t, err)
	require.Len(t, result.Postings, 1)
	require.Equal(t, int64(30), result.Postings[0].Amount.Int64())
}

// TestSafeExecCompiled_VersionMismatchRejected: program and vars must be
// encoded with the same wire format version — admission produces both in one
// compilation pass, so a pair straddling two versions was assembled by
// different binaries and its vars layout cannot be trusted against the
// program. Surfaced loudly, never executed. The mismatch is forged by
// patching the version header of the encoded vars (bytes [4:6], after the
// 4-byte magic) to one below the program's, which still decodes (the decoder
// accepts any version up to its own) but must be refused by the pairing check.
func TestSafeExecCompiled_VersionMismatchRejected(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := compileScript(mustParse(t, script), script, nil)
	require.NotNil(t, compiled)

	program, decErr := numscriptlib.DecodeCompiledProgram(compiled.Program)
	require.NoError(t, decErr)
	require.Positive(t, program.Version)

	staleVars := bytes.Clone(compiled.Vars)
	binary.LittleEndian.PutUint16(staleVars[4:], program.Version-1)

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(100)}}

	_, err := SafeExecCompiled(NewNumscriptCache(16), compiled.Program, staleVars, NewVMStore(source, false))
	require.NotNil(t, err)

	var runtimeErr *domain.ErrNumscriptRuntime
	require.ErrorAs(t, err, &runtimeErr)
	require.Contains(t, runtimeErr.Detail, "format version mismatch")
}

// TestSafeExecCompiled_MissingFundsClassification: the VM's missing-funds error
// maps to the same domain error the interpreter path raises.
func TestSafeExecCompiled_MissingFundsClassification(t *testing.T) {
	t.Parallel()

	script := `send [COIN 30] (
  source = @src
  destination = @dst
)`
	compiled := compileScript(mustParse(t, script), script, nil)
	require.NotNil(t, compiled)

	source := mapValueSource{balances: map[string]*big.Int{"src\x00COIN\x00": big.NewInt(10)}}

	_, err := SafeExecCompiled(NewNumscriptCache(16), compiled.Program, compiled.Vars, NewVMStore(source, false))
	require.NotNil(t, err)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "COIN", insufficientFunds.Asset)
	require.Equal(t, "30", insufficientFunds.Amount)
	require.Equal(t, "10", insufficientFunds.Balance)
}
