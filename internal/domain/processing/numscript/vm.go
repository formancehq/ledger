package numscript

import (
	"bytes"
	"context"
	"errors"
	"math/big"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// CompiledScript is the Numscript VM artifact admission compiles on the
// leader's parallel path and binds to the order (OrderTechnical): the encoded
// bytecode, the runtime vars encoded against that program's variable layout,
// and the XXH3-128 hash of the exact script text it was compiled from. The VM is
// the only execution engine: the FSM decodes and executes this artifact on
// every node, and recompiles it from the script text (CompileForReplay) only
// for a scripted order that arrives without one. A present artifact the FSM
// cannot run fails the order loudly (see SafeExecCompiled).
//
// program and vars are the decoded forms of Program and Vars, kept for
// admission's own effects run so it need not decode what it just encoded.
//
// AlreadyCompiled reports whether this exact NumscriptCache instance had
// already compiled this script hash before this call — i.e. whether the
// program half is a cache hit on this entry, not a fresh compile. Admission
// uses it to decide whether a proposal can omit Program and keep only Vars
// and ScriptHash: a false positive or negative here is tolerated by design
// (see admission.go) — the FSM apply path recompiles from the script text
// whenever a program it needs turns out not to be cached, the same fallback
// already used for a fully missing artifact.
type CompiledScript struct {
	Program         []byte
	Vars            []byte
	ScriptHash      []byte
	AlreadyCompiled bool

	program numscriptlib.CompiledProgram
	vars    numscriptlib.Vars
}

// compileScript binds an order's vars to its script's compile on the admission
// path. The script-dependent half — compiling and encoding the bytecode — is
// computed once per cached script (lruEntry.compileParsed) and shared by every
// order carrying it; only the vars encoding runs per order. A script the VM
// cannot run is an admission rejection: ErrNumscriptCompile when the compiler
// rejects the script or a var value does not bind to its layout (both
// deterministic, freezable validation failures), a panicError (IsPanic) when
// the library panics.
func compileScript(entry *lruEntry, vars map[string]string) (out *CompiledScript, err domain.SerializableError) {
	defer func() {
		if panicErr := numscriptPanicToDescribable(recover()); panicErr != nil {
			out = nil
			err = panicErr
		}
	}()

	compiled, compileErr, alreadyCompiled := entry.compileParsed()
	if compileErr != nil {
		return nil, compileErr
	}

	encodedVars, encErr := compiled.varsEncoder.Encode(vars)
	if encErr != nil {
		return nil, &domain.ErrNumscriptCompile{Detail: encErr.Error()}
	}

	hash := entry.hash

	return &CompiledScript{
		// The order's artifact travels into OrderTechnical; give it its own
		// bytes rather than aliasing the entry shared by every order of the script.
		Program:         bytes.Clone(compiled.encoded),
		Vars:            encodedVars.Encode(),
		ScriptHash:      hash[:],
		AlreadyCompiled: alreadyCompiled,
		program:         compiled.program,
		vars:            encodedVars,
	}, nil
}

// CompileForReplay compiles script with vars exactly as admission does, for a
// caller that re-runs an audited order, or applies a committed one that
// arrived without its artifact. The audit keeps only the business part of an
// order, so an audited order never carries the compiled code. Under the same
// bundled library every compilation of a script means the same thing, so
// running this one gives the order its original outcome; history applied by a
// library with different execution semantics can replay differently (see
// docs/ops/deployment.md, "Upgrading across the Numscript VM execution
// change").
func CompileForReplay(cache *NumscriptCache, script string, vars map[string]string) (*CompiledScript, domain.SerializableError) {
	entry := cache.getOrParseEntry(script)
	if entry.script.err != nil {
		return nil, entry.script.err
	}

	return compileScript(entry, vars)
}

// execCompiledScript runs an admission-compiled script on a fresh VM instance.
// Admission executes concurrently, so it never touches the FSM's warm
// instances (which must not run concurrently — see compiledLruEntry); the
// program itself is immutable and safe to share across instances.
func execCompiledScript(compiled *CompiledScript, store *VMStore) (numscriptlib.ExecutionResult, domain.SerializableError) {
	return safeExecVM(numscriptlib.NewVm(compiled.program), &compiled.vars, store)
}

// safeExecVM executes a VM instance with the panic-recovery and
// error-conversion contract shared by every VM execution.
func safeExecVM(vm *numscriptlib.Vm, vars *numscriptlib.Vars, store *VMStore) (result numscriptlib.ExecutionResult, err domain.SerializableError) {
	defer func() {
		if panicErr := numscriptPanicToDescribable(recover()); panicErr != nil {
			result = numscriptlib.ExecutionResult{}
			err = panicErr
		}
	}()

	result, execErr := numscriptlib.ExecVm(context.Background(), vm, vars, store)

	return result, convertVMError(execErr)
}

// NewVMStore adapts a ValueSource to the numscript VM's Store interface, the
// per-key counterpart of the resolver-facing Store built by NewStore: same
// ValueSource, same force semantics (unlimited balances, real metadata), same
// scope rejection — a scope view of (account, asset) would collapse onto the
// single volume (EN-1406 P1-2), and account metadata has no scope dimension.
func NewVMStore(source ValueSource, force bool) *VMStore {
	return &VMStore{source: source, force: force}
}

type VMStore struct {
	source ValueSource
	force  bool
}

func (s *VMStore) GetBalance(_ context.Context, account, scope, asset, color string) (*big.Int, error) {
	if scope != "" {
		return nil, domain.ErrScopedBalanceUnsupported
	}

	if s.force {
		return new(big.Int).Set(MaxForceBalance), nil
	}

	balance, err := s.source.Balance(account, asset, color)
	if err != nil {
		return nil, err
	}

	if balance == nil {
		balance = new(big.Int)
	}

	return balance, nil
}

func (s *VMStore) GetMetadata(_ context.Context, account, scope, key string) (string, bool, error) {
	if scope != "" {
		return "", false, domain.ErrScopedBalanceUnsupported
	}

	return s.source.Metadata(account, key)
}

// SafeExecCompiled decodes, verifies and executes an admission-compiled
// artifact on the FSM apply path, recovering library panics into ErrNumscriptRuntime.
// Every failure before execution is a loud internal error
// (ErrNumscriptRuntime), not a client error: the VM is the only engine, so an
// artifact the FSM cannot execute as-is fails the order the
// same way on every node running this binary, so the outcome stays a pure
// function of the committed entry and the running binary (invariant #2) and
// the defect surfaces instead of being papered over (invariant #7).
//
//   - Header and bytecode version: the library's decoders refuse a half with
//     an invalid header, or of a bytecode version the bundled library cannot
//     read (UnsupportedBytecodeVersionError), so foreign bytecode is never
//     run. Admission produces the artifact with this very library and v3 has
//     no cross-version replay contract yet, so neither is repaired from the
//     script text.
//   - Decode and verification: the artifact was produced by our own compiler
//     from a script that parsed, so malformed bytes mean a codec or compiler
//     bug, and the verifier is what entitles the VM to execute wire-supplied
//     bytecode without per-instruction defensive checks.
//
// Decode and verification go through cache, once per artifact: the
// verification outcome for a given artifact never changes (the vars pool sizes
// it checks LoadVar indices against are fixed by the program's own variable
// layout, not by the per-order values). Execution reuses the entry's single warm VM instance
// (see compiledLruEntry for the reuse contract: always safe sequentially,
// never concurrently); the library releases store when the run returns, so the
// cached instance never keeps it, nor the apply Scope it reaches. Cache and
// warm instance alike only move work, never
// results, so apply stays deterministic: a cached entry serves only the exact
// program bytes it verified, so the node always runs the committed artifact.
// scriptHash is the cache key: the order's HashScript(text), which the caller
// has already checked against the resolved text (see getOrDecodeCompiled).
func SafeExecCompiled(cache *NumscriptCache, scriptHash, programBytes, varsBytes []byte, store *VMStore) (result numscriptlib.ExecutionResult, err domain.SerializableError) {
	defer func() {
		if panicErr := numscriptPanicToDescribable(recover()); panicErr != nil {
			result = numscriptlib.ExecutionResult{}
			err = panicErr
		}
	}()

	vars, decErr := numscriptlib.DecodeVars(varsBytes)
	if decErr != nil {
		return numscriptlib.ExecutionResult{}, &domain.ErrNumscriptRuntime{
			Detail: "decoding compiled numscript vars: " + decErr.Error(),
		}
	}

	entry, err := cache.getOrDecodeCompiled(scriptHash, programBytes, &vars)
	if err != nil {
		return numscriptlib.ExecutionResult{}, err
	}

	return safeExecVM(entry.vm, &vars, store)
}

// SafeExecIfCached runs scriptHash's bytecode directly from this node's own
// apply-side cache when present, for a committed entry that carries no
// program bytes of its own — admission omitted them because its own compile
// cache already had this hash compiled (see admission.go and
// docs/technical/architecture/subsystems/scripting/numscript-library.md).
// Unlike SafeExecCompiled there is no program of the caller's own to verify
// a hit against, so this never clones or compares program bytes: a hit's
// cached bytes are used as-is. found is false on a cache miss (restart, LRU
// eviction, a newly joined replica, or a leadership change before this node
// ever applied the hash-establishing entry); the caller must then recompile
// from the script text (CompileForReplay) and execute the ordinary
// SafeExecCompiled, which also warms this cache for next time.
func SafeExecIfCached(cache *NumscriptCache, scriptHash [16]byte, varsBytes []byte, store *VMStore) (result numscriptlib.ExecutionResult, err domain.SerializableError, found bool) {
	defer func() {
		if panicErr := numscriptPanicToDescribable(recover()); panicErr != nil {
			result = numscriptlib.ExecutionResult{}
			err = panicErr
		}
	}()

	cache.compiledMu.RLock()
	elem, ok := cache.compiledCache[scriptHash]
	cache.compiledMu.RUnlock()

	if !ok {
		return numscriptlib.ExecutionResult{}, nil, false
	}

	entry, _ := elem.Value.(*compiledLruEntry)

	vars, decErr := numscriptlib.DecodeVars(varsBytes)
	if decErr != nil {
		return numscriptlib.ExecutionResult{}, &domain.ErrNumscriptRuntime{
			Detail: "decoding compiled numscript vars: " + decErr.Error(),
		}, true
	}

	if !entry.verified.CheckVars(&vars) {
		if _, verifyErr := numscriptlib.VerifyCompiledProgramWithVars(entry.vm.Program, &vars); verifyErr != nil {
			return numscriptlib.ExecutionResult{}, &domain.ErrNumscriptRuntime{
				Detail: "verifying compiled numscript program: " + verifyErr.Error(),
			}, true
		}
	}

	result, err = safeExecVM(entry.vm, &vars, store)

	return result, err, true
}

// convertVMError is convertNumscriptError's counterpart for the VM's error
// types: the same missing-funds mapping (the VM error carries no account or
// color either, so ColorKnown stays false), the same typed-failure pass-through
// (the VM wraps store errors, which Unwrap to the domain sentinel a rejected
// scoped read raises), and the same conservative ErrNumscriptRuntime residue.
// Asset scaling never reaches here: dependency resolution rejects a scaling
// script at admission (ErrNumscriptScalingUnsupported), before compilation.
func convertVMError(err error) domain.SerializableError {
	if err == nil {
		return nil
	}

	if missingFunds, ok := errors.AsType[numscriptlib.VmMissingFundsError](err); ok {
		return &domain.ErrInsufficientFunds{
			Asset:   missingFunds.Asset,
			Amount:  missingFunds.Needed.String(),
			Balance: missingFunds.Got.String(),
		}
	}

	if d, ok := errors.AsType[domain.SerializableError](err); ok {
		return d
	}

	return &domain.ErrNumscriptRuntime{Detail: err.Error()}
}
