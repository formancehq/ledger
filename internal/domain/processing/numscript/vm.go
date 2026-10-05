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
// and the BLAKE3 hash of the exact script text it was compiled from. The VM is
// the only execution engine: the FSM decodes and executes this artifact on
// every node, and recompiles it from the script text (CompileForReplay) for a
// scripted order that arrives without one, or with one of a bytecode version
// the bundled library cannot read (numscriptlib.CurrentBytecodeVersion.CanRead).
//
// program and vars are the decoded forms of Program and Vars, kept for
// admission's own effects run so it need not decode what it just encoded.
type CompiledScript struct {
	Program    []byte
	Vars       []byte
	ScriptHash []byte

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

	compiled, compileErr := entry.compileParsed()
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
		Program:    bytes.Clone(compiled.encoded),
		Vars:       encodedVars.Encode(),
		ScriptHash: hash[:],
		program:    compiled.program,
		vars:       encodedVars,
	}, nil
}

// CompileForReplay compiles script with vars exactly as admission does, for a
// caller that re-runs an audited order, or applies a committed order whose
// artifact carries a bytecode version this binary cannot read. The audit keeps
// only the business part of an order, so an audited order never carries the
// compiled code. Under the
// same bundled library every compilation of a script means the same thing, so
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
//   - Bytecode version: the apply path never hands this function an artifact
//     the bundled library cannot read (numscriptlib.CurrentBytecodeVersion.CanRead
//     on each half's peeked header): it recompiles such an artifact from the
//     script text first.
//     The library's decoders still refuse one with
//     UnsupportedBytecodeVersionError, so a caller that skipped that check
//     fails loudly here rather than run foreign bytecode.
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
// never concurrently). Cache and warm instance alike only move work, never
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
