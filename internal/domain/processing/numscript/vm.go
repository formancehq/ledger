package numscript

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// CompiledScript is the Numscript VM artifact admission compiles on the
// leader's parallel path and binds to the order (OrderTechnical): the encoded
// bytecode, the runtime vars encoded against that program's variable layout,
// and the BLAKE3 hash of the exact script text it was compiled from. The FSM
// decodes and executes it on every node instead of re-interpreting the text.
type CompiledScript struct {
	Program    []byte
	Vars       []byte
	ScriptHash []byte
}

// compileScript binds an order's vars to its script's compile on the admission
// path. The script-dependent half — compiling and encoding the bytecode — is
// computed once per cached script (lruEntry.compileParsed) and shared by every
// order carrying it; only the vars encoding runs per order. It returns nil on
// ANY failure — a script the compiler does not support yet (e.g. asset
// scaling), a var value the encoder rejects, or a recovered panic — because
// absence of the artifact only means the FSM executes the script with the
// tree-walking interpreter instead, which produces the authoritative outcome
// (including the client-facing error for a bad var value). Compilation is an
// optimization, never an admission verdict.
func compileScript(entry *lruEntry, vars map[string]string) (out *CompiledScript) {
	defer func() {
		if recover() != nil {
			out = nil
		}
	}()

	compiled := entry.compileParsed()
	if compiled == nil {
		return nil
	}

	encodedVars, err := compiled.varsEncoder.Encode(vars)
	if err != nil {
		return nil
	}

	hash := entry.hash

	return &CompiledScript{
		// The order's artifact travels into OrderTechnical; give it its own
		// bytes rather than aliasing the entry shared by every order of the script.
		Program:    bytes.Clone(compiled.program),
		Vars:       encodedVars.Encode(),
		ScriptHash: hash[:],
	}
}

// NewVMStore adapts a ValueSource to the numscript VM's Store interface, the
// per-key counterpart of the interpreter-facing Store built by NewStore: same
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
// artifact on the FSM apply path, with the same panic-recovery contract as
// SafeRun. Every failure before execution is a loud internal error
// (ErrNumscriptRuntime), not a client error and never a fallback to the
// interpreter: an artifact the FSM cannot execute as-is fails the order the
// same way on every node running this binary, so the outcome stays a pure
// function of the committed entry and the running binary (invariant #2) and
// the defect surfaces instead of being papered over (invariant #7).
//
//   - Bytecode version: both halves must carry exactly the bytecode version
//     (major.minor) the bundled library compiles to,
//     numscriptlib.CurrentBytecodeVersion. The library's own decoder already
//     refuses another major (existing encodings changed meaning — its 1→2
//     bump moved opcode operand banks) and a newer minor (opcodes this build
//     does not know); it would still read an older minor of the same major,
//     since a minor bump is additive by contract, and the ledger deliberately
//     does not execute even that: exact match is the conservative default
//     until a minor bump has actually been exercised, and relaxing it to the
//     library's CanRead is a one-line decision. An artifact of another
//     version came from another binary — a Raft log replayed across a library
//     upgrade, a rollback, a mixed-binary window — and is rejected rather than
//     run as foreign bytecode. Both versions come out of the committed entry's
//     own bytes, and the bundled version is a property of the binary exactly
//     like the library's execution semantics, so apply stays a pure function
//     of (committed entry, running binary).
//   - Decode and verification: the artifact was produced by our own compiler
//     from a script that parsed, so malformed bytes mean a codec or compiler
//     bug, and the verifier is what entitles the VM to execute wire-supplied
//     bytecode without per-instruction defensive checks.
//
// Decode and verification go through cache: the verifier is a static pass over
// the whole program, orders of magnitude more expensive than execution itself,
// and its outcome for a given artifact never changes (the vars pool sizes it
// checks LoadVar indices against are fixed by the program's own variable
// layout, not by the per-order values). Caching it is what makes the VM path
// cheaper than the interpreter per apply, exactly as the parse cache does for
// the interpreter path. Execution reuses the entry's single warm VM instance
// (see compiledLruEntry for the reuse contract: always safe sequentially,
// never concurrently). Cache and warm instance alike only move work, never
// results, so apply stays deterministic. scriptHash is the cache key: the
// order's HashScript(text), which the caller has already checked against the
// resolved text (see getOrDecodeCompiled for why it keys the cache).
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

	if vars.Version != numscriptlib.CurrentBytecodeVersion {
		return numscriptlib.ExecutionResult{}, &domain.ErrNumscriptRuntime{
			Detail: fmt.Sprintf(
				"compiled numscript vars encoded with bytecode version %s; this binary executes %s only",
				vars.Version, numscriptlib.CurrentBytecodeVersion,
			),
		}
	}

	entry, err := cache.getOrDecodeCompiled(scriptHash, programBytes, &vars)
	if err != nil {
		return numscriptlib.ExecutionResult{}, err
	}

	result, execErr := numscriptlib.ExecVm(context.Background(), entry.vm, &vars, store)

	return result, convertVMError(execErr)
}

// convertVMError is convertNumscriptError's counterpart for the VM's error
// types: the same missing-funds mapping (the VM error carries no account or
// color either, so ColorKnown stays false), the same typed-failure pass-through
// (the VM wraps store errors, which Unwrap to the domain sentinel a rejected
// scoped read raises), and the same conservative ErrNumscriptRuntime residue.
// Asset scaling never reaches here: a scaling script does not compile, so it
// has no artifact and runs on the interpreter path.
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
