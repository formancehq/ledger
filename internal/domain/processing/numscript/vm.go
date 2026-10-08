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
// bytecode and its hash (HashProgram), the runtime vars encoded against that
// program's variable layout, and the XXH3-128 hash of the exact script text it
// was compiled from. The VM is the only execution engine: the FSM executes
// this artifact on every node that can use it — the committed bytes when they
// travel by value, bytes proven by ProgramHash to be these when they travel
// by reference — and otherwise derives program and vars from the script text
// with its own library (see SafeExecCommitted): a scripted order that arrives
// without an artifact, or a replica whose library version differs from the
// one that produced it. A present artifact this binary can read but not
// decode or verify fails the order loudly (see SafeExecCompiled).
//
// program and vars are the decoded forms of Program and Vars, kept for
// admission's own effects run so it need not decode what it just encoded.
//
// AlreadyCompiled reports whether this exact NumscriptCache instance had
// already compiled this script hash before this call — i.e. whether the
// program half is a cache hit on this entry, not a fresh compile. Admission
// reads it as "the bytecode for this script has been sent by this instance
// before" and then binds ProgramHash to the order instead of Program (see
// admission.go). The signal is approximate by design — it says nothing about
// what any replica has cached — and the FSM tolerates it being wrong either
// way: bytes sent again are a plain by-value apply, and a reference no
// replica can serve from cache is recompiled from the script text (see
// SafeExecCommitted).
type CompiledScript struct {
	Program         []byte
	ProgramHash     []byte
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

	hash, programHash := entry.hash, compiled.encodedHash

	return &CompiledScript{
		// The order's artifact travels into OrderTechnical; give it its own
		// bytes rather than aliasing the entry shared by every order of the script.
		Program:         bytes.Clone(compiled.encoded),
		ProgramHash:     programHash[:],
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
//     run here. A version this library cannot read is not a defect but
//     another library version's artifact (a replica on a different version
//     during a rolling upgrade): SafeExecCommitted routes such an order to
//     the script text before it reaches this function, so one arriving here
//     is a caller bug.
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

// CommittedArtifact is a scripted order's VM artifact as committed in
// OrderTechnical, in one of the two shapes admission produces (classified by
// the caller, see the processing package's classifyCompiledArtifact): Vars
// always set, and exactly one of Program (the bytecode by value) and
// ProgramHash (the bytecode by reference, HashProgram of the bytes).
type CommittedArtifact struct {
	Program     []byte
	ProgramHash [16]byte
	Vars        []byte
}

// readable inspects the header of every half present, before anything is
// decoded. headerErr is set when a header does not even parse (truncated, bad
// magic): that half is not another version's artifact but a corrupt one, and
// it fails the order loudly (invariant #7) with the message the decoder would
// have produced for it. Otherwise readable reports whether this binary's
// bundled library can read the bytecode version of every half present
// (CurrentBytecodeVersion.CanRead). Both halves are inspected before either
// verdict, so a corrupt half is never masked by a foreign version in the
// other one: an artifact that is both corrupt and foreign fails instead of
// being derived from the text, and fails the same way on a replica whose
// library does read the foreign half.
func (a CommittedArtifact) readable() (readable bool, headerErr domain.SerializableError) {
	varsVersion, err := numscriptlib.PeekVarsVersion(a.Vars)
	if err != nil {
		return false, &domain.ErrNumscriptRuntime{
			Detail: "decoding compiled numscript vars: " + err.Error(),
		}
	}

	readable = numscriptlib.CurrentBytecodeVersion.CanRead(varsVersion)

	if len(a.Program) > 0 {
		programVersion, err := numscriptlib.PeekCompiledProgramVersion(a.Program)
		if err != nil {
			return false, &domain.ErrNumscriptRuntime{
				Detail: "decoding compiled numscript program: " + err.Error(),
			}
		}

		readable = readable && numscriptlib.CurrentBytecodeVersion.CanRead(programVersion)
	}

	return readable, nil
}

// SafeExecCommitted executes a committed scripted order on the FSM apply path:
// the committed artifact when this binary can use it, otherwise program and
// vars derived from the script text with this binary's own library
// (SafeExecFromText), exactly as audit replay derives every order. This
// binary can use the artifact when:
//
//   - every half carries a bytecode version its bundled library reads. A
//     version it cannot read means another library version produced the
//     artifact — a replica running a different version during a rolling
//     upgrade — which is the ordinary reason to derive from the text, never
//     a failure. A half whose header does not parse at all is corrupt, not
//     foreign, and fails the order before either verdict, whatever version
//     the other half carries (see readable);
//   - by value, nothing more: the committed bytes run with the committed vars
//     (SafeExecCompiled);
//   - by reference, bytes with the committed program hash are at hand: this
//     node's cache entry for the script (the steady state — the earlier
//     by-value order of the script warmed every replica that applied it), or
//     this binary's own compile of the text when it reproduces the hash
//     (after a restart or an LRU eviction, on a replica that joined later),
//     which is then cached for the next reference. A compile that does not
//     reproduce the hash means another library version compiled the committed
//     bytes; the committed vars were encoded against that program's variable
//     layout and must not run against this one — equal pool sizes with a
//     different layout would post wrong amounts with no error — so the order
//     is derived from the text, vars included.
//
// Within one library version every replica therefore runs the same bytes with
// the same committed vars, whatever its cache holds (invariant #2). Across
// library versions a replica runs its own compilation of the same text with
// vars it encodes itself, and the outcome agrees because the Numscript library
// keeps a script's semantics stable across versions — the contract the store
// checker's audit replay already relies on for every scripted order, and the
// reason a version difference never fails an order here. What does fail
// loudly (ErrNumscriptRuntime) is a half whose header does not parse, an
// artifact this library reads but cannot decode or verify, or committed vars
// the cached program's layout does not cover: corrupt bytes, not a version
// difference. Headers are inspected and vars are decoded before any cache
// access, so a corrupt artifact fails the same way on every replica.
//
// scriptHash must be HashScript(script), which the caller has already checked
// against the order's committed script hash: it keys both cache sides, and
// the parse side would otherwise cache the text under another script's key.
func SafeExecCommitted(cache *NumscriptCache, scriptHash [16]byte, artifact CommittedArtifact, script string, scriptVars map[string]string, store *VMStore) (result numscriptlib.ExecutionResult, err domain.SerializableError) {
	defer func() {
		if panicErr := numscriptPanicToDescribable(recover()); panicErr != nil {
			result = numscriptlib.ExecutionResult{}
			err = panicErr
		}
	}()

	readable, headerErr := artifact.readable()
	if headerErr != nil {
		return numscriptlib.ExecutionResult{}, headerErr
	}

	if !readable {
		return SafeExecFromText(cache, script, scriptVars, store)
	}

	if len(artifact.Program) > 0 {
		return SafeExecCompiled(cache, scriptHash[:], artifact.Program, artifact.Vars, store)
	}

	vars, decErr := numscriptlib.DecodeVars(artifact.Vars)
	if decErr != nil {
		return numscriptlib.ExecutionResult{}, &domain.ErrNumscriptRuntime{
			Detail: "decoding compiled numscript vars: " + decErr.Error(),
		}
	}

	entry, reproduced, resolveErr := cache.resolveByReference(scriptHash, artifact.ProgramHash, script, &vars)
	if resolveErr != nil {
		return numscriptlib.ExecutionResult{}, resolveErr
	}

	if !reproduced {
		return SafeExecFromText(cache, script, scriptVars, store)
	}

	return safeExecVM(entry.vm, &vars, store)
}

// SafeExecFromText compiles script with scriptVars under this binary's bundled
// library, exactly as admission does (CompileForReplay), and executes the
// result, caching the program like any by-value artifact. It serves an order
// that carries no artifact — the store checker's audit replay — and one whose
// artifact this binary cannot use (see SafeExecCommitted). Under the same
// library every compilation of a script means the same thing; across library
// versions the outcome rests on the library keeping the script's semantics
// stable (see docs/ops/deployment.md).
func SafeExecFromText(cache *NumscriptCache, script string, scriptVars map[string]string, store *VMStore) (numscriptlib.ExecutionResult, domain.SerializableError) {
	compiled, compileErr := CompileForReplay(cache, script, scriptVars)
	if compileErr != nil {
		return numscriptlib.ExecutionResult{}, compileErr
	}

	return SafeExecCompiled(cache, compiled.ScriptHash, compiled.Program, compiled.Vars, store)
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
