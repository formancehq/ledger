package numscript

import (
	"context"
	"errors"
	"math/big"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// CompiledScript is admission's locally compiled program and bound vars.
// It is used to validate the request and predict effects within a batch.
type CompiledScript struct {
	program    *compiledProgram
	vars       numscriptlib.Vars
	scriptHash [16]byte
}

// compileScript binds an order's vars to its script's compile on the admission
// path. The script-dependent compile is computed once per cached script
// (lruEntry.compileParsed) and shared by every order carrying it; binding
// the vars runs per order. A script the VM
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
		return nil, mapCompilerError(encErr)
	}

	return &CompiledScript{
		program:    compiled,
		vars:       encodedVars,
		scriptHash: entry.hash,
	}, nil
}

// compileFromText compiles script and vars from business input. Admission,
// FSM apply, and audit replay use the same compiler. Agreement across binary
// versions still requires stable script semantics (see docs/ops/deployment.md).
func compileFromText(cache *NumscriptCache, script string, vars map[string]string) (*CompiledScript, domain.SerializableError) {
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
	return safeExecVM(numscriptlib.NewVm(compiled.program.program), &compiled.vars, store)
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

// SafeExecCompiled verifies and executes a locally compiled program,
// recovering library panics into ErrNumscriptRuntime. Verification failures
// are internal errors. The cache keeps one VM instance per local compilation;
// the FSM uses it sequentially, and the library releases its store
// after each run so it cannot retain an old proposal's Scope. Cache hits
// affect performance only.
func SafeExecCompiled(cache *NumscriptCache, compiled *CompiledScript, store *VMStore) (result numscriptlib.ExecutionResult, err domain.SerializableError) {
	defer func() {
		if panicErr := numscriptPanicToDescribable(recover()); panicErr != nil {
			result = numscriptlib.ExecutionResult{}
			err = panicErr
		}
	}()

	entry, err := cache.getOrCreateVM(compiled.scriptHash, compiled.program, &compiled.vars)
	if err != nil {
		return numscriptlib.ExecutionResult{}, err
	}

	return safeExecVM(entry.vm, &compiled.vars, store)
}

// SafeExecFromText compiles the order's script and vars with this binary's
// bundled library and executes them. FSM apply and audit replay both use this
// path. The program is cached for later orders; cache state affects only
// performance. Cross-version agreement requires stable script semantics.
func SafeExecFromText(cache *NumscriptCache, script string, scriptVars map[string]string, store *VMStore) (numscriptlib.ExecutionResult, domain.SerializableError) {
	compiled, compileErr := compileFromText(cache, script, scriptVars)
	if compileErr != nil {
		return numscriptlib.ExecutionResult{}, compileErr
	}

	return SafeExecCompiled(cache, compiled, store)
}

// convertVMError is convertNumscriptError's counterpart for the VM's error
// types: the same missing-funds mapping (the VM error carries no account or
// color either, so ColorKnown stays false), the same typed-failure pass-through
// (the VM wraps store errors, which Unwrap to the domain sentinel a rejected
// scoped read raises), and ErrNumscriptExecution for every failure the script
// itself caused. What remains — VmInternalError (malformed bytecode) and store
// errors with no domain type — is ErrNumscriptRuntime.
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
	if mapped := mapVMError(err); mapped != nil {
		return mapped
	}

	return &domain.ErrNumscriptRuntime{Detail: err.Error()}
}

func mapVMError(err error) domain.SerializableError {
	makeError := func(code string, facts map[string]string) domain.SerializableError {
		return &domain.ErrNumscriptExecution{Detail: err.Error(), Code: code, Facts: facts}
	}
	if e, ok := errors.AsType[numscriptlib.VmNegativeAmountError](err); ok {
		return makeError("NUMSCRIPT_NEGATIVE_AMOUNT", map[string]string{"amount": e.Amount.String()})
	}
	if e, ok := errors.AsType[numscriptlib.VmNegativeBalanceError](err); ok {
		return makeError("NUMSCRIPT_NEGATIVE_BALANCE", map[string]string{"account": e.Account, "amount": e.Amount.String()})
	}
	if e, ok := errors.AsType[numscriptlib.VmAssetMismatchError](err); ok {
		return makeError("NUMSCRIPT_ASSET_MISMATCH", map[string]string{"expected": e.Expected, "got": e.Got})
	}
	if e, ok := errors.AsType[numscriptlib.VmInvalidAllotmentSum](err); ok {
		return makeError("NUMSCRIPT_INVALID_ALLOTMENT_SUM", map[string]string{"actualSum": e.ActualSum.String()})
	}
	if e, ok := errors.AsType[numscriptlib.VmNegativePortionError](err); ok {
		return makeError("NUMSCRIPT_NEGATIVE_PORTION", map[string]string{"portion": e.Portion.String()})
	}
	if e, ok := errors.AsType[numscriptlib.VmDivideByZeroError](err); ok {
		return makeError("NUMSCRIPT_DIVIDE_BY_ZERO", map[string]string{"numerator": e.Numerator.String()})
	}
	if e, ok := errors.AsType[numscriptlib.VmInvalidAccountName](err); ok {
		return makeError("NUMSCRIPT_INVALID_ACCOUNT_NAME", map[string]string{"name": e.Name})
	}
	if e, ok := errors.AsType[numscriptlib.VmInvalidColor](err); ok {
		return makeError("NUMSCRIPT_INVALID_COLOR", map[string]string{"color": e.Color})
	}
	if e, ok := errors.AsType[numscriptlib.VmInvalidScope](err); ok {
		return makeError("NUMSCRIPT_INVALID_SCOPE", map[string]string{"scope": e.Scope})
	}
	if e, ok := errors.AsType[numscriptlib.VmCannotCastScopedAccountToString](err); ok {
		return makeError("NUMSCRIPT_CANNOT_CAST_SCOPED_ACCOUNT", map[string]string{"account": e.Account, "scope": e.Scope})
	}
	if e, ok := errors.AsType[numscriptlib.VmInvalidUncappedSource](err); ok {
		return makeError("NUMSCRIPT_INVALID_UNCAPPED_SOURCE", map[string]string{"account": e.Account})
	}
	if e, ok := errors.AsType[numscriptlib.VmMetadataNotFoundError](err); ok {
		return makeError("NUMSCRIPT_METADATA_NOT_FOUND", map[string]string{"account": e.Account, "key": e.Key})
	}
	if e, ok := errors.AsType[numscriptlib.VmBadMetaValueError](err); ok {
		return makeError("NUMSCRIPT_BAD_META_VALUE", map[string]string{"account": e.Account, "key": e.Key})
	}
	return nil
}
