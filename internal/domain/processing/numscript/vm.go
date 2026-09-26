package numscript

import (
	"context"
	"errors"
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

// compileScript compiles a parsed script and binds its vars on the admission
// path. It returns nil on ANY failure — a script the compiler does not support
// yet (e.g. asset scaling), a var value the encoder rejects, or a recovered
// panic — because absence of the artifact only means the FSM executes the
// script with the tree-walking interpreter instead, which produces the
// authoritative outcome (including the client-facing error for a bad var
// value). Compilation is an optimization, never an admission verdict.
func compileScript(parsed numscriptlib.ParseResult, script string, vars map[string]string) (out *CompiledScript) {
	defer func() {
		if recover() != nil {
			out = nil
		}
	}()

	varsEncoder, program, err := parsed.Compile()
	if err != nil {
		return nil
	}

	encodedVars, err := varsEncoder.Encode(vars)
	if err != nil {
		return nil
	}

	hash := HashScript(script)

	return &CompiledScript{
		Program:    program.Encode(),
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
// SafeRun. Decode and verification failures are loud internal errors, not
// client errors: the artifact was produced by our own compiler from a script
// that parsed, so malformed bytes mean a codec or compiler bug (invariant #7),
// and the verifier is what entitles the VM to execute wire-supplied bytecode
// without per-instruction defensive checks.
func SafeExecCompiled(programBytes, varsBytes []byte, store *VMStore) (result numscriptlib.ExecutionResult, err domain.Describable) {
	defer func() {
		if panicErr := numscriptPanicToDescribable(recover()); panicErr != nil {
			result = numscriptlib.ExecutionResult{}
			err = panicErr
		}
	}()

	program, decErr := numscriptlib.DecodeCompiledProgram(programBytes)
	if decErr != nil {
		return numscriptlib.ExecutionResult{}, &domain.ErrNumscriptRuntime{
			Detail: "decoding compiled numscript program: " + decErr.Error(),
		}
	}

	vars, decErr := numscriptlib.DecodeVars(varsBytes)
	if decErr != nil {
		return numscriptlib.ExecutionResult{}, &domain.ErrNumscriptRuntime{
			Detail: "decoding compiled numscript vars: " + decErr.Error(),
		}
	}

	if verifyErr := numscriptlib.VerifyCompiledProgramWithVars(program, &vars); verifyErr != nil {
		return numscriptlib.ExecutionResult{}, &domain.ErrNumscriptRuntime{
			Detail: "verifying compiled numscript program: " + verifyErr.Error(),
		}
	}

	result, execErr := numscriptlib.ExecVm(context.Background(), numscriptlib.NewVm(program), &vars, store)

	return result, convertVMError(execErr)
}

// convertVMError is convertNumscriptError's counterpart for the VM's error
// types: the same missing-funds mapping (the VM error carries no account or
// color either, so ColorKnown stays false), the same Describable pass-through
// (the VM wraps store errors, which Unwrap to the domain sentinel a rejected
// scoped read raises), and the same conservative ErrNumscriptRuntime residue.
// Asset scaling never reaches here: a scaling script does not compile, so it
// has no artifact and runs on the interpreter path.
func convertVMError(err error) domain.Describable {
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

	if d, ok := errors.AsType[domain.Describable](err); ok {
		return d
	}

	return &domain.ErrNumscriptRuntime{Detail: err.Error()}
}
