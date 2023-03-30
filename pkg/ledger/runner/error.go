package runner

import (
	"github.com/formancehq/ledger/pkg/ledger/state"
	"github.com/formancehq/ledger/pkg/machine/vm"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/pkg/errors"
)

var (
	// Specific VM Errors
	ErrNoScript                  = errors.New("no script")
	ErrCompilationFailed         = errors.New("compilation failed")
	ErrInsufficientFund          = errors.New("insufficient fund")
	ErrScriptMetadataOverride    = errors.New("script metadata override")
	ErrInvalidResourceResolution = errors.New("invalid resource resolution")
	// All other non-specific VM Errors
	ErrVM = errors.New("vm error")

	// Specific Runner Errors
	ErrNoPostings = errors.New("transaction has no postings")
	// All other non-specific Runner Errors
	ErrRunner = errors.New("runner other errors")

	// All storage errors
	ErrStorage = errors.New("storage error")

	// Specific State Errors
	ErrConflict        = errors.New("conflict error")
	ErrPastTransaction = errors.New("cannot pass a timestamp prior to the last transaction")
	// All other non-specific State Errors
	ErrState = errors.New("state error")
)

func StateErrorToRunnerError(err error) error {
	switch {
	case state.IsConflictError(err):
		return errorsutil.NewError(ErrConflict, err)
	case state.IsPastTransaction(err):
		return errorsutil.NewError(ErrPastTransaction, err)
	case state.IsStorageError(err):
		return errorsutil.NewError(ErrStorage, err)
	default:
		return errorsutil.NewError(ErrState, err)
	}
}

func VMErrorToRunnerError(err error) error {
	switch {
	case vm.IsCompilationFailedError(err):
		return errorsutil.NewError(ErrCompilationFailed, err)
	case vm.IsInsufficientFundError(err):
		return errorsutil.NewError(ErrInsufficientFund, err)
	case vm.IsMetadataOverrideError(err):
		return errorsutil.NewError(ErrScriptMetadataOverride, err)
	case vm.IsResourceResolutionInvalidTypeFromExtSourcesError(err),
		vm.IsResourceResolutionMissingMetadataError(err):
		return errorsutil.NewError(ErrInvalidResourceResolution, err)
	default:
		return errorsutil.NewError(ErrVM, err)
	}
}

func IsNoScriptError(err error) bool {
	return errors.Is(err, ErrNoScript)
}

func IsCompilationFailedError(err error) bool {
	return errors.Is(err, ErrCompilationFailed)
}

func IsNoPostingsError(err error) bool {
	return errors.Is(err, ErrNoPostings)
}

func IsStateError(err error) bool {
	return errors.Is(err, ErrState)
}

func IsPastTransactionError(err error) bool {
	return errors.Is(err, ErrPastTransaction)
}

func IsConflictError(err error) bool {
	return errors.Is(err, ErrConflict)
}

func IsStorageError(err error) bool {
	return errors.Is(err, ErrStorage)
}

func IsScriptMetadataOverrideError(err error) bool {
	return errors.Is(err, ErrScriptMetadataOverride)
}

func IsInsufficientFundError(err error) bool {
	return errors.Is(err, ErrInsufficientFund)
}

func IsRunnerError(err error) bool {
	return errors.Is(err, ErrRunner)
}

func IsVMError(err error) bool {
	return errors.Is(err, ErrVM)
}

func IsInvalidResourceResolutionError(err error) bool {
	return errors.Is(err, ErrInvalidResourceResolution)
}
