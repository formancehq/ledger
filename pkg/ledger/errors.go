package ledger

import (
	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/formancehq/ledger/pkg/ledger/state"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/pkg/errors"
)

var (
	// Specific Storage errors
	ErrNotFound  = errors.New("not found")
	ErrMigration = errors.New("migration error")
	// All other non-specific Storage errors
	ErrStorage = errors.New("storage error")

	// All validation errors when executing ledger functions
	ErrValidation = errors.New("validation error")

	// All cache errors
	ErrCache = errors.New("cache error")

	// All lock errors
	ErrLock = errors.New("lock error")

	// All query errors
	ErrQuery = errors.New("query error")

	// Specific Runner errors
	ErrNoPostings                = errors.New("transaction has no postings")
	ErrConflict                  = errors.New("conflict error")
	ErrPastTransaction           = errors.New("cannot pass a timestamp prior to the last transaction")
	ErrNoScript                  = errors.New("no script")
	ErrCompilationFailed         = errors.New("compilation failed")
	ErrInsufficientFund          = errors.New("insufficient fund")
	ErrScriptMetadataOverride    = errors.New("script metadata override")
	ErrInvalidResourceResolution = errors.New("invalid resource resolution")
	// All other non-specific Runner errors
	ErrRunner = errors.New("runner error")
)

func RunnerErrorToLedgerError(err error) error {
	switch {
	case runner.IsNoPostingsError(err):
		return errorsutil.NewError(ErrNoPostings, err)
	case runner.IsConflictError(err):
		return errorsutil.NewError(ErrConflict, err)
	case runner.IsPastTransactionError(err):
		return errorsutil.NewError(ErrPastTransaction, err)
	case runner.IsNoScriptError(err):
		return errorsutil.NewError(ErrNoScript, err)
	case runner.IsCompilationFailedError(err):
		return errorsutil.NewError(ErrCompilationFailed, err)
	case runner.IsInsufficientFundError(err):
		return errorsutil.NewError(ErrInsufficientFund, err)
	case runner.IsScriptMetadataOverrideError(err):
		return errorsutil.NewError(ErrScriptMetadataOverride, err)
	case runner.IsInvalidResourceResolutionError(err):
		return errorsutil.NewError(ErrInvalidResourceResolution, err)
	case state.IsStorageError(err):
		return errorsutil.NewError(ErrStorage, err)
	default:
		return errorsutil.NewError(ErrRunner, err)
	}
}

func IsNotFoundError(err error) bool {
	return errors.Is(err, ErrNotFound)
}

func IsValidationError(err error) bool {
	return errors.Is(err, ErrValidation)
}

func IsRunnerError(err error) bool {
	return errors.Is(err, ErrRunner)
}

func IsStorageError(err error) bool {
	return errors.Is(err, ErrStorage)
}

func IsQueryError(err error) bool {
	return errors.Is(err, ErrQuery)
}

func IsCacheError(err error) bool {
	return errors.Is(err, ErrCache)
}

func IsLockError(err error) bool {
	return errors.Is(err, ErrLock)
}

func IsMigrationError(err error) bool {
	return errors.Is(err, ErrMigration)
}

func IsPastTransactionError(err error) bool {
	return errors.Is(err, ErrPastTransaction)
}

func IsConflictError(err error) bool {
	return errors.Is(err, ErrConflict)
}

func IsNoPostingsError(err error) bool {
	return errors.Is(err, ErrNoPostings)
}

func IsNoScriptError(err error) bool {
	return errors.Is(err, ErrNoScript)
}

func IsCompilationFailedError(err error) bool {
	return errors.Is(err, ErrCompilationFailed)
}

func IsInsufficientFundError(err error) bool {
	return errors.Is(err, ErrInsufficientFund)
}

func IsScriptMetadataOverrideError(err error) bool {
	return errors.Is(err, ErrScriptMetadataOverride)
}

func IsInvalidResourceResolutionError(err error) bool {
	return errors.Is(err, ErrInvalidResourceResolution)
}
