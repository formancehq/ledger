package vm

import (
	"github.com/pkg/errors"
)

var (
	ErrInsufficientFund                            = errors.New("insufficient fund")
	ErrCompilationFailed                           = errors.New("compilation failed")
	ErrInvalidScript                               = errors.New("invalid script")
	ErrScriptFailed                                = errors.New("script exited with error code")
	ErrMetadataOverride                            = errors.New("metadata override")
	ErrResourcesNotInitialized                     = errors.New("resources not initialized")
	ErrBalancesNotInitialized                      = errors.New("balances not initialized")
	ErrResourceNotFound                            = errors.New("resource not found")
	ErrNegativeMonetaryAmount                      = errors.New("negative monetary amount")
	ErrInvalidVars                                 = errors.New("invalid vars")
	ErrResourceResolutionMissingMetadata           = errors.New("missing metadata")
	ErrResourceResolutionInvalidTypeFromExtSources = errors.New("invalid type from external sources")
)

func IsInsufficientFundError(err error) bool {
	return errors.Is(err, ErrInsufficientFund)
}

func IsCompilationFailedError(err error) bool {
	return errors.Is(err, ErrCompilationFailed)
}

func IsMetadataOverrideError(err error) bool {
	return errors.Is(err, ErrMetadataOverride)
}

func IsResourceResolutionMissingMetadataError(err error) bool {
	return errors.Is(err, ErrResourceResolutionMissingMetadata)
}

func IsResourceResolutionInvalidTypeFromExtSourcesError(err error) bool {
	return errors.Is(err, ErrResourceResolutionInvalidTypeFromExtSources)
}

func IsResourcesNotInitializedError(err error) bool {
	return errors.Is(err, ErrResourcesNotInitialized)
}

func IsBalancesNotInitializedError(err error) bool {
	return errors.Is(err, ErrBalancesNotInitialized)
}

func IsResourceNotFoundError(err error) bool {
	return errors.Is(err, ErrResourceNotFound)
}

func IsNegativeMonetaryAmountError(err error) bool {
	return errors.Is(err, ErrNegativeMonetaryAmount)
}

func IsInvalidVarsError(err error) bool {
	return errors.Is(err, ErrInvalidVars)
}

func IsInvalidScriptError(err error) bool {
	return errors.Is(err, ErrInvalidScript)
}

func IsScriptFailedError(err error) bool {
	return errors.Is(err, ErrScriptFailed)
}
