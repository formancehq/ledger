package vm

import (
	ledger "github.com/formancehq/ledger/internal"
	"github.com/pkg/errors"
)

var (
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
	return errors.Is(err, ledger.ErrInsufficientFund)
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
