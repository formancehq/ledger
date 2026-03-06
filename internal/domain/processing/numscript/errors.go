package numscript

import (
	"errors"
	"fmt"
)

// ErrScriptRequired is returned when a Numscript payload has no script content.
var ErrScriptRequired = errors.New("numscript: script is required")

// ErrNumscriptParse is returned when a Numscript program has syntax errors.
type ErrNumscriptParse struct {
	Details string
}

func (e *ErrNumscriptParse) Error() string {
	return "numscript parse error: " + e.Details
}

// ErrNonDeterministicScript is returned when a Numscript script calls
// GetBalances or GetAccountsMetadata more than once during discovery.
// Deterministic scripts must declare all their reads in a single batch.
type ErrNonDeterministicScript struct {
	Method string // "GetBalances" or "GetAccountsMetadata"
}

func (e *ErrNonDeterministicScript) Error() string {
	return fmt.Sprintf("non-deterministic script: %s called more than once", e.Method)
}

// ErrBalanceNotPreloaded is returned when the balance for an account was not
// preloaded by the admission layer before script execution.
type ErrBalanceNotPreloaded struct {
	Account string
	Asset   string
}

func (e *ErrBalanceNotPreloaded) Error() string {
	return fmt.Sprintf("balance not preloaded for account %q asset %q", e.Account, e.Asset)
}
