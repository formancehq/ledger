package numscript

import (
	"fmt"
)

// ErrNonDeterministicScript is returned when a Numscript script calls
// GetBalances or GetAccountsMetadata more than once during discovery.
// Deterministic scripts must declare all their reads in a single batch.
type ErrNonDeterministicScript struct {
	Method string // "GetBalances" or "GetAccountsMetadata"
}

func (e *ErrNonDeterministicScript) Error() string {
	return fmt.Sprintf("non-deterministic script: %s called more than once", e.Method)
}
