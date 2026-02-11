package processing

import (
	"errors"
	"fmt"
	"math/big"
)

// Reason constants shared between server and client for gRPC error mapping.
const (
	ErrReasonLedgerAlreadyExists          = "LEDGER_ALREADY_EXISTS"
	ErrReasonLedgerNotFound               = "LEDGER_NOT_FOUND"
	ErrReasonIdempotencyKeyConflict       = "IDEMPOTENCY_KEY_CONFLICT"
	ErrReasonTransactionReferenceConflict = "TRANSACTION_REFERENCE_CONFLICT"
	ErrReasonTransactionNotFound          = "TRANSACTION_NOT_FOUND"
	ErrReasonTransactionAlreadyReverted   = "TRANSACTION_ALREADY_REVERTED"
	ErrReasonInsufficientFunds            = "INSUFFICIENT_FUNDS"
	ErrReasonBalanceNotFound              = "BALANCE_NOT_FOUND"
	ErrReasonBalanceNotPreloaded          = "BALANCE_NOT_PRELOADED"
	ErrReasonNumscriptParseError          = "NUMSCRIPT_PARSE_ERROR"
	ErrReasonValidation                   = "VALIDATION"
)

// BusinessError wraps a processing error to distinguish it from infrastructure errors.
// It flows through futures → admission → controller → gRPC server, where the interceptor
// maps it to proper gRPC status codes.
type BusinessError struct {
	Err error
}

func (e *BusinessError) Error() string {
	return e.Err.Error()
}

func (e *BusinessError) Unwrap() error {
	return e.Err
}

// Sentinel validation errors (no context needed).
var (
	ErrTargetRequired      = errors.New("target is required")
	ErrMetadataKeyRequired = errors.New("key is required")
	ErrScriptRequired      = errors.New("numscript: script is required")
)

// ErrLedgerAlreadyExists is returned when attempting to create a ledger that already exists.
type ErrLedgerAlreadyExists struct {
	Name string
}

func (e *ErrLedgerAlreadyExists) Error() string {
	return fmt.Sprintf("ledger already exists: %s", e.Name)
}

// ErrLedgerNotFound is returned when a referenced ledger does not exist.
type ErrLedgerNotFound struct {
	Name string
}

func (e *ErrLedgerNotFound) Error() string {
	return fmt.Sprintf("ledger does not exist: %s", e.Name)
}

// ErrIdempotencyKeyConflict is returned when an idempotency key is reused with different content.
type ErrIdempotencyKeyConflict struct {
	Key string
}

func (e *ErrIdempotencyKeyConflict) Error() string {
	return fmt.Sprintf("idempotency key conflict: key %q used with different request content", e.Key)
}

// ErrTransactionReferenceConflict is returned when a transaction reference already exists in the same ledger.
type ErrTransactionReferenceConflict struct {
	LedgerID  uint32
	Reference string
}

func (e *ErrTransactionReferenceConflict) Error() string {
	return fmt.Sprintf("transaction reference %q already exists in ledger %d", e.Reference, e.LedgerID)
}

// ErrTransactionNotFound is returned when a transaction ID is beyond the known range.
type ErrTransactionNotFound struct {
	TransactionID uint64
}

func (e *ErrTransactionNotFound) Error() string {
	return fmt.Sprintf("transaction %d does not exist", e.TransactionID)
}

// ErrTransactionAlreadyReverted is returned when attempting to revert an already-reverted transaction.
type ErrTransactionAlreadyReverted struct {
	TransactionID uint64
}

func (e *ErrTransactionAlreadyReverted) Error() string {
	return fmt.Sprintf("transaction %d is already reverted", e.TransactionID)
}

// ErrInsufficientFunds is returned when a source account does not have enough balance.
type ErrInsufficientFunds struct {
	Account string
	Asset   string
	Amount  *big.Int // requested amount
	Balance *big.Int // available balance (input - output)
}

func (e *ErrInsufficientFunds) Error() string {
	return fmt.Sprintf(
		"insufficient funds on account %q for asset %s: needed %s, available %s",
		e.Account, e.Asset, e.Amount, e.Balance,
	)
}

// ErrBalanceNotFound is returned when the balance for a source account cannot be determined.
type ErrBalanceNotFound struct {
	Account string
	Asset   string
}

func (e *ErrBalanceNotFound) Error() string {
	return fmt.Sprintf("balance not found for account %q asset %q", e.Account, e.Asset)
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

// ErrNumscriptParse is returned when a Numscript program has syntax errors.
type ErrNumscriptParse struct {
	Details string
}

func (e *ErrNumscriptParse) Error() string {
	return fmt.Sprintf("numscript parse error: %s", e.Details)
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
