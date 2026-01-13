package service

import "errors"

var (
	ErrCompilationFailed            = errors.New("compilation failed")
	ErrMetadataOverride             = errors.New("metadata override")
	ErrInvalidVars                  = errors.New("invalid vars")
	ErrTransactionReferenceConflict = errors.New("transaction reference conflict")
	ErrIdempotencyKeyConflict       = errors.New("idempotency key conflict")
	ErrInsufficientFunds            = errors.New("insufficient funds")
	ErrAlreadyReverted              = errors.New("already reverted")
	ErrImport                       = errors.New("import error")
)
