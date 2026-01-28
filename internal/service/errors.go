package service

import "errors"

var (
	ErrIdempotencyKeyConflict = errors.New("idempotency key conflict")
	ErrInsufficientFunds      = errors.New("insufficient funds")
)
