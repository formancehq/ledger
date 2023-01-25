package wallet

import (
	"errors"
)

var (
	ErrWalletNotFound          = errors.New("wallet not found")
	ErrHoldNotFound            = errors.New("hold not found")
	ErrInsufficientFundError   = errors.New("insufficient fund")
	ErrClosedHold              = errors.New("closed hold")
	ErrBalanceAlreadyExists    = errors.New("balance already exists")
	ErrInvalidBalanceName      = errors.New("invalid balance name")
	ErrReservedBalanceName     = errors.New("reserved balance name")
	ErrBalanceNotExists        = errors.New("balance not exists")
	ErrInvalidBalanceSpecified = errors.New("invalid balance specified")
)
