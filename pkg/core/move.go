package core

import (
	"math/big"
)

type Move struct {
	TransactionID uint64
	Amount        *big.Int
	Asset         string
	Account       string
	PostingIndex  uint8
	IsSource      bool
	Timestamp     Time
}
