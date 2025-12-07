package http

import (
	"github.com/formancehq/ledger-v3-poc/internal"
)

// LedgerResponse represents a ledger with its bucket name
type LedgerResponse struct {
	ledger.LedgerInfo
	Bucket string `json:"bucket"`
}
