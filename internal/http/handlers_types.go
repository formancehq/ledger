package http

import (
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

// LedgerResponse represents a ledger with its bucket name
type LedgerResponse struct {
	service.LedgerInfo
	Bucket string `json:"bucket"`
}

