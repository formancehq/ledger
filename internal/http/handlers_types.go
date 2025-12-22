package http

import (
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// LedgerResponse represents a ledger
type LedgerResponse struct {
	*ledgerpb.LedgerInfo
}
