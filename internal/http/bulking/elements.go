package bulking

import (
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// Bulk is a channel of protobuf LedgerAction
type Bulk chan *ledgerpb.LedgerAction

// NewLedgerActionResult creates a new LedgerActionResult from a log or error
func NewLedgerActionResult(elementID int, log *ledgerpb.Log, err error) *ledgerpb.LedgerActionResult {
	result := &ledgerpb.LedgerActionResult{
		ElementId: int32(elementID),
	}
	if err != nil {
		result.ErrorCode = "ERROR"
		result.ErrorDescription = err.Error()
	}
	if log != nil {
		result.LogId = log.Id
		result.Log = log
	}
	return result
}

// HasError returns true if the result has an error
func HasError(result *ledgerpb.LedgerActionResult) bool {
	return result.ErrorCode != ""
}
