package bulking

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// Bulk is a channel of protobuf LedgerAction
type Bulk chan *servicepb.LedgerAction

// LedgerActionResult represents the result of a single ledger action
type LedgerActionResult struct {
	ElementID        int
	LogID            uint64
	ErrorCode        string
	ErrorDescription string
	Log              *commonpb.LedgerLog
}

// NewLedgerActionResult creates a new LedgerActionResult from a log or error
func NewLedgerActionResult(elementID int, log *commonpb.LedgerLog, err error) *LedgerActionResult {
	result := &LedgerActionResult{
		ElementID: elementID,
	}
	if err != nil {
		result.ErrorCode = "ERROR"
		result.ErrorDescription = err.Error()
	}
	if log != nil {
		result.LogID = log.Id
		result.Log = log
	}
	return result
}

// HasError returns true if the result has an error
func (r *LedgerActionResult) HasError() bool {
	return r.ErrorCode != ""
}
