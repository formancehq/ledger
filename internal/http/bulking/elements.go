package bulking

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// Bulk is a channel of protobuf LedgerAction
type Bulk chan *servicepb.LedgerAction

// NewLedgerActionResult creates a new LedgerActionResult from a log or error
func NewLedgerActionResult(elementID int, log *commonpb.Log, err error) *servicepb.LedgerActionResult {
	result := &servicepb.LedgerActionResult{
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
func HasError(result *servicepb.LedgerActionResult) bool {
	return result.ErrorCode != ""
}
