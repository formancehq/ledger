package bulking

import (
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// Bulk is a channel of protobuf BulkElement
type Bulk chan *ledgerpb.BulkElement

// NewBulkElementResult creates a new BulkElementResult from a log or error
func NewBulkElementResult(elementID int, log *ledgerpb.Log, err error) *ledgerpb.BulkElementResult {
	result := &ledgerpb.BulkElementResult{
		ElementId: int32(elementID),
	}
	if err != nil {
		result.ErrorCode = "ERROR"
		result.ErrorDescription = err.Error()
	}
	if log != nil {
		result.LogId = log.Id
		result.Data = &ledgerpb.BulkElementResult_Log{Log: log}
	}
	return result
}

// HasError returns true if the result has an error
func HasError(result *ledgerpb.BulkElementResult) bool {
	return result.ErrorCode != ""
}
