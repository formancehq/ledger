package internal

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// ExtractCreatedTransaction extracts the CreatedTransaction from an Apply response.
func ExtractCreatedTransaction(resp *servicepb.ApplyResponse) *commonpb.CreatedTransaction {
	if resp == nil || len(resp.Logs) == 0 {
		return nil
	}

	applyLog := resp.Logs[0].Payload.GetApply()
	if applyLog == nil {
		return nil
	}

	return applyLog.Log.Data.GetCreatedTransaction()
}
