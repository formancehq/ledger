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

// CheckCreatedTransaction extracts the CreatedTransaction from an Apply response
// AND verifies post-commit volume consistency (balance == input - output) on
// every account it touches. Returns the extracted transaction so callers can
// reuse its fields (TxId, postings, …), or nil if the response did not carry
// one (ambiguous error path).
func CheckCreatedTransaction(resp *servicepb.ApplyResponse, details Details) *commonpb.CreatedTransaction {
	ct := ExtractCreatedTransaction(resp)
	if ct == nil {
		return nil
	}

	CheckPostCommitVolumes(ct.GetTransaction().GetPostCommitVolumes(), details)

	return ct
}
