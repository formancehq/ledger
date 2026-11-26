package internal

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// CreatedTransactionFromLog extracts the CreatedTransaction carried by one log,
// or nil when the log carried none (ambiguous error path).
//
// A wrapper without its Transaction payload is malformed rather than absent:
// callers read Transaction.Id off the result, and a zero id there is
// indistinguishable from a real one. It is reported and treated as nil so a
// caller's nil guard covers it. Every path reaching a CreatedTransaction goes
// through here, including the bulk drivers selecting one log out of many.
func CreatedTransactionFromLog(log *commonpb.Log) *commonpb.CreatedTransaction {
	applyLog := log.GetPayload().GetApply()
	if applyLog == nil {
		return nil
	}

	ct := applyLog.GetLog().GetData().GetCreatedTransaction()
	if ct == nil {
		return nil
	}

	if ct.GetTransaction() == nil {
		assert.Unreachable("CreatedTransaction must carry its transaction payload", nil)

		return nil
	}

	return ct
}

// ExtractCreatedTransaction extracts the CreatedTransaction from the first log
// of an Apply response.
func ExtractCreatedTransaction(resp *servicepb.ApplyResponse) *commonpb.CreatedTransaction {
	if resp == nil || len(resp.GetLogs()) == 0 {
		return nil
	}

	return CreatedTransactionFromLog(resp.GetLogs()[0])
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
