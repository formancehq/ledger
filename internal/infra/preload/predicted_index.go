package preload

import "github.com/formancehq/ledger/v3/internal/pkg/protowireutil"

// AppendProposalPredictedIndex appends the raw protobuf wire encoding
// of Proposal.predicted_index (field 7, fixed64) to an already-marshaled
// Proposal.
//
// PredictedIndex was zero when the command was pre-marshaled (proto3
// omits zero-valued scalars), so appending it now produces exactly one
// occurrence on the wire. This avoids re-marshaling the entire
// Proposal (which can be megabytes for large batches) while holding
// the proposal lock.
func AppendProposalPredictedIndex(data []byte, index uint64) []byte {
	if index == 0 {
		return data
	}

	return protowireutil.AppendFixed64(data, 7, index)
}
