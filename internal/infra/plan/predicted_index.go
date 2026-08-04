package plan

import (
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/pkg/protowireutil"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// predictedIndexField is Proposal.predicted_index's wire number, read from the
// descriptor rather than hardcoded: this file hand-rolls the wire encoding, so
// a renumbering in raft_cmd.proto would otherwise silently append the value
// under whatever field now owns the old number, with nothing to catch it at
// compile time.
var predictedIndexField = func() protowire.Number {
	fd := (&raftcmdpb.Proposal{}).ProtoReflect().Descriptor().Fields().ByName("predicted_index")
	if fd == nil {
		panic("invariant: raft.Proposal has no predicted_index field")
	}

	return fd.Number()
}()

// AppendProposalPredictedIndex appends the raw protobuf wire encoding
// of Proposal.predicted_index (fixed64) to an already-marshaled Proposal.
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

	return protowireutil.AppendFixed64(data, predictedIndexField, index)
}
