package state

import (
	"fmt"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// IsCheckpointTriggerOrder reports whether o is a checkpoint-triggering order
// (CreateQueryCheckpoint or CloseChapter). These orders cause the FSM to enter
// a maintenance window after their batch is committed. Both live under the
// system-scoped wrapper.
func IsCheckpointTriggerOrder(o *raftcmdpb.Order) bool {
	system := o.GetSystemScoped()
	if system == nil {
		return false
	}

	switch system.GetPayload().(type) {
	case *raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint,
		*raftcmdpb.SystemScopedOrder_CloseChapter:
		return true
	default:
		return false
	}
}

// CheckpointOrderPosition describes where a checkpoint-trigger order appears
// in a proposal's order list.
type CheckpointOrderPosition int

const (
	// CheckpointOrderAbsent indicates no checkpoint-trigger order is present.
	CheckpointOrderAbsent CheckpointOrderPosition = iota
	// CheckpointOrderLast indicates a checkpoint-trigger order is present and
	// occupies the last slot of the proposal's order list. This is the only
	// valid placement.
	CheckpointOrderLast
	// CheckpointOrderInvalid indicates a checkpoint-trigger order is present
	// at a position other than the last. This is forbidden: it would force the
	// FSM to commit mid-batch and race the pipelined committer.
	CheckpointOrderInvalid
)

// ClassifyCheckpointOrderPosition inspects orders and reports the placement of
// any checkpoint-trigger order. The invariant is "a checkpoint-trigger order
// must be the last order in a proposal" — this function is the single source
// of truth for that check, used by admission, the FSM, and the applier
// pre-split.
func ClassifyCheckpointOrderPosition(orders []*raftcmdpb.Order) CheckpointOrderPosition {
	for i, o := range orders {
		if !IsCheckpointTriggerOrder(o) {
			continue
		}

		if i == len(orders)-1 {
			return CheckpointOrderLast
		}

		return CheckpointOrderInvalid
	}

	return CheckpointOrderAbsent
}

// ProposalRequiresCheckpoint reports whether the proposal will cause the FSM
// to enter maintenance after the batch commit. Assumes the proposal has already
// been validated (the trigger order, if any, is the last one).
func ProposalRequiresCheckpoint(p *raftcmdpb.Proposal) bool {
	orders := p.GetOrders()
	if len(orders) == 0 {
		return false
	}

	return IsCheckpointTriggerOrder(orders[len(orders)-1])
}

// ValidateCheckpointEntryPositions scans entries and returns an error if any
// normal entry carries a proposal whose orders contain a checkpoint-trigger
// while the entry itself is not the last of the slice. The applier pre-splits
// the slice so this should never trip in production; PrepareEntries calls it
// upfront so a malformed batch is rejected before any in-memory FSM mutation.
//
// Unmarshal errors are intentionally swallowed: the main apply loop will
// surface them with the appropriate error semantics.
func ValidateCheckpointEntryPositions(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	last := len(entries) - 1

	for i, entry := range entries {
		if i == last {
			break
		}

		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}

		cmd := raftcmdpb.ProposalFromVTPool()
		err := cmd.UnmarshalVT(entry.Data)
		triggers := err == nil && ClassifyCheckpointOrderPosition(cmd.GetOrders()) != CheckpointOrderAbsent
		cmd.ReturnToVTPool()

		if triggers {
			return fmt.Errorf(
				"checkpoint trigger entry at position %d/%d (raft index %d) — applier must pre-split",
				i, len(entries), entry.Index,
			)
		}
	}

	return nil
}

// ValidateCheckpointEntryPositionsDecoded is the no-unmarshal variant of
// ValidateCheckpointEntryPositions used on the hot path: it inspects the
// pre-decoded proposals attached to each DecodedEntry instead of decoding
// raw entry.Data. Same semantics as ValidateCheckpointEntryPositions: an
// error if any non-last entry carries a checkpoint-trigger order.
func ValidateCheckpointEntryPositionsDecoded(decoded []DecodedEntry) error {
	if len(decoded) == 0 {
		return nil
	}

	last := len(decoded) - 1

	for i := range last {
		if decoded[i].Proposal == nil {
			continue
		}

		if ClassifyCheckpointOrderPosition(decoded[i].Proposal.GetOrders()) == CheckpointOrderAbsent {
			continue
		}

		return fmt.Errorf(
			"checkpoint trigger entry at position %d/%d (raft index %d) — applier must pre-split",
			i, len(decoded), decoded[i].Entry.Index,
		)
	}

	return nil
}
