package state

import (
	"fmt"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// DecodedEntry pairs a Raft entry with its already-unmarshalled Proposal.
//
// Proposal is nil for entries that carry no proposal payload (config-change
// entries, empty entries). Pre-decoding at the applier boundary lets the
// downstream stages (checkpoint boundary detection, position validation,
// FSM apply loop) read the proposal directly instead of re-unmarshalling
// the raw entry.Data at every step.
//
// Ownership and lifetime: the producer of a DecodedEntry slice (typically
// the applier) owns every non-nil Proposal and MUST return them to the VT
// pool by calling ReleaseDecodedEntries once the slice is no longer used.
// After release, Proposal pointers are invalid and must not be read again.
// The FSM apply path may keep references only for the duration of a single
// PrepareDecodedEntries call — the returned *PreparedBatch deliberately
// does not retain any Proposal pointer.
type DecodedEntry struct {
	Entry    *raftpb.Entry
	Proposal *raftcmdpb.Proposal
}

// DecodeEntries unmarshals every normal entry's Data into a pooled
// *raftcmdpb.Proposal. Non-normal entries (raftpb.EntryConfChange*) and
// entries with empty Data get a nil Proposal so the slice keeps positional
// alignment with the source entries.
//
// On error, any proposals already decoded are returned to the pool and a
// nil slice is returned so callers need not call ReleaseDecodedEntries
// themselves.
func DecodeEntries(entries []*raftpb.Entry) ([]DecodedEntry, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	decoded := make([]DecodedEntry, len(entries))

	for i := range entries {
		decoded[i].Entry = entries[i]

		if entries[i].GetType() != raftpb.EntryNormal || len(entries[i].GetData()) == 0 {
			continue
		}

		cmd := raftcmdpb.ProposalFromVTPool()
		if err := cmd.UnmarshalVT(entries[i].GetData()); err != nil {
			cmd.ReturnToVTPool()
			ReleaseDecodedEntries(decoded[:i])

			return nil, fmt.Errorf("unmarshaling proposal at raft index %d: %w", entries[i].GetIndex(), err)
		}

		decoded[i].Proposal = cmd
	}

	return decoded, nil
}

// ReleaseDecodedEntries returns every non-nil Proposal in decoded back to
// the VT pool. Safe to call with a nil or empty slice. After this call no
// DecodedEntry.Proposal pointer in the slice may be accessed.
func ReleaseDecodedEntries(decoded []DecodedEntry) {
	for i := range decoded {
		if decoded[i].Proposal != nil {
			decoded[i].Proposal.ReturnToVTPool()
			decoded[i].Proposal = nil
		}
	}
}

// DecodedEntryRequiresCheckpoint reports whether d carries a checkpoint-
// triggering order. Mirrors ProposalRequiresCheckpoint but reads the
// pre-decoded proposal so the hot path never re-unmarshals.
func DecodedEntryRequiresCheckpoint(d DecodedEntry) bool {
	if d.Proposal == nil {
		return false
	}

	return ProposalRequiresCheckpoint(d.Proposal)
}
