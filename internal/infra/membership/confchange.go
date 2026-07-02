package membership

import (
	"encoding/json"
	"fmt"

	"go.etcd.io/raft/v3/raftpb"
)

// ConfChangeContext carries peer addresses alongside a Raft ConfChange so that
// all nodes (not just the leader) can learn the new peer's addresses when the
// ConfChange is committed.
type ConfChangeContext struct {
	RaftAddress    string `json:"raftAddress"`
	ServiceAddress string `json:"serviceAddress"`
}

// MarshalConfChangeContext serialises a ConfChangeContext to JSON bytes
// suitable for embedding in ConfChange.Context.
func MarshalConfChangeContext(ctx ConfChangeContext) ([]byte, error) {
	data, err := json.Marshal(ctx)
	if err != nil {
		return nil, fmt.Errorf("marshaling ConfChangeContext: %w", err)
	}

	return data, nil
}

// UnmarshalConfChangeContext deserialises a ConfChangeContext from JSON bytes
// that were stored in ConfChange.Context.
func UnmarshalConfChangeContext(data []byte) (ConfChangeContext, error) {
	var ctx ConfChangeContext

	err := json.Unmarshal(data, &ctx)
	if err != nil {
		return ConfChangeContext{}, fmt.Errorf("unmarshaling ConfChangeContext: %w", err)
	}

	return ctx, nil
}

// UnmarshalConfChangeV2 decodes a ConfChange or ConfChangeV2 entry into a
// unified ConfChangeV2. Returns false for entries that are not conf-changes.
func UnmarshalConfChangeV2(entry raftpb.Entry) (raftpb.ConfChangeV2, bool, error) {
	var cc raftpb.ConfChangeV2

	switch entry.Type {
	case raftpb.EntryConfChange:
		var ccV1 raftpb.ConfChange

		err := ccV1.Unmarshal(entry.Data)
		if err != nil {
			return cc, false, fmt.Errorf("unmarshaling ConfChange: %w", err)
		}

		cc = ccV1.AsV2()
		// V1->V2 conversion does not copy Context; propagate it manually.
		cc.Context = ccV1.Context
	case raftpb.EntryConfChangeV2:
		err := cc.Unmarshal(entry.Data)
		if err != nil {
			return cc, false, fmt.Errorf("unmarshaling ConfChangeV2: %w", err)
		}
	default:
		return cc, false, nil
	}

	return cc, true, nil
}

// WalkConfChangeContexts iterates the Changes in cc and invokes fn once
// per change with (type, nodeID, ctx). ctx is non-nil for Add /
// AddLearnerNode when cc.Context carries a payload (PromoteLearner sends
// AddNode with empty Context — ctx is nil there); ctx is always nil for
// RemoveNode. Other ConfChange types (UpdateNode, etc.) are silently
// skipped — callers only react to add/remove today.
//
// A single cc.Context carries exactly one peer address. A ConfChangeV2
// that bundles multiple Add / AddLearnerNode changes with a non-empty
// Context is therefore an invariant violation (all local propose paths
// emit single-Add batches; joint consensus isn't used) — we surface it
// as a loud error per invariant #7 rather than silently degrading, so
// the FSM apply path aborts and a divergent state (voters recorded in
// ConfState with no dialable Pebble row) is caught immediately instead
// of leaking downstream.
//
// Used by both Membership.WriteConfChange (FSM Pebble write) and
// Node.finishReady (post-commit cache + transport wiring) so the decode +
// dispatch shape stays in one place.
func WalkConfChangeContexts(cc raftpb.ConfChangeV2, fn func(raftpb.ConfChangeType, uint64, *ConfChangeContext) error) error {
	addNodeIDs := make([]uint64, 0, len(cc.Changes))
	for _, change := range cc.Changes {
		if change.Type == raftpb.ConfChangeAddNode || change.Type == raftpb.ConfChangeAddLearnerNode {
			addNodeIDs = append(addNodeIDs, change.NodeID)
		}
	}

	if len(addNodeIDs) > 1 && len(cc.Context) > 0 {
		return fmt.Errorf("invariant: ConfChangeV2 carries %d Add/AddLearner changes with a non-empty Context (nodes=%v); one Context can only address a single peer, all local propose paths emit single-Add batches", len(addNodeIDs), addNodeIDs)
	}

	var cached *ConfChangeContext

	for _, change := range cc.Changes {
		switch change.Type {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
			var ctx *ConfChangeContext
			if len(cc.Context) > 0 {
				if cached == nil {
					decoded, err := UnmarshalConfChangeContext(cc.Context)
					if err != nil {
						return fmt.Errorf("invariant: unmarshal ConfChange context for node %d: %w", change.NodeID, err)
					}

					cached = &decoded
				}

				ctx = cached
			}

			if err := fn(change.Type, change.NodeID, ctx); err != nil {
				return err
			}
		case raftpb.ConfChangeRemoveNode:
			if err := fn(change.Type, change.NodeID, nil); err != nil {
				return err
			}
		}
	}

	return nil
}
