package node

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

// unmarshalConfChangeV2 decodes a ConfChange or ConfChangeV2 entry into a
// unified ConfChangeV2. Returns false for entries that are not conf-changes.
func unmarshalConfChangeV2(entry raftpb.Entry) (raftpb.ConfChangeV2, bool, error) {
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

// walkConfChangeContexts iterates the Changes in cc and invokes fn once
// per change with (type, nodeID, ctx). ctx is non-nil for Add /
// AddLearnerNode when cc.Context carries a payload (PromoteLearner sends
// AddNode with empty Context — ctx is nil there); ctx is always nil for
// RemoveNode. The Context payload is unmarshalled at most once per cc,
// even when multiple changes share it. Other ConfChange types (UpdateNode,
// etc.) are silently skipped — callers only react to add/remove today.
//
// Used by both Membership.WriteConfChange (FSM Pebble write) and
// Node.finishReady (post-commit cache + transport wiring) so the decode +
// dispatch shape stays in one place.
func walkConfChangeContexts(cc raftpb.ConfChangeV2, fn func(raftpb.ConfChangeType, uint64, *ConfChangeContext) error) error {
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
