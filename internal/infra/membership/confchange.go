package membership

import (
	"bytes"
	"encoding/json"
	"fmt"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// ConfChangeContext carries peer addresses alongside a Raft ConfChange so that
// all nodes (not just the leader) can learn the new peer's addresses when the
// ConfChange is committed.
//
// InstanceID (EN-1045) is a 16-byte UUID identifying the specific (pod, PVC)
// incarnation of this peer. Populated on Add/AddLearner from the JoinAsLearner
// RPC; empty for bootstrap-initial-peer entries whose instance IDs are not
// known at cluster-formation time (they get filled in when the peer later
// joins). See docs/technical/architecture/subsystems/consensus/removed-member-registry.md.
type ConfChangeContext struct {
	RaftAddress    string `json:"raftAddress"`
	ServiceAddress string `json:"serviceAddress"`
	InstanceID     []byte `json:"instanceId,omitempty"`
}

// Equal reports whether two ConfChangeContext values carry identical
// addresses and instance ID. The []byte InstanceID field prevents the use
// of Go's == operator on the struct directly.
func (c ConfChangeContext) Equal(other ConfChangeContext) bool {
	return c.RaftAddress == other.RaftAddress &&
		c.ServiceAddress == other.ServiceAddress &&
		bytes.Equal(c.InstanceID, other.InstanceID)
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
// unified ConfChangeV2. Returns nil for entries that are not conf-changes.
func UnmarshalConfChangeV2(entry *raftpb.Entry) (*raftpb.ConfChangeV2, bool, error) {
	switch entry.GetType() {
	case raftpb.EntryConfChange:
		var ccV1 raftpb.ConfChange

		err := proto.Unmarshal(entry.GetData(), &ccV1)
		if err != nil {
			return nil, false, fmt.Errorf("unmarshaling ConfChange: %w", err)
		}

		cc := ccV1.AsV2()
		// V1->V2 conversion does not copy Context; propagate it manually.
		cc.Context = ccV1.Context

		return cc, true, nil
	case raftpb.EntryConfChangeV2:
		cc := &raftpb.ConfChangeV2{}

		err := proto.Unmarshal(entry.GetData(), cc)
		if err != nil {
			return nil, false, fmt.Errorf("unmarshaling ConfChangeV2: %w", err)
		}

		return cc, true, nil
	default:
		return nil, false, nil
	}
}

// WalkConfChangeContexts iterates the Changes in cc and invokes fn once
// per change with (type, nodeID, ctx).
//
// ctx is non-nil for Add / AddLearnerNode / UpdateNode / RemoveNode when
// cc.Context carries a payload:
//   - Add / AddLearnerNode carry the joining peer's addresses and
//     instanceID (see JoinAsLearner path).
//   - UpdateNode carries the same payload as Add/AddLearner and is used
//     to refresh an existing peer row — currently the admin
//     cluster.AddLearner + boot flow (EN-1045) where the row was
//     initially written with a nil instance_id.
//   - RemoveNode carries the removed peer's instanceID so the FSM apply
//     path lands the corresponding RemovedMemberEntry atomically with the
//     peer row delete (EN-1045). The RaftAddress / ServiceAddress fields
//     on the RemoveNode ctx are empty by convention.
//   - PromoteLearner sends AddNode with empty Context — ctx is nil there.
//
// A single cc.Context carries exactly one peer identity. A ConfChangeV2
// that bundles multiple Add/AddLearner/UpdateNode/RemoveNode changes with
// a non-empty Context is an invariant violation — all local propose paths
// emit single-op batches, joint consensus isn't used — surfaced as a loud
// error per invariant #7 so the FSM apply aborts before a divergent state
// leaks downstream.
//
// Used by both Membership.WriteConfChange (FSM Pebble write, incl. the
// RemovedMemberEntry write) and Node.finishReady (post-commit cache +
// transport wiring) so the decode + dispatch shape stays in one place.
func WalkConfChangeContexts(cc *raftpb.ConfChangeV2, fn func(raftpb.ConfChangeType, uint64, *ConfChangeContext) error) error {
	if cc == nil {
		return nil
	}

	contextConsumingNodeIDs := make([]uint64, 0, len(cc.Changes))
	for _, change := range cc.Changes {
		switch change.GetType() {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode, raftpb.ConfChangeUpdateNode, raftpb.ConfChangeRemoveNode:
			contextConsumingNodeIDs = append(contextConsumingNodeIDs, change.GetNodeId())
		}
	}

	if len(contextConsumingNodeIDs) > 1 && len(cc.Context) > 0 {
		return fmt.Errorf("invariant: ConfChangeV2 carries %d Add/AddLearner/Update/Remove changes with a non-empty Context (nodes=%v); one Context can only address a single peer, all local propose paths emit single-op batches", len(contextConsumingNodeIDs), contextConsumingNodeIDs)
	}

	var cached *ConfChangeContext

	for _, change := range cc.Changes {
		switch change.GetType() {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode, raftpb.ConfChangeUpdateNode, raftpb.ConfChangeRemoveNode:
			var ctx *ConfChangeContext
			if len(cc.Context) > 0 {
				if cached == nil {
					decoded, err := UnmarshalConfChangeContext(cc.Context)
					if err != nil {
						return fmt.Errorf("invariant: unmarshal ConfChange context for node %d: %w", change.GetNodeId(), err)
					}

					cached = &decoded
				}

				ctx = cached
			}

			if err := fn(change.GetType(), change.GetNodeId(), ctx); err != nil {
				return err
			}
		}
	}

	return nil
}
