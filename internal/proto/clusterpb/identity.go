package clusterpb

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LegacyNodeID returns zero for an identity that cannot be represented by the
// published 32-bit fields. It must never wrap onto another peer's identity.
func LegacyNodeID(id uint64) uint32 {
	if id > uint64(^uint32(0)) {
		return 0
	}

	return uint32(id)
}

// NewStateRequest opts into full-width responses, including for leader routing.
// Older servers still receive the exact legacy target when it fits. Callers
// targeting a large ID must check the response's LocalNode64 presence.
func NewStateRequest(id uint64) *GetClusterStateRequest {
	return &GetClusterStateRequest{NodeId: LegacyNodeID(id), NodeId64: new(id)}
}

func (m *GetClusterStateRequest) TargetID() uint64 {
	if m != nil && m.NodeId64 != nil {
		return m.GetNodeId64()
	}

	return uint64(m.GetNodeId())
}

func (m *NodeInfo) NodeID() uint64 {
	if m.GetId64() != 0 {
		return m.GetId64()
	}

	return uint64(m.GetId())
}

func (m *ClusterState) LeaderID() uint64 {
	if m != nil && m.Leader64 != nil {
		return m.GetLeader64()
	}

	return uint64(m.GetLeader())
}

func (m *ClusterState) LocalNodeID() uint64 {
	if m != nil && m.LocalNode64 != nil {
		return m.GetLocalNode64()
	}

	return uint64(m.GetLocalNode())
}

// NewTransferRequest leaves the legacy target zero for large IDs, causing an
// older server to reject the operation instead of transferring to a wrong peer.
func NewTransferRequest(id uint64) *TransferLeadershipRequest {
	return &TransferLeadershipRequest{Transferee: LegacyNodeID(id), Transferee64: id}
}

func (m *TransferLeadershipRequest) TargetID() uint64 {
	if m.GetTransferee64() != 0 {
		return m.GetTransferee64()
	}

	return uint64(m.GetTransferee())
}

func (m *TransferLeadershipResponse) ConfirmedLeaderID() uint64 {
	if m.GetNewLeader64() != 0 {
		return m.GetNewLeader64()
	}

	return uint64(m.GetNewLeader())
}

// ValidateStateTarget rejects old-server responses to full-width directed reads.
// Older servers ignore node_id64 and may route the legacy zero to their leader.
func ValidateStateTarget(target uint64, state *ClusterState) error {
	if state != nil && state.LocalNode64 == nil && legacyResponseTruncatesIdentity(state) {
		return status.Error(codes.FailedPrecondition, "peer does not support full-width cluster identities")
	}
	if target > uint64(^uint32(0)) && (state == nil || state.LocalNode64 == nil || state.LocalNodeID() != target) {
		return status.Error(codes.FailedPrecondition, "peer does not support the requested full-width node identity")
	}

	return nil
}

// HasLargeIdentity identifies responses unsafe for the published legacy fields.
func HasLargeIdentity(state *ClusterState) bool {
	if state.LeaderID() > uint64(^uint32(0)) || state.LocalNodeID() > uint64(^uint32(0)) {
		return true
	}
	for _, peer := range state.GetNodes() {
		if peer.NodeID() > uint64(^uint32(0)) {
			return true
		}
	}

	return false
}

// Nested Raft IDs were already uint64 in old servers. A high current leader or
// progress key in a topology response exposes narrowing of the legacy fields.
// A historical high vote alone does not make the legacy projection invalid.
func legacyResponseTruncatesIdentity(state *ClusterState) bool {
	if HasLargeIdentity(state) || state.GetRaftStatus().GetLeader() > uint64(^uint32(0)) {
		return true
	}
	if len(state.GetNodes()) > 0 {
		for id := range state.GetRaftStatus().GetProgress() {
			if id > uint64(^uint32(0)) {
				return true
			}
		}
	}

	return false
}
