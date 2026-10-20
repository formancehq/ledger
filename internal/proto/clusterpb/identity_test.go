package clusterpb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestPublishedIdentityFieldsRemainUint32(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		message proto.Message
		name    protoreflect.Name
		number  protoreflect.FieldNumber
	}{
		{&GetClusterStateRequest{}, "node_id", 1}, {&NodeInfo{}, "id", 1},
		{&ClusterState{}, "leader", 2}, {&ClusterState{}, "local_node", 4},
		{&TransferLeadershipRequest{}, "transferee", 1}, {&TransferLeadershipResponse{}, "new_leader", 1},
	} {
		field := tc.message.ProtoReflect().Descriptor().Fields().ByName(tc.name)
		require.Equal(t, tc.number, field.Number())
		require.Equal(t, protoreflect.Uint32Kind, field.Kind())
	}
}

func TestFullWidthRequestsNeverAliasLegacyTargets(t *testing.T) {
	t.Parallel()
	for _, id := range []uint64{0, 1, 4294967296, 4294967297, ^uint64(0)} {
		state := NewStateRequest(id)
		transfer := NewTransferRequest(id)
		require.Equal(t, id, state.TargetID())
		require.Equal(t, id, transfer.TargetID())
		for _, msg := range []proto.Message{state, transfer} {
			data, err := proto.Marshal(msg)
			require.NoError(t, err)
			var legacy uint64
			for len(data) > 0 {
				num, typ, n := protowire.ConsumeTag(data)
				require.Positive(t, n)
				data = data[n:]
				if num == 1 {
					legacy, n = protowire.ConsumeVarint(data)
				} else {
					n = protowire.ConsumeFieldValue(num, typ, data)
				}
				require.Positive(t, n)
				data = data[n:]
			}
			if id <= uint64(^uint32(0)) {
				require.Equal(t, id, legacy)
			} else {
				require.Zero(t, legacy)
			}
		}
	}
}

func TestIdentityReadersAcceptLegacyResponses(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint64(7), (&NodeInfo{Id: 7}).NodeID())
	require.Equal(t, uint64(7), (&ClusterState{Leader: 7}).LeaderID())
	require.Equal(t, uint64(7), (&ClusterState{LocalNode: 7}).LocalNodeID())
	require.Equal(t, uint64(7), (&TransferLeadershipResponse{NewLeader: 7}).ConfirmedLeaderID())
	require.NoError(t, ValidateStateTarget(7, &ClusterState{LocalNode: 7}))
	require.ErrorContains(t, ValidateStateTarget(0, &ClusterState{Leader: 1, RaftStatus: &RaftStatus{Leader: 4294967297}}), "full-width cluster identities")
	require.ErrorContains(t, ValidateStateTarget(0, &ClusterState{Nodes: []*NodeInfo{{Id: 1}}, RaftStatus: &RaftStatus{Progress: map[uint64]*ProgressInfo{4294967297: {}}}}), "full-width cluster identities")
	require.ErrorContains(t, ValidateStateTarget(4294967297, &ClusterState{LocalNode: 1}), "full-width")
	require.ErrorContains(t, ValidateStateTarget(4294967297, &ClusterState{LocalNode64: proto.Uint64(1)}), "full-width")
	require.NoError(t, ValidateStateTarget(4294967297, &ClusterState{LocalNode64: proto.Uint64(4294967297)}))
}

func TestLegacyStatusAllowsFullWidthHistoricalVote(t *testing.T) {
	t.Parallel()
	state := &ClusterState{Leader: 1, LocalNode: 1, Nodes: []*NodeInfo{{Id: 1}}, RaftStatus: &RaftStatus{Leader: 1, Vote: 4294967297}}
	require.False(t, HasLargeIdentity(state))
	require.NoError(t, ValidateStateTarget(0, state))
}
