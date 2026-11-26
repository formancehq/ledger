package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
)

func TestNodeMemberIDs(t *testing.T) {
	t.Parallel()

	var n Node
	require.Nil(t, n.MemberIDs())

	n.confState.Store(&raftpb.ConfState{
		Voters:         []uint64{3, 1},
		Learners:       []uint64{4},
		VotersOutgoing: []uint64{1, 2},
		LearnersNext:   []uint64{3, 5},
	})

	require.Equal(t, []uint64{1, 2, 3, 4, 5}, n.MemberIDs())
}

func TestCapabilityMemberRolesIncludesJointConfiguration(t *testing.T) {
	t.Parallel()

	roles := capabilityMemberRoles(&raftpb.ConfState{
		Voters:         []uint64{1},
		Learners:       []uint64{4},
		VotersOutgoing: []uint64{2},
		LearnersNext:   []uint64{5},
	})

	require.Equal(t, map[uint64]capabilities.MemberRole{
		1: capabilities.Voter,
		2: capabilities.Voter,
		4: capabilities.Learner,
		5: capabilities.Learner,
	}, roles)
}
