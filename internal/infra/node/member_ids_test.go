package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
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
