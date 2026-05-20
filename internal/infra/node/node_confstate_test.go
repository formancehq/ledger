package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestConfStateContainsNode(t *testing.T) {
	t.Parallel()

	t.Run("node in voters", func(t *testing.T) {
		t.Parallel()

		cs := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
		require.True(t, confStateContainsNode(cs, 2))
	})

	t.Run("node in learners", func(t *testing.T) {
		t.Parallel()

		cs := raftpb.ConfState{
			Voters:   []uint64{1, 2},
			Learners: []uint64{3, 4},
		}
		require.True(t, confStateContainsNode(cs, 4))
	})

	t.Run("node absent", func(t *testing.T) {
		t.Parallel()

		cs := raftpb.ConfState{
			Voters:   []uint64{1, 2},
			Learners: []uint64{3},
		}
		require.False(t, confStateContainsNode(cs, 99))
	})

	t.Run("empty ConfState", func(t *testing.T) {
		t.Parallel()

		cs := raftpb.ConfState{}
		require.False(t, confStateContainsNode(cs, 1))
	})
}
