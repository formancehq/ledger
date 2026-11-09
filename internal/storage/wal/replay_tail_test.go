package wal

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// writeResurrectableTail reproduces the WAL history behind the Antithesis
// monotonic :prefix-violation (run 59d605e7…-64-4, node n2): an uncommitted
// term-4 tail 1..7 is overwritten from index 4 by a term-6 leader that commits
// through 6, then a local snapshot is taken at 6. The on-disk WAL still holds
// the stale record (7, term 4) and no newer record above index 6.
func writeResurrectableTail(t *testing.T, dir string) {
	t.Helper()

	w := newTestWALAt(t, dir)
	stale := make([]*raftpb.Entry, 7)
	for i := range stale {
		stale[i] = ent(uint64(i+1), 4, nil)
	}
	require.NoError(t, w.Append(hs(4, 2, 3), stale))
	require.NoError(t, w.Append(hs(6, 0, 6), []*raftpb.Entry{ent(4, 6, nil), ent(5, 6, nil), ent(6, 6, nil)}))

	lastIndex, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(6), lastIndex, "the live log ends at 6 before restart")

	require.NoError(t, w.CreateSnapshot(6, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}, nil))
	require.NoError(t, w.Close())
}

func TestRestartDiscardsTruncatedTailResurrectedAboveSnapshot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeResurrectableTail(t, dir)

	reopened := newTestWALAt(t, dir)
	lastIndex, err := reopened.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(6), lastIndex)

	lastTerm, err := reopened.Term(lastIndex)
	require.NoError(t, err)
	require.Equal(t, uint64(6), lastTerm)

	_, err = reopened.Term(7)
	require.Error(t, err, "the discarded entry must not be addressable")

	// The live log continues normally from the snapshot and survives a
	// further restart.
	require.NoError(t, reopened.Append(hs(7, 0, 6), []*raftpb.Entry{ent(7, 7, []byte{7})}))
	require.NoError(t, reopened.Close())

	again := newTestWALAt(t, dir)
	lastIndex, err = again.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(7), lastIndex)
	lastTerm, err = again.Term(7)
	require.NoError(t, err)
	require.Equal(t, uint64(7), lastTerm)
}

// TestRestartedVoterRejectsCandidateMissingCommittedEntries checks the Raft
// consequence: with the resurrected (7, term 4) as its last entry, the voter
// would grant a candidate ending at (6, term 5), which lacks committed entries
// 4..6 of term 6.
func TestRestartedVoterRejectsCandidateMissingCommittedEntries(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeResurrectableTail(t, dir)
	reopened := newTestWALAt(t, dir)

	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              2,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         reopened,
		Applied:         6,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
	})
	require.NoError(t, err)

	require.NoError(t, rawNode.Step(&raftpb.Message{
		Type:    new(raftpb.MsgVote),
		From:    new(uint64(1)),
		To:      new(uint64(2)),
		Term:    new(uint64(8)),
		LogTerm: new(uint64(5)),
		Index:   new(uint64(6)),
	}))

	require.True(t, rawNode.HasReady())
	var responses []*raftpb.Message
	for _, msg := range rawNode.Ready().Messages {
		if msg.GetType() == raftpb.MsgVoteResp {
			responses = append(responses, msg)
		}
	}
	require.Len(t, responses, 1)
	require.True(t, responses[0].GetReject(), "a voter whose log ends at (6, term 6) must reject a candidate ending at (6, term 5)")
}

func TestDiscardStaleReplayTail(t *testing.T) {
	t.Parallel()

	t.Run("keeps a valid suffix", func(t *testing.T) {
		t.Parallel()

		entries := []*raftpb.Entry{ent(84, 12, nil), ent(85, 12, nil), ent(86, 13, nil)}
		tail, err := discardStaleReplayTail(12, 83, entries)
		require.NoError(t, err)
		require.Equal(t, entries, tail.kept)
		require.Zero(t, tail.discarded)
		require.Nil(t, tail.firstDiscarded)
	})

	t.Run("keeps an empty suffix", func(t *testing.T) {
		t.Parallel()

		tail, err := discardStaleReplayTail(12, 83, nil)
		require.NoError(t, err)
		require.Empty(t, tail.kept)
		require.Zero(t, tail.discarded)
	})

	t.Run("discards a suffix below the snapshot term", func(t *testing.T) {
		t.Parallel()

		// Shape observed on n2: snapshot (83, term 12), resurrected (84, term 3).
		tail, err := discardStaleReplayTail(12, 83, []*raftpb.Entry{ent(84, 3, nil)})
		require.NoError(t, err)
		require.Empty(t, tail.kept)
		require.Equal(t, 1, tail.discarded)
		require.Equal(t, uint64(84), tail.firstDiscarded.GetIndex())
	})

	t.Run("discards from the first term decrease", func(t *testing.T) {
		t.Parallel()

		entries := []*raftpb.Entry{ent(84, 12, nil), ent(85, 13, nil), ent(86, 12, nil), ent(87, 14, nil)}
		tail, err := discardStaleReplayTail(12, 84, entries)
		require.NoError(t, err)
		require.Equal(t, entries[:2], tail.kept)
		require.Equal(t, 2, tail.discarded)
		require.Equal(t, uint64(86), tail.firstDiscarded.GetIndex())
		require.Equal(t, 2, cap(tail.kept), "appends to the kept prefix must not overwrite the discarded entries")
	})

	t.Run("fails closed when the invalid suffix starts at the commit index", func(t *testing.T) {
		t.Parallel()

		_, err := discardStaleReplayTail(12, 84, []*raftpb.Entry{ent(84, 3, nil)})
		require.ErrorIs(t, err, errStaleTailIntersectsCommit)
	})

	t.Run("fails closed when the invalid suffix starts below the commit index", func(t *testing.T) {
		t.Parallel()

		_, err := discardStaleReplayTail(12, 86, []*raftpb.Entry{ent(84, 12, nil), ent(85, 3, nil), ent(86, 3, nil)})
		require.ErrorIs(t, err, errStaleTailIntersectsCommit)
	})
}
