package wal

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestReceivedSnapshotWALReplacementCrashStates(t *testing.T) {
	t.Parallel()

	for _, stage := range []string{
		"snapshot file only",
		"replacement prepared",
		"old directory retired",
		"candidate lost after retire",
		"new directory published",
	} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestWALReplacementCrashChild$")
			child.Env = append(os.Environ(), "LEDGER_WAL_CRASH_STAGE="+stage, "LEDGER_WAL_CRASH_DIR="+dir)
			output, err := child.CombinedOutput()
			require.NoError(t, err, string(output))

			restarted := newTestWALAt(t, dir)
			state, _, err := restarted.InitialState()
			require.NoError(t, err)
			require.Equal(t, uint64(6), state.GetTerm())
			require.Equal(t, uint64(2), state.GetVote())
			require.Equal(t, uint64(5), state.GetCommit())
			last, err := restarted.LastIndex()
			require.NoError(t, err)
			require.Equal(t, uint64(5), last)
			term, err := restarted.Term(5)
			require.NoError(t, err)
			require.Equal(t, uint64(6), term)
			if stage == "snapshot file only" || stage == "replacement prepared" || stage == "candidate lost after retire" {
				loaded, err := restarted.Snapshot()
				require.NoError(t, err)
				require.Equal(t, uint64(0), loaded.GetMetadata().GetIndex(), "old WAL remains selected before the switch")
			} else {
				loaded, err := restarted.Snapshot()
				require.NoError(t, err)
				require.Equal(t, uint64(4), loaded.GetMetadata().GetIndex())
			}
			_, err = os.Stat(filepath.Join(dir, replacementWALDir))
			require.ErrorIs(t, err, os.ErrNotExist)
			_, err = os.Stat(filepath.Join(dir, previousWALDir))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestWALReplacementCrashChild(t *testing.T) {
	stage := os.Getenv("LEDGER_WAL_CRASH_STAGE")
	if stage == "" {
		return
	}
	dir := os.Getenv("LEDGER_WAL_CRASH_DIR")
	w := newTestWALAt(t, dir)
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	require.NoError(t, w.Append(hs(6, 2, 2), []*raftpb.Entry{
		ent(1, 1, nil), ent(2, 1, nil), ent(3, 3, nil),
		ent(4, 3, nil), ent(5, 3, nil),
	}))
	require.NoError(t, w.Append(hs(6, 2, 5), []*raftpb.Entry{
		ent(3, 2, nil), ent(4, 2, nil), ent(5, 6, nil),
	}))
	snap := &raftpb.Snapshot{Metadata: snapshotMeta(4, 2, cs), Data: []byte("received")}
	require.NoError(t, w.snapshotter.Save(snap))
	if stage != "snapshot file only" {
		require.NoError(t, w.prepareReplacementWAL(snap, hs(6, 2, 5), []*raftpb.Entry{ent(5, 6, nil)}))
	}
	if stage == "old directory retired" || stage == "candidate lost after retire" || stage == "new directory published" {
		require.NoError(t, os.Rename(filepath.Join(dir, etcdWalDir), filepath.Join(dir, previousWALDir)))
		require.NoError(t, syncDirectory(dir))
	}
	if stage == "candidate lost after retire" {
		require.NoError(t, os.RemoveAll(filepath.Join(dir, replacementWALDir)))
	}
	if stage == "new directory published" {
		require.NoError(t, os.Rename(filepath.Join(dir, replacementWALDir), filepath.Join(dir, etcdWalDir)))
		require.NoError(t, syncDirectory(dir))
	}
	// Exit without WAL Close, snapshotter cleanup or purge shutdown. The
	// parent must recover the durable bytes in a different OS process.
	os.Exit(0)
}

func TestWALReplacementMissingActiveFailsClosed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	require.NoError(t, w.Append(hs(2, 1, 1), []*raftpb.Entry{ent(1, 2, nil)}))
	require.NoError(t, w.Close())
	require.NoError(t, os.Rename(filepath.Join(dir, etcdWalDir), filepath.Join(dir, replacementWALDir)))
	_, err := New(dir, w.logger, w.meter)
	require.ErrorContains(t, err, "replacement WAL exists without an active or previous WAL")
}

func TestConsecutiveReceivedSnapshotsReplaceSameWALPath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	require.NoError(t, w.Append(hs(3, 1, 3), []*raftpb.Entry{
		ent(1, 1, nil), ent(2, 2, nil), ent(3, 3, nil),
	}))
	require.NoError(t, w.ApplySnapshot(&raftpb.Snapshot{Metadata: snapshotMeta(3, 3, cs)}, nil))
	require.NoError(t, w.Append(hs(4, 2, 4), []*raftpb.Entry{ent(4, 4, nil)}))
	require.NoError(t, w.ApplySnapshot(&raftpb.Snapshot{Metadata: snapshotMeta(4, 4, cs)}, nil))
	require.NoError(t, w.Close())

	restarted := newTestWALAt(t, dir)
	last, err := restarted.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), last)
	state, gotCS, err := restarted.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(4), state.GetTerm())
	require.Equal(t, uint64(2), state.GetVote())
	require.Equal(t, uint64(4), state.GetCommit())
	require.Equal(t, cs.GetVoters(), gotCS.GetVoters())
	_, err = os.Stat(filepath.Join(dir, replacementWALDir))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(dir, previousWALDir))
	require.ErrorIs(t, err, os.ErrNotExist)
}
