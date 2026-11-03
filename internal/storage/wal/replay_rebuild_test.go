package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3/raftpb"
)

var testConfState = &raftpb.ConfState{Voters: []uint64{1, 2, 3}}

func receivedSnapshot(index, term uint64) *raftpb.Snapshot {
	return &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index:     new(index),
		Term:      new(term),
		ConfState: testConfState,
	}}
}

// requireRecoveredLog reopens dir and checks the recovered last (index, term)
// and that no entry above it is addressable. It reopens twice: the result must
// not depend on how many restarts happened since the faulty history.
func requireRecoveredLog(t *testing.T, dir string, lastIndex, lastTerm uint64) {
	t.Helper()

	for range 2 {
		w := newTestWALAt(t, dir)
		gotIndex, err := w.LastIndex()
		require.NoError(t, err)
		require.Equal(t, lastIndex, gotIndex, "recovered last index")
		gotTerm, err := w.Term(gotIndex)
		require.NoError(t, err)
		require.Equal(t, lastTerm, gotTerm, "recovered last term")
		_, err = w.Term(gotIndex + 1)
		require.Error(t, err)
		require.NoError(t, w.Close())
	}
}

func entries(from, to, term uint64) []*raftpb.Entry {
	out := make([]*raftpb.Entry, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, ent(i, term, []byte{byte(i)}))
	}

	return out
}

// A later leader overwrites a conflicting entry with an older-term entry, and
// the follower snapshots at that entry. The stale tail's term is above the
// snapshot term, so the term check alone keeps it.
func TestRestartDiscardsTailAfterLowerTermOverwrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	require.NoError(t, w.Append(hs(1, 1, 4), entries(1, 4, 1)))
	require.NoError(t, w.Append(hs(3, 1, 4), entries(5, 8, 3)))
	require.NoError(t, w.Append(hs(4, 0, 5), []*raftpb.Entry{ent(5, 2, nil)}))
	require.NoError(t, w.CreateSnapshot(5, testConfState, nil))
	require.NoError(t, w.Close())

	requireRecoveredLog(t, dir, 5, 2)
}

// An installed snapshot that did not match the log discards the whole log
// without writing entry records. A stale tail whose term is above the
// snapshot term must not come back.
func TestRestartDiscardsTailReplacedByReceivedSnapshot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	require.NoError(t, w.Append(hs(5, 2, 5), append(entries(1, 5, 1), entries(6, 10, 5)...)))
	require.NoError(t, w.ApplySnapshot(receivedSnapshot(6, 4)))
	require.NoError(t, w.Close())

	requireRecoveredLog(t, dir, 6, 4)
}

// processReady persists a combined snapshot/entry Ready's entries before
// installing the snapshot, then appends them again. A crash between the two
// leaves the suffix only before the snapshot records; it must survive because
// the durable HardState may already commit it.
func TestRestartKeepsReadySuffixPersistedBeforeSnapshotInstall(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	require.NoError(t, w.Append(hs(5, 2, 5), append(entries(1, 5, 1), entries(6, 10, 5)...)))

	// The Ready carries snapshot (6, term 4) and entries 7..8 of term 6.
	require.NoError(t, w.Append(hs(6, 0, 8), entries(7, 8, 6)))
	require.NoError(t, w.ApplySnapshot(receivedSnapshot(6, 4)))
	// Crash before installReadySnapshot appends the suffix again.
	require.NoError(t, w.Close())

	requireRecoveredLog(t, dir, 8, 6)
}

func TestRestartKeepsReadySuffixAppendedAfterSnapshotInstall(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	require.NoError(t, w.Append(hs(5, 2, 5), append(entries(1, 5, 1), entries(6, 10, 5)...)))

	suffix := entries(7, 8, 6)
	require.NoError(t, w.Append(hs(6, 0, 8), suffix))
	require.NoError(t, w.ApplySnapshot(receivedSnapshot(6, 4)))
	require.NoError(t, w.Append(hs(6, 0, 8), suffix))
	require.NoError(t, w.Close())

	requireRecoveredLog(t, dir, 8, 6)
}

// A local snapshot, a ConfState update of that snapshot, and compaction never
// discard retained entries above the snapshot.
func TestRestartKeepsEntriesAboveLocalSnapshot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	require.NoError(t, w.Append(hs(2, 1, 5), entries(1, 8, 2)))
	require.NoError(t, w.CreateSnapshot(5, testConfState, nil))
	require.NoError(t, w.Compact(5))
	require.NoError(t, w.UpdateSnapshotConfState(&raftpb.ConfState{Voters: []uint64{1, 2, 3, 4}}))
	require.NoError(t, w.Close())

	requireRecoveredLog(t, dir, 8, 2)
}

// ApplySnapshot advances Commit to the snapshot index but keeps the current
// election Term and its Vote: the snapshot term is only the term of the
// snapshot's last entry.
func TestApplySnapshotKeepsDurableTermAndVote(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	require.NoError(t, w.Append(hs(14, 1, 5), entries(1, 8, 6)))
	require.NoError(t, w.ApplySnapshot(receivedSnapshot(7, 12)))
	require.NoError(t, w.Close())

	reopened := newTestWALAt(t, dir)
	state, _, err := reopened.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(14), state.GetTerm())
	require.Equal(t, uint64(1), state.GetVote())
	require.Equal(t, uint64(7), state.GetCommit())
}

func TestRebuildReplayLog(t *testing.T) {
	t.Parallel()

	entryRec := func(index, term uint64) replayRecord {
		return replayRecord{kind: wal.EntryType, entry: logID{index: index, term: term}}
	}
	stateRec := replayRecord{kind: wal.StateType}
	committed := func(commit uint64) replayRecord {
		return replayRecord{kind: wal.StateType, commit: commit}
	}
	metadataRec := replayRecord{kind: wal.MetadataType}
	snapRec := func(index, term uint64) replayRecord {
		return replayRecord{kind: wal.SnapshotType, snapshot: logID{index: index, term: term}}
	}
	ids := func(from, to, term uint64) []logID {
		var out []logID
		for i := from; i <= to; i++ {
			out = append(out, logID{index: i, term: term})
		}

		return out
	}

	t.Run("overwrite at or below the replay snapshot truncates the tail", func(t *testing.T) {
		t.Parallel()

		got, err := rebuildReplayLog([]replayRecord{
			entryRec(1, 3), entryRec(2, 3), entryRec(3, 3), stateRec,
			entryRec(2, 6), stateRec,
			snapRec(2, 6),
		}, logID{index: 2, term: 6}, 2)
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("segment header HardState does not split a Save call", func(t *testing.T) {
		t.Parallel()

		got, err := rebuildReplayLog([]replayRecord{
			entryRec(1, 1), entryRec(2, 5), entryRec(3, 5), stateRec,
			entryRec(3, 6), stateRec,
			metadataRec, stateRec, // etcd cut: crc, metadata, current HardState
			snapRec(2, 4), committed(3), snapRec(2, 4), // received: the log holds (2, term 5)
		}, logID{index: 2, term: 4}, 3)
		require.NoError(t, err)
		require.Equal(t, ids(3, 3, 6), got)
	})

	t.Run("a second Save call before the snapshot drops the suffix candidate", func(t *testing.T) {
		t.Parallel()

		got, err := rebuildReplayLog([]replayRecord{
			entryRec(1, 1), entryRec(2, 5), entryRec(3, 5), stateRec,
			stateRec, // the snapshot Ready persists only a HardState
			snapRec(2, 4), committed(2), snapRec(2, 4),
		}, logID{index: 2, term: 4}, 2)
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("an uncommitted guard snapshot record is ignored", func(t *testing.T) {
		t.Parallel()

		got, err := rebuildReplayLog([]replayRecord{
			entryRec(1, 1), entryRec(2, 5), entryRec(3, 5), committed(1),
			snapRec(2, 4), // guard written, HardState never reached index 2
		}, logID{index: 0}, 1)
		require.NoError(t, err)
		require.Equal(t, []logID{{1, 1}, {2, 5}, {3, 5}}, got)
	})

	t.Run("a guard whose HardState was lost stays ignored after commit passes it", func(t *testing.T) {
		t.Parallel()

		// Crash after the guard record: the restarted node resumes its old log,
		// a leader repairs it from index 3, and commit later reaches 4.
		got, err := rebuildReplayLog([]replayRecord{
			entryRec(1, 1), entryRec(2, 1), entryRec(3, 2), committed(1),
			snapRec(3, 4),
			entryRec(3, 4), entryRec(4, 4), committed(4),
		}, logID{index: 1, term: 1}, 4)
		require.NoError(t, err)
		require.Equal(t, []logID{{2, 1}, {3, 4}, {4, 4}}, got)
	})

	t.Run("a received snapshot above the replay snapshot fails closed", func(t *testing.T) {
		t.Parallel()

		_, err := rebuildReplayLog([]replayRecord{
			entryRec(1, 1), entryRec(2, 1), stateRec,
			snapRec(4, 2), committed(4), snapRec(4, 2), entryRec(5, 2), committed(5),
		}, logID{index: 1, term: 1}, 5)
		require.ErrorIs(t, err, errReplayRebuild)
	})
}

func TestTrimToRebuiltLog(t *testing.T) {
	t.Parallel()

	readAll := []*raftpb.Entry{ent(7, 6, nil), ent(8, 6, nil), ent(9, 3, nil)}

	got, err := trimToRebuiltLog(readAll, []logID{{7, 6}, {8, 6}})
	require.NoError(t, err)
	require.Equal(t, readAll[:2], got)
	require.Equal(t, 2, cap(got))

	_, err = trimToRebuiltLog(readAll, []logID{{7, 6}, {8, 5}})
	require.ErrorIs(t, err, errReplayRebuild, "the rebuild may only remove a suffix")

	_, err = trimToRebuiltLog(readAll[:1], []logID{{7, 6}, {8, 6}})
	require.ErrorIs(t, err, errReplayRebuild)
}
