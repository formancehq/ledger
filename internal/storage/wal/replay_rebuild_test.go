package wal

import (
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

var testConfState = &raftpb.ConfState{Voters: []uint64{1, 2, 3}}

func receivedSnapshot(index, term uint64) *raftpb.Snapshot {
	return &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index:     new(index),
		Term:      new(term),
		ConfState: testConfState,
	}}
}

// requireRecoveredLog reopens dir and checks the whole recovered log: the
// snapshot boundary, every retained (index, term) and payload above it, and
// that nothing past the last entry is addressable. It reopens twice: the
// result must not depend on how many restarts happened since the history.
// Payloads follow the entries helper (one byte equal to the index).
func requireRecoveredLog(t *testing.T, dir string, boundary logID, above ...logID) {
	t.Helper()

	for range 2 {
		w := newTestWALAt(t, dir)

		firstIndex, err := w.FirstIndex()
		require.NoError(t, err)
		require.Equal(t, boundary.index+1, firstIndex, "recovered first index")
		boundaryTerm, err := w.Term(boundary.index)
		require.NoError(t, err)
		require.Equal(t, boundary.term, boundaryTerm, "recovered boundary term")

		lastIndex, err := w.LastIndex()
		require.NoError(t, err)
		require.Equal(t, boundary.index+uint64(len(above)), lastIndex, "recovered last index")

		var got []logID
		if len(above) > 0 {
			recovered, err := w.Entries(firstIndex, lastIndex+1, math.MaxUint64)
			require.NoError(t, err)
			for _, e := range recovered {
				got = append(got, logID{index: e.GetIndex(), term: e.GetTerm()})
				require.Equal(t, []byte{byte(e.GetIndex())}, e.GetData(), "payload of entry %d", e.GetIndex())
			}
		}
		require.Equal(t, above, got, "recovered entries")

		_, err = w.Term(lastIndex + 1)
		require.Error(t, err)
		require.NoError(t, w.Close())
	}
}

func ids(from, to, term uint64) []logID {
	var out []logID
	for i := from; i <= to; i++ {
		out = append(out, logID{index: i, term: term})
	}

	return out
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

	requireRecoveredLog(t, dir, logID{5, 2})
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

	requireRecoveredLog(t, dir, logID{6, 4})
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

	requireRecoveredLog(t, dir, logID{6, 4}, ids(7, 8, 6)...)
}

// persistSnapshotGuardOnly runs the first durable steps of ApplySnapshot (the
// snapshot file, then the synced guard record) and stops, as a crash right
// after the guard would.
func persistSnapshotGuardOnly(t *testing.T, w *DefaultWAL, snap *raftpb.Snapshot) {
	t.Helper()

	require.NoError(t, w.snapshotter.Save(snap))
	require.NoError(t, w.wal.SaveSnapshot(&walpb.Snapshot{
		Index:     new(snap.GetMetadata().GetIndex()),
		Term:      new(snap.GetMetadata().GetTerm()),
		ConfState: snap.GetMetadata().GetConfState(),
	}))
}

// processReady persists a received snapshot's Ready HardState (commit at the
// snapshot index) before ApplySnapshot writes its guard, and the guard's sync
// flushes it. A crash right after the guard leaves the snapshot in force on
// restart, so the discarded tail must not come back.
func TestRestartDiscardsTailWhenCrashFollowsCommittedSnapshotGuard(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	require.NoError(t, w.Append(hs(5, 2, 5), append(entries(1, 5, 1), entries(6, 10, 5)...)))
	require.NoError(t, w.Append(hs(6, 0, 6), nil))
	persistSnapshotGuardOnly(t, w, receivedSnapshot(6, 4))
	require.NoError(t, w.Close())

	requireRecoveredLog(t, dir, logID{6, 4})
}

func TestRestartKeepsReadySuffixWhenCrashFollowsSnapshotGuard(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	require.NoError(t, w.Append(hs(5, 2, 5), append(entries(1, 5, 1), entries(6, 10, 5)...)))
	require.NoError(t, w.Append(hs(6, 0, 8), entries(7, 8, 6)))
	persistSnapshotGuardOnly(t, w, receivedSnapshot(6, 4))
	require.NoError(t, w.Close())

	requireRecoveredLog(t, dir, logID{6, 4}, ids(7, 8, 6)...)
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

	requireRecoveredLog(t, dir, logID{6, 4}, ids(7, 8, 6)...)
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

	requireRecoveredLog(t, dir, logID{5, 2}, ids(6, 8, 2)...)
}

// abandonedGuardHistory crashes right after a received snapshot (3, term 4)
// writes its guard while commit is 1, so the first restart resumes from the
// local snapshot (1, term 2). A leader then repairs indexes 2..4 and commits 4,
// which makes etcd select the abandoned guard as the replay snapshot of the
// next restart.
func abandonedGuardHistory(t *testing.T, repairTermAt3 uint64) string {
	t.Helper()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	require.NoError(t, w.Append(hs(2, 1, 1), entries(1, 1, 2)))
	require.NoError(t, w.CreateSnapshot(1, testConfState, nil))
	persistSnapshotGuardOnly(t, w, receivedSnapshot(3, 4))
	require.NoError(t, w.Close())

	w = newTestWALAt(t, dir)
	first, err := w.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), first, "first restart ignores the uncommitted guard")
	require.NoError(t, w.Append(hs(5, 0, 4), []*raftpb.Entry{
		ent(2, 2, []byte{2}), ent(3, repairTermAt3, []byte{3}), ent(4, 5, []byte{4}),
	}))
	require.NoError(t, w.Close())

	return dir
}

// A received snapshot is committed state, so the repairing leader holds the
// snapshot's (3, term 4). The abandoned guard selected later agrees with the
// live log.
func TestRestartSelectsAbandonedGuardOnceCommitPassesIt(t *testing.T) {
	t.Parallel()

	requireRecoveredLog(t, abandonedGuardHistory(t, 4), logID{3, 4}, logID{4, 5})
}

// Raft cannot commit another term at the index of a committed snapshot. If the
// WAL says it did, the selected replay snapshot contradicts the live log, and
// startup must fail instead of reporting the snapshot term at that index.
func TestRestartFailsClosedWhenReplaySnapshotContradictsLog(t *testing.T) {
	t.Parallel()

	dir := abandonedGuardHistory(t, 5)
	_, err := New(dir, logging.FromContext(logging.TestingContext()), noop.NewMeterProvider().Meter("test"))
	require.ErrorIs(t, err, errReplayRebuild)
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

	// The log holds (2, term 5), so snapshot (2, term 4) was received. Entry
	// (3, term 6) starts right after it and would qualify as the Ready suffix.
	suffixBeforeSnapshot := []replayRecord{
		entryRec(1, 1), entryRec(2, 5), committed(1),
		entryRec(3, 6), committed(1),
	}

	t.Run("the Save call just before a received snapshot is kept", func(t *testing.T) {
		t.Parallel()

		records := append(slices.Clone(suffixBeforeSnapshot),
			snapRec(2, 4), committed(2), snapRec(2, 4))
		got, err := rebuildReplayLog(records, logID{index: 2, term: 4}, 2)
		require.NoError(t, err)
		require.Equal(t, []logID{{3, 6}}, got)
	})

	t.Run("a second Save call before the snapshot drops the suffix candidate", func(t *testing.T) {
		t.Parallel()

		records := append(slices.Clone(suffixBeforeSnapshot),
			committed(2), // the snapshot Ready persists only a HardState
			snapRec(2, 4), committed(2), snapRec(2, 4))
		got, err := rebuildReplayLog(records, logID{index: 2, term: 4}, 2)
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
		// whose stale (3, term 2) differs from the guard. A leader repairs it
		// from index 3 with the snapshot's (3, term 4), and commit later
		// reaches 4. Counting the guard would treat it as received and discard
		// the log it sits before.
		got, err := rebuildReplayLog([]replayRecord{
			entryRec(1, 1), entryRec(2, 1), entryRec(3, 2), committed(1),
			snapRec(3, 4),
			entryRec(3, 4), entryRec(4, 5), committed(4),
		}, logID{index: 1, term: 1}, 4)
		require.NoError(t, err)
		require.Equal(t, []logID{{2, 1}, {3, 4}, {4, 5}}, got)
	})

	t.Run("a replay snapshot whose term the rebuilt log contradicts fails closed", func(t *testing.T) {
		t.Parallel()

		_, err := rebuildReplayLog([]replayRecord{
			entryRec(1, 1), entryRec(2, 1), entryRec(3, 2), committed(1),
			snapRec(3, 4),
			entryRec(3, 5), entryRec(4, 5), committed(4),
		}, logID{index: 3, term: 4}, 4)
		require.ErrorIs(t, err, errReplayRebuild)
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

// Replay runs on every restart over every retained record, so contiguous
// appends must not copy the retained log each time. Capping the retained slice
// at every append copied the whole log per entry, about 1.3 GB for 10,000
// entries; amortized growth stays near 1 MB.
func TestRebuildReplayLogContiguousAppendsAllocateLinearly(t *testing.T) {
	t.Parallel()

	const n = 10_000
	records := make([]replayRecord, 0, 2*n)
	for i := uint64(1); i <= n; i++ {
		records = append(records,
			replayRecord{kind: wal.EntryType, entry: logID{index: i, term: 1}},
			replayRecord{kind: wal.StateType, commit: i - 1},
		)
	}

	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			got, err := rebuildReplayLog(records, logID{}, n)
			if err != nil || len(got) != n {
				b.Fatal("unexpected rebuild result")
			}
		}
	})
	require.Less(t, result.AllocedBytesPerOp(), int64(16<<20), "contiguous replay must amortize slice growth")
}

// The rebuild removes a suffix that ReadAll kept; when that suffix starts at
// or below the durable commit index, reconciliation must fail closed rather
// than lower the commit. The discardStaleReplayTail backstop cannot catch a
// missing check here: reconciliation already removed the suffix before it.
func TestReconcileFailsClosedWhenRebuildDropsCommittedPosition(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	etcdWAL, err := wal.Create(zap.NewNop(), dir, nil)
	require.NoError(t, err)
	require.NoError(t, etcdWAL.Save(hs(1, 0, 0), entries(1, 5, 1)))
	// Overwrite at 2 truncates 3..5 in the live log; a later local snapshot at
	// 2 hides that overwrite from ReadAll. The commit index then claims 4.
	require.NoError(t, etcdWAL.Save(hs(2, 0, 2), []*raftpb.Entry{ent(2, 2, []byte{2})}))
	require.NoError(t, etcdWAL.SaveSnapshot(&walpb.Snapshot{Index: new(uint64(2)), Term: new(uint64(2)), ConfState: testConfState}))
	require.NoError(t, etcdWAL.Save(hs(2, 0, 4), nil))
	require.NoError(t, etcdWAL.Close())

	readAll := entries(3, 5, 1)
	_, err = reconcileReplayedEntries(dir, &raftpb.Snapshot{Metadata: snapshotMeta(2, 2, nil)}, hs(2, 0, 4), readAll,
		logging.FromContext(logging.TestingContext()))
	require.ErrorIs(t, err, errStaleTailIntersectsCommit)
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
