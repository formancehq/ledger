package wal

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/go-libs/v4/logging"
)

func countWALFiles(t *testing.T, dir string) int {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	count := 0

	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			count++
		}
	}

	return count
}

func newTestWAL(t *testing.T, opts ...Option) *DefaultWAL {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	w, err := New(t.TempDir(), logger, meter, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	return w
}

func TestPurgeOldWALSegments(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t, WithPurgeInterval(100*time.Millisecond))

	// Write enough data to force WAL segment rotation.
	// etcd WAL segments are 64MB, so writing ~200MB should create at least 3 segments.
	// ReleaseLockTo requires at least 3 segments to actually release the oldest lock,
	// because it always keeps the segment just before the target index locked.
	const numEntries = 10

	entryData := make([]byte, 20*1024*1024)
	for i := uint64(1); i <= numEntries; i++ {
		err := w.Append(
			raftpb.HardState{Term: 1, Vote: 1, Commit: i},
			[]raftpb.Entry{{Index: i, Term: 1, Data: entryData}},
		)
		require.NoError(t, err)
	}

	segmentsAfterWrite := countWALFiles(t, w.etcdWalDir)
	require.GreaterOrEqual(t, segmentsAfterWrite, 3, "writing ~200MB should create at least 3 WAL segments")

	// Create a snapshot at a high index and compact to release locks on old segments.
	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(numEntries, cs, nil))
	require.NoError(t, w.Compact(numEntries))

	// The background purger should eventually delete old unlocked segments.
	require.Eventually(t, func() bool {
		return countWALFiles(t, w.etcdWalDir) < segmentsAfterWrite
	}, 10*time.Second, 200*time.Millisecond, "old WAL segments should be purged")
}

// --- InitialState tests ---

func TestInitialState_EmptyWAL(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	hs, cs, err := w.InitialState()
	require.NoError(t, err)
	require.True(t, raft.IsEmptyHardState(hs), "empty WAL should return empty hard state")
	require.Empty(t, cs.Voters, "empty WAL should return empty conf state")
}

func TestInitialState_AfterAppend(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	hs := raftpb.HardState{Term: 3, Vote: 1, Commit: 5}
	err := w.Append(hs, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("data")},
	})
	require.NoError(t, err)

	gotHS, _, err := w.InitialState()
	require.NoError(t, err)
	require.Equal(t, hs.Term, gotHS.Term)
	require.Equal(t, hs.Vote, gotHS.Vote)
	require.Equal(t, hs.Commit, gotHS.Commit)
}

func TestInitialState_AfterSnapshot(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Create initial snapshot on empty storage
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	require.NoError(t, w.CreateSnapshot(0, cs, nil))

	_, gotCS, err := w.InitialState()
	require.NoError(t, err)
	require.Equal(t, cs.Voters, gotCS.Voters)
}

// --- Entries tests ---

func TestEntries_Basic(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
		{Index: 4, Term: 2, Data: []byte("d")},
		{Index: 5, Term: 2, Data: []byte("e")},
	}
	require.NoError(t, w.Append(raftpb.HardState{Term: 2, Vote: 1, Commit: 5}, entries))

	// Read all entries
	got, err := w.Entries(1, 6, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, got, 5)
	require.Equal(t, uint64(1), got[0].Index)
	require.Equal(t, uint64(5), got[4].Index)
}

func TestEntries_SubRange(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
	}
	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, entries))

	got, err := w.Entries(2, 4, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, uint64(2), got[0].Index)
	require.Equal(t, uint64(3), got[1].Index)
}

func TestEntries_MaxSizeLimit(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: make([]byte, 100)},
		{Index: 2, Term: 1, Data: make([]byte, 100)},
		{Index: 3, Term: 1, Data: make([]byte, 100)},
	}
	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, entries))

	// Use a very small maxSize that should still return at least the first entry
	got, err := w.Entries(1, 4, 1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(got), 1, "should return at least 1 entry even with small maxSize")
	require.LessOrEqual(t, len(got), 3)
}

func TestEntries_InvalidRange(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 1}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
	}))

	// lo >= hi
	_, err := w.Entries(5, 3, math.MaxUint64)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid range")
}

func TestEntries_Compacted(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
	}
	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, entries))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(2, cs, nil))
	require.NoError(t, w.Compact(2))

	// Asking for entries before the snapshot index should return ErrCompacted
	_, err := w.Entries(1, 3, math.MaxUint64)
	require.ErrorIs(t, err, raft.ErrCompacted)
}

func TestEntries_EmptyStorage(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	_, err := w.Entries(1, 2, math.MaxUint64)
	require.Error(t, err)
}

func TestEntries_HiOutOfBound(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 2}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
	}))

	_, err := w.Entries(1, 100, math.MaxUint64)
	require.Error(t, err)
}

// --- Term tests ---

func TestTerm_Basic(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 2, Data: []byte("b")},
		{Index: 3, Term: 3, Data: []byte("c")},
	}
	require.NoError(t, w.Append(raftpb.HardState{Term: 3, Vote: 1, Commit: 3}, entries))

	term, err := w.Term(1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), term)

	term, err = w.Term(2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), term)

	term, err = w.Term(3)
	require.NoError(t, err)
	require.Equal(t, uint64(3), term)
}

func TestTerm_SnapshotIndex(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 2, Data: []byte("b")},
		{Index: 3, Term: 3, Data: []byte("c")},
	}
	require.NoError(t, w.Append(raftpb.HardState{Term: 3, Vote: 1, Commit: 3}, entries))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(2, cs, nil))
	require.NoError(t, w.Compact(2))

	// Term at snapshot index should return snapshot term
	term, err := w.Term(2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), term)
}

func TestTerm_OutOfBound(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 2}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
	}))

	// Beyond last entry
	_, err := w.Term(100)
	require.Error(t, err)
}

func TestTerm_Compacted(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 2, Data: []byte("b")},
		{Index: 3, Term: 3, Data: []byte("c")},
	}
	require.NoError(t, w.Append(raftpb.HardState{Term: 3, Vote: 1, Commit: 3}, entries))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(2, cs, nil))
	require.NoError(t, w.Compact(2))

	// Entry before snapshot should be compacted
	_, err := w.Term(1)
	require.ErrorIs(t, err, raft.ErrCompacted)
}

// --- LastIndex tests ---

func TestLastIndex_EmptyWAL(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	idx, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), idx, "empty WAL should have last index 0")
}

func TestLastIndex_WithEntries(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
	}))

	idx, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), idx)
}

func TestLastIndex_WithSnapshotOnly(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(0, cs, nil))
	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     10,
			Term:      2,
			ConfState: *cs,
		},
		Data: []byte("snapshot data"),
	}
	require.NoError(t, w.ApplySnapshot(snap))

	idx, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(10), idx, "last index should be snapshot index when no entries")
}

// --- FirstIndex tests ---

func TestFirstIndex_EmptyWAL(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	idx, err := w.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), idx, "empty WAL with snapshot.index=0 should have first index 1")
}

func TestFirstIndex_WithEntries(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
	}))

	idx, err := w.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), idx)
}

func TestFirstIndex_AfterCompaction(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 5}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
		{Index: 4, Term: 1, Data: []byte("d")},
		{Index: 5, Term: 1, Data: []byte("e")},
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(3, cs, nil))
	require.NoError(t, w.Compact(3))

	idx, err := w.FirstIndex()
	require.NoError(t, err)
	// After compacting to index 3, the first available entry should be 3 or later
	require.GreaterOrEqual(t, idx, uint64(3))
}

// --- Snapshot tests ---

func TestSnapshot_Empty(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	snap, err := w.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(0), snap.Metadata.Index)
	require.Equal(t, uint64(0), snap.Metadata.Term)
}

func TestSnapshot_AfterCreateSnapshot(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(raftpb.HardState{Term: 2, Vote: 1, Commit: 3}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 2, Data: []byte("b")},
		{Index: 3, Term: 2, Data: []byte("c")},
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1, 2}}
	require.NoError(t, w.CreateSnapshot(3, cs, []byte("snap-data")))

	snap, err := w.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(3), snap.Metadata.Index)
	require.Equal(t, uint64(2), snap.Metadata.Term)
	require.Equal(t, []byte("snap-data"), snap.Data)
	require.Equal(t, cs.Voters, snap.Metadata.ConfState.Voters)
}

// --- ApplySnapshot tests ---

func TestApplySnapshot(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Append some entries first
	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 2}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
	}))

	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     10,
			Term:      5,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
		Data: []byte("full-snapshot"),
	}
	require.NoError(t, w.ApplySnapshot(snap))

	// Verify snapshot is applied
	gotSnap, err := w.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(10), gotSnap.Metadata.Index)
	require.Equal(t, uint64(5), gotSnap.Metadata.Term)
	require.Equal(t, []byte("full-snapshot"), gotSnap.Data)

	// Entries should be cleared after applying snapshot
	idx, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(10), idx, "last index should be snapshot index after apply")
}

// --- Append edge cases ---

func TestAppend_NoChangeNoOp(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Appending same hard state with no entries should be a no-op
	err := w.Append(raftpb.HardState{}, nil)
	require.NoError(t, err)
}

func TestAppend_HardStateOnly(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	hs := raftpb.HardState{Term: 5, Vote: 2, Commit: 0}
	require.NoError(t, w.Append(hs, nil))

	gotHS, _, err := w.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(5), gotHS.Term)
	require.Equal(t, uint64(2), gotHS.Vote)
}

func TestAppend_MultipleAppends(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// First batch
	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 2}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
	}))

	// Second batch
	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 4}, []raftpb.Entry{
		{Index: 3, Term: 1, Data: []byte("c")},
		{Index: 4, Term: 1, Data: []byte("d")},
	}))

	idx, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), idx)

	ents, err := w.Entries(1, 5, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, ents, 4)
}

func TestAppend_Truncation(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// First append
	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
	}))

	// Overwrite from index 2 with a different term (simulates leader change)
	require.NoError(t, w.Append(raftpb.HardState{Term: 2, Vote: 2, Commit: 2}, []raftpb.Entry{
		{Index: 2, Term: 2, Data: []byte("b-new")},
	}))

	idx, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), idx)

	ents, err := w.Entries(1, 3, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, ents, 2)
	require.Equal(t, uint64(1), ents[0].Term)
	require.Equal(t, uint64(2), ents[1].Term)
	require.Equal(t, []byte("b-new"), ents[1].Data)
}

func TestAppend_GapCreatesAppend(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Append entries at index 1-2
	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 2}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
	}))

	// Append entries at index 5 (gap from 3-4 -- this shouldn't happen in valid Raft
	// but tests the code path where entries[0].Index > offset+len(s.entries))
	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 5}, []raftpb.Entry{
		{Index: 5, Term: 1, Data: []byte("e")},
	}))

	idx, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), idx)
}

// --- Compact tests ---

func TestCompact_Basic(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 5}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
		{Index: 4, Term: 1, Data: []byte("d")},
		{Index: 5, Term: 1, Data: []byte("e")},
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(3, cs, nil))
	require.NoError(t, w.Compact(3))

	// After compaction, entries before index 3 should be compacted
	_, err := w.Entries(1, 3, math.MaxUint64)
	require.ErrorIs(t, err, raft.ErrCompacted)

	// Entries after compaction point should still be available
	ents, err := w.Entries(3, 6, math.MaxUint64)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(ents), 1)
}

func TestCompact_AfterSnapshot(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Attempt compact with index > snapshot.Metadata.Index should error
	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
	}))

	// Compact at index 5 without a snapshot at that index should fail
	err := w.Compact(5)
	require.Error(t, err)
}

func TestCompact_BeforeFirstIndex(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(3, cs, nil))
	require.NoError(t, w.Compact(3))

	// Compact again at the same index (before first index) should return ErrCompacted
	err := w.Compact(1)
	require.Error(t, err)
}

func TestCompact_EmptyEntriesAfterApplySnapshot(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Apply a snapshot which clears entries
	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     10,
			Term:      2,
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
		},
	}
	require.NoError(t, w.ApplySnapshot(snap))

	// Compact at snapshot index with no entries should succeed (no-op via empty entries path)
	// firstIndex = snapshot.Metadata.Index + 1 = 11
	// compactIndex = 10 <= snapshot.Metadata.Index = 10 (passes first check)
	// compactIndex = 10 < firstIndex = 11 (returns ErrCompacted)
	// But the real no-op path is when compactIndex == firstIndex and entries are empty
	// Let's verify the correct error path
	err := w.Compact(10)
	require.Error(t, err, "compact at snapshot index with no entries should return ErrCompacted")
}

func TestCompact_TruncateAll(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(3, cs, nil))

	// Compact all entries
	require.NoError(t, w.Compact(3))

	// All entries should be removed
	idx, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), idx, "last index should be snapshot index")
}

// --- CreateSnapshot tests ---

func TestCreateSnapshot_EmptyStorage(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	require.NoError(t, w.CreateSnapshot(0, cs, []byte("initial")))

	snap, err := w.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(0), snap.Metadata.Index)
	require.Equal(t, uint64(0), snap.Metadata.Term)
	require.Equal(t, []byte("initial"), snap.Data)
}

func TestCreateSnapshot_RestoreOnEmptyStorage(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// On empty storage, creating a snapshot at a non-zero index (restore scenario)
	// should use term 1
	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(100, cs, []byte("restored")))

	snap, err := w.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(100), snap.Metadata.Index)
	require.Equal(t, uint64(1), snap.Metadata.Term, "restore snapshot should use term 1")
}

func TestCreateSnapshot_OutOfDate(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 3}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(3, cs, nil))

	// Creating another snapshot at a lower or equal index should fail
	err := w.CreateSnapshot(2, cs, nil)
	require.ErrorIs(t, err, raft.ErrSnapOutOfDate)

	err = w.CreateSnapshot(3, cs, nil)
	require.ErrorIs(t, err, raft.ErrSnapOutOfDate)
}

// --- unmarshalStateFile tests ---

func TestUnmarshalStateFile_TooShort(t *testing.T) {
	t.Parallel()

	err := unmarshalStateFile([]byte{1, 2, 3}, &raftpb.Snapshot{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "too short")
}

func TestUnmarshalStateFile_Truncated(t *testing.T) {
	t.Parallel()

	// Header says 100 bytes but data is only 16 bytes total
	data := make([]byte, 16)
	data[0] = 0
	data[1] = 0
	data[2] = 0
	data[3] = 0
	data[4] = 0
	data[5] = 0
	data[6] = 0
	data[7] = 100 // snapshot length = 100, but only 8 bytes remain

	err := unmarshalStateFile(data, &raftpb.Snapshot{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "truncated")
}

func TestUnmarshalStateFile_ValidRoundTrip(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Append entries and create a snapshot
	require.NoError(t, w.Append(raftpb.HardState{Term: 2, Vote: 1, Commit: 3}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 2, Data: []byte("b")},
		{Index: 3, Term: 2, Data: []byte("c")},
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(3, cs, []byte("test-data")))

	// Read the state file and unmarshal it
	data, err := os.ReadFile(w.stateFile)
	require.NoError(t, err)

	var snap raftpb.Snapshot
	require.NoError(t, unmarshalStateFile(data, &snap))
	require.Equal(t, uint64(3), snap.Metadata.Index)
	require.Equal(t, uint64(2), snap.Metadata.Term)
	require.Equal(t, []byte("test-data"), snap.Data)
}

// --- WAL persistence/restart test ---

func TestWAL_Persistence(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")
	dir := t.TempDir()

	// Create WAL and write data
	w, err := New(dir, logger, meter)
	require.NoError(t, err)

	require.NoError(t, w.Append(raftpb.HardState{Term: 3, Vote: 1, Commit: 2}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("first")},
		{Index: 2, Term: 3, Data: []byte("second")},
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1, 2}}
	require.NoError(t, w.CreateSnapshot(2, cs, []byte("snap")))

	require.NoError(t, w.Close())

	// Reopen WAL
	w2, err := New(dir, logger, meter)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	// Verify hard state persisted
	hs, gotCS, err := w2.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(3), hs.Term)
	require.Equal(t, uint64(1), hs.Vote)
	require.Equal(t, uint64(2), hs.Commit)
	require.Equal(t, []uint64{1, 2}, gotCS.Voters)

	// Verify snapshot persisted
	snap, err := w2.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(2), snap.Metadata.Index)
	require.Equal(t, []byte("snap"), snap.Data)

	// Verify entries: after snapshot at index 2, entries before the snapshot
	// may or may not be available depending on the etcd WAL replay.
	// At minimum, last index should be >= snapshot index.
	lastIdx, err := w2.LastIndex()
	require.NoError(t, err)
	require.GreaterOrEqual(t, lastIdx, uint64(2))
}

// --- WithPurgeInterval option test ---

func TestWithPurgeInterval(t *testing.T) {
	t.Parallel()

	w := &DefaultWAL{purgeInterval: defaultPurgeInterval}
	WithPurgeInterval(42 * time.Second)(w)
	require.Equal(t, 42*time.Second, w.purgeInterval)
}

// --- WAL repair tests ---

func TestWAL_RepairCorruptedEntry(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")
	dir := t.TempDir()

	// Step 1: Create a WAL and write data.
	w, err := New(dir, logger, meter)
	require.NoError(t, err)

	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 5}, []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("entry-1")},
		{Index: 2, Term: 1, Data: []byte("entry-2")},
		{Index: 3, Term: 1, Data: []byte("entry-3")},
		{Index: 4, Term: 1, Data: []byte("entry-4")},
		{Index: 5, Term: 1, Data: []byte("entry-5")},
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(3, cs, []byte("snapshot-data")))

	require.NoError(t, w.Append(raftpb.HardState{Term: 1, Vote: 1, Commit: 8}, []raftpb.Entry{
		{Index: 6, Term: 1, Data: []byte("entry-6")},
		{Index: 7, Term: 1, Data: []byte("entry-7")},
		{Index: 8, Term: 1, Data: []byte("entry-8")},
	}))

	require.NoError(t, w.Close())

	// Step 2: Corrupt the last WAL segment by truncating mid-record.
	etcdDir := filepath.Join(dir, "etcd")
	dirEntries, err := os.ReadDir(etcdDir)
	require.NoError(t, err)

	var lastWALFile string

	for _, e := range dirEntries {
		if filepath.Ext(e.Name()) == ".wal" {
			lastWALFile = filepath.Join(etcdDir, e.Name())
		}
	}

	require.NotEmpty(t, lastWALFile, "should find at least one .wal file")

	// etcd pre-allocates WAL files; find the end of actual data (last non-zero byte)
	// and truncate within it to simulate a partial write from OOMKill.
	fileData, err := os.ReadFile(lastWALFile)
	require.NoError(t, err)

	lastNonZero := len(fileData) - 1
	for lastNonZero > 0 && fileData[lastNonZero] == 0 {
		lastNonZero--
	}

	truncateAt := int64(lastNonZero - 5)
	require.Positive(t, truncateAt)
	require.NoError(t, os.Truncate(lastWALFile, truncateAt))

	// Step 3: Re-open — should auto-repair and succeed.
	w2, err := New(dir, logger, meter)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	// Step 4: Verify snapshot is preserved.
	snap, err := w2.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(3), snap.Metadata.Index)
	require.Equal(t, []byte("snapshot-data"), snap.Data)

	// Step 5: Verify some entries were recovered.
	lastIdx, err := w2.LastIndex()
	require.NoError(t, err)
	require.GreaterOrEqual(t, lastIdx, uint64(3), "should recover entries at least up to snapshot")

	// Step 6: Verify .broken backup file was created by wal.Repair.
	dirEntries, err = os.ReadDir(etcdDir)
	require.NoError(t, err)

	hasBroken := false

	for _, e := range dirEntries {
		if filepath.Ext(e.Name()) == ".broken" {
			hasBroken = true

			break
		}
	}

	require.True(t, hasBroken, "repair should create a .broken backup file")
}
