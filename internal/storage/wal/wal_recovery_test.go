package wal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

// These tests pin the WAL recovery contract against the etcd ReadAll defect
// described on reconstructEntries: a truncating overwrite written at or below
// the snapshot index is skipped together with the truncation it implies, so the
// physically earlier entries it replaced survive replay. The node's last-entry
// term then no longer describes its real log, which is enough for it to grant a
// vote it must refuse and let a replica missing committed entries win an
// election.
//
// The first three cases fail without the reconstruction. The last three pin the
// opposite hazard: a repair that is too eager discards healthy state or refuses
// a recoverable startup.

func testConfState() *raftpb.ConfState {
	return &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
}

// assertRecoveredLog reopens dir and asserts the log the node comes up with.
func assertRecoveredLog(t *testing.T, dir string, wantLastIndex, wantLastTerm uint64) *DefaultWAL {
	t.Helper()

	reopened := newTestWALAt(t, dir)

	lastIndex, err := reopened.LastIndex()
	require.NoError(t, err)
	require.Equal(t, wantLastIndex, lastIndex, "recovered log must end at the real last index")

	lastTerm, err := reopened.Term(lastIndex)
	require.NoError(t, err)
	require.Equal(t, wantLastTerm, lastTerm,
		"recovered last-entry term decides vote eligibility; a regressed term lets this node vote for a shorter log")

	return reopened
}

// TestRecovery_DiscardsResurrectedLowerTermSuffix is the failure Antithesis
// observed: n1 recovered [87/5 … 92/5] under snapshot 86/12 and voted for a
// replica whose log was missing two acknowledged transactions.
func TestRecovery_DiscardsResurrectedLowerTermSuffix(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)

	// The suffix that a later leader will overwrite.
	old := make([]*raftpb.Entry, 0, 13)
	for i := uint64(80); i <= 92; i++ {
		old = append(old, ent(i, 5, []byte("old")))
	}

	require.NoError(t, w.Append(hs(5, 1, 79), old))

	// One truncating append at index 80 replaces the whole suffix with a
	// shorter, newer-term history. etcd's WAL is append-only, so both versions
	// are physically present and only replay decides which one survives.
	require.NoError(t, w.Append(hs(12, 1, 86), []*raftpb.Entry{
		ent(80, 6, []byte("new")),
		ent(81, 7, []byte("new")),
		ent(82, 8, []byte("new")),
		ent(83, 10, []byte("new")),
		ent(84, 10, []byte("new")),
		ent(85, 12, []byte("new")),
		ent(86, 12, []byte("new")),
	}))

	require.NoError(t, w.CreateSnapshot(86, testConfState(), nil))
	require.NoError(t, w.Close())

	reopened := assertRecoveredLog(t, dir, 86, 12)

	for _, e := range reopened.entries {
		require.NotEqual(t, uint64(5), e.GetTerm(),
			"an overwritten term-5 entry must not survive replay (index %d)", e.GetIndex())
	}
}

// TestRecovery_DiscardsResurrectedHigherTermSuffix covers the backfill shape: a
// later leader replicates older committed entries, which keep their original
// term. The resurrected suffix then carries a term *higher* than the snapshot's,
// so any check comparing the recovered term against the snapshot term is blind
// to it.
func TestRecovery_DiscardsResurrectedHigherTermSuffix(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)

	// Uncommitted entries this node accepted from a leader in term 20.
	local := make([]*raftpb.Entry, 0, 13)
	for i := uint64(80); i <= 92; i++ {
		local = append(local, ent(i, 20, []byte("term20")))
	}

	require.NoError(t, w.Append(hs(20, 2, 79), local))

	// A leader elected in term 21 backfills the entries actually committed in
	// term 12. They are replicated with their original term, which is lower
	// than the suffix they replace.
	backfilled := make([]*raftpb.Entry, 0, 7)
	for i := uint64(80); i <= 86; i++ {
		backfilled = append(backfilled, ent(i, 12, []byte("term12")))
	}

	require.NoError(t, w.Append(hs(21, 3, 86), backfilled))

	require.NoError(t, w.CreateSnapshot(86, testConfState(), nil))
	require.NoError(t, w.Close())

	reopened := assertRecoveredLog(t, dir, 86, 12)

	for _, e := range reopened.entries {
		require.NotEqual(t, uint64(20), e.GetTerm(),
			"an overwritten term-20 entry must not survive replay (index %d)", e.GetIndex())
	}
}

// TestRecovery_DiscardsSuffixConflictingWithInstalledSnapshot covers the second
// escape: ApplySnapshot clears the entry cache in memory but persists only
// snapshot records and HardState, so the conflicting suffix is still on disk and
// replay restores it.
func TestRecovery_DiscardsSuffixConflictingWithInstalledSnapshot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)

	local := make([]*raftpb.Entry, 0, 13)
	for i := uint64(80); i <= 92; i++ {
		local = append(local, ent(i, 20, []byte("term20")))
	}

	require.NoError(t, w.Append(hs(20, 2, 79), local))

	// The leader sends a snapshot that conflicts at index 86 (term 12, not 20),
	// so every local entry from 80 on is obsolete.
	require.NoError(t, w.ApplySnapshot(&raftpb.Snapshot{
		Metadata: snapshotMeta(86, 12, testConfState()),
	}))
	require.NoError(t, w.Close())

	reopened := assertRecoveredLog(t, dir, 86, 12)
	require.Empty(t, reopened.entries,
		"an installed snapshot conflicting at its own index invalidates the whole cached suffix")
}

// TestRecovery_KeepsEntriesAfterInterruptedSnapshotInstall pins the opposite
// hazard. ApplySnapshot persists a guard snapshot record *before* advancing
// HardState, so a crash in between leaves a record that does not describe
// durable state. Honouring it would delete committed entries and refuse a
// recoverable startup.
func TestRecovery_KeepsEntriesAfterInterruptedSnapshotInstall(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)

	entries := make([]*raftpb.Entry, 0, 10)
	for i := uint64(1); i <= 10; i++ {
		entries = append(entries, ent(i, 1, []byte("d")))
	}

	require.NoError(t, w.Append(hs(1, 1, 7), entries))
	require.NoError(t, w.CreateSnapshot(5, testConfState(), nil))

	// The guard record an interrupted ApplySnapshot leaves behind: index 9 is
	// beyond the durable commit of 7, and no HardState follows it.
	require.NoError(t, w.wal.SaveSnapshot(&walpb.Snapshot{
		Index:     new(uint64(9)),
		Term:      new(uint64(2)),
		ConfState: testConfState(),
	}))
	require.NoError(t, w.Close())

	reopened := assertRecoveredLog(t, dir, 10, 1)
	require.Len(t, reopened.entries, 5, "entries 6..10 must survive an unacknowledged guard record")
	require.Equal(t, uint64(6), reopened.entries[0].GetIndex())
}

// TestRecovery_KeepsOverwriteBelowFinalCommit pins that an ordinary overwrite of
// uncommitted entries is not mistaken for corruption. Indices 2 and 3 are
// replaced before the commit index reaches them, so the discarded versions sit
// inside the final committed range yet were never committed.
func TestRecovery_KeepsOverwriteBelowFinalCommit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)

	require.NoError(t, w.Append(hs(1, 1, 1), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
	}))
	require.NoError(t, w.CreateSnapshot(1, testConfState(), nil))

	// A new leader in term 2 replaces the uncommitted tail, then it commits.
	require.NoError(t, w.Append(hs(2, 2, 3), []*raftpb.Entry{
		ent(2, 2, []byte("b2")),
		ent(3, 2, []byte("c2")),
	}))
	require.NoError(t, w.Close())

	reopened := assertRecoveredLog(t, dir, 3, 2)
	require.Len(t, reopened.entries, 2)
	require.Equal(t, uint64(2), reopened.entries[0].GetTerm(), "the term-2 version must win")
}

// TestRecovery_KeepsHealthyTailAfterSnapshot is the plain case: entries written
// after a local snapshot stay available for follower catch-up.
func TestRecovery_KeepsHealthyTailAfterSnapshot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)

	entries := make([]*raftpb.Entry, 0, 9)
	for i := uint64(80); i <= 88; i++ {
		entries = append(entries, ent(i, 12, []byte("d")))
	}

	require.NoError(t, w.Append(hs(12, 1, 88), entries))
	require.NoError(t, w.CreateSnapshot(86, testConfState(), nil))
	require.NoError(t, w.Close())

	reopened := assertRecoveredLog(t, dir, 88, 12)
	require.Len(t, reopened.entries, 2, "entries 87 and 88 must remain available")
	require.Equal(t, uint64(87), reopened.entries[0].GetIndex())
}

// TestRecovery_SucceedsAfterSegmentReclamation pins the CRC requirement. A
// decoder built over a subset of the segments starts at CRC zero and must import
// each segment's seed; without that the first record of a reclaimed WAL fails to
// validate and recovery refuses an entirely healthy node.
func TestRecovery_SucceedsAfterSegmentReclamation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir, withPurgeInterval(100*time.Millisecond))

	// etcd preallocates 64MB segments, and ReleaseLockTo only releases the
	// oldest lock once at least three exist — same shape as the purge test.
	const numEntries = 10

	entryData := make([]byte, 20*1024*1024)
	for i := uint64(1); i <= numEntries; i++ {
		require.NoError(t, w.Append(hs(1, 1, i), []*raftpb.Entry{ent(i, 1, entryData)}))
	}

	segmentsAfterWrite := countWALFiles(t, w.etcdWalDir)
	require.GreaterOrEqual(t, segmentsAfterWrite, 3, "writing ~200MB should create at least 3 WAL segments")

	require.NoError(t, w.CreateSnapshot(numEntries, testConfState(), nil))
	require.NoError(t, w.Compact(numEntries))

	require.Eventually(t, func() bool {
		return countWALFiles(t, w.etcdWalDir) < segmentsAfterWrite
	}, 10*time.Second, 200*time.Millisecond, "old WAL segments should be purged")

	require.NoError(t, w.Close())

	assertRecoveredLog(t, dir, numEntries, 1)
}

// TestResolveWALRecords covers the fold in isolation, without a WAL on disk.
func TestResolveWALRecords(t *testing.T) {
	t.Parallel()

	entryRec := func(index, term uint64) walRecord {
		return walRecord{entry: ent(index, term, nil)}
	}
	snapRec := func(index, term uint64) walRecord {
		return walRecord{snap: &walpb.Snapshot{Index: new(index), Term: new(term)}}
	}

	type want struct {
		index uint64
		term  uint64
	}

	for _, tc := range []struct {
		name      string
		records   []walRecord
		snapIndex uint64
		commit    uint64
		want      []want
	}{
		{
			name:      "overwrite truncates the replaced suffix",
			records:   []walRecord{entryRec(1, 1), entryRec(2, 1), entryRec(3, 1), entryRec(2, 2)},
			snapIndex: 0,
			commit:    2,
			want:      []want{{1, 1}, {2, 2}},
		},
		{
			name:      "snapshot drops only the compacted prefix when it agrees",
			records:   []walRecord{entryRec(1, 1), entryRec(2, 1), entryRec(3, 1), snapRec(2, 1)},
			snapIndex: 2,
			commit:    3,
			want:      []want{{3, 1}},
		},
		{
			name:      "snapshot conflicting at its index invalidates the whole log",
			records:   []walRecord{entryRec(1, 1), entryRec(2, 5), entryRec(3, 5), snapRec(2, 9)},
			snapIndex: 2,
			commit:    3,
			want:      nil,
		},
		{
			name:      "snapshot beyond the durable commit has no effect",
			records:   []walRecord{entryRec(1, 1), entryRec(2, 1), entryRec(3, 1), snapRec(3, 9)},
			snapIndex: 0,
			commit:    2,
			want:      []want{{1, 1}, {2, 1}, {3, 1}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := resolveWALRecords(tc.records, tc.snapIndex, tc.commit)

			require.Len(t, got, len(tc.want))

			for i, w := range tc.want {
				require.Equal(t, w.index, got[i].GetIndex(), "entry %d index", i)
				require.Equal(t, w.term, got[i].GetTerm(), "entry %d term", i)
			}
		})
	}
}
