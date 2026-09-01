package wal

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	etcdwal "go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// hs builds a *raftpb.HardState from scalar fields for terse test literals.
func hs(term, vote, commit uint64) *raftpb.HardState {
	return &raftpb.HardState{
		Term:   new(term),
		Vote:   new(vote),
		Commit: new(commit),
	}
}

// ent builds a *raftpb.Entry from scalar fields for terse test literals.
func ent(index, term uint64, data []byte) *raftpb.Entry {
	return &raftpb.Entry{
		Index: new(index),
		Term:  new(term),
		Data:  data,
	}
}

// snapshotMeta builds a *raftpb.SnapshotMetadata for terse test literals.
func snapshotMeta(index, term uint64, cs *raftpb.ConfState) *raftpb.SnapshotMetadata {
	return &raftpb.SnapshotMetadata{
		Index:     new(index),
		Term:      new(term),
		ConfState: cs,
	}
}

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

type stubWALInitializer struct {
	readErr       error
	saveErr       error
	closeErr      error
	readCalls     int
	saveCalls     int
	closeCalls    int
	savedSnapshot *walpb.Snapshot
}

func (s *stubWALInitializer) ReadAll() ([]byte, *raftpb.HardState, []*raftpb.Entry, error) {
	s.readCalls++

	return nil, nil, nil, s.readErr
}

func (s *stubWALInitializer) SaveSnapshot(snapshot *walpb.Snapshot) error {
	s.saveCalls++
	s.savedSnapshot = snapshot

	return s.saveErr
}

func (s *stubWALInitializer) Close() error {
	s.closeCalls++

	return s.closeErr
}

func TestInitializeNewWAL(t *testing.T) {
	t.Parallel()

	readFailure := errors.New("read failure")
	saveFailure := errors.New("save failure")
	closeFailure := errors.New("close failure")

	tests := []struct {
		name          string
		initializer   *stubWALInitializer
		expectedError []error
		expectedReads int
		expectedSaves int
	}{
		{
			name:          "success",
			initializer:   &stubWALInitializer{},
			expectedReads: 1,
			expectedSaves: 1,
		},
		{
			name:          "read failure still closes",
			initializer:   &stubWALInitializer{readErr: readFailure},
			expectedError: []error{readFailure},
			expectedReads: 1,
		},
		{
			name:          "read and close failures are joined",
			initializer:   &stubWALInitializer{readErr: readFailure, closeErr: closeFailure},
			expectedError: []error{readFailure, closeFailure},
			expectedReads: 1,
		},
		{
			name:          "save failure still closes",
			initializer:   &stubWALInitializer{saveErr: saveFailure},
			expectedError: []error{saveFailure},
			expectedReads: 1,
			expectedSaves: 1,
		},
		{
			name:          "save and close failures are joined",
			initializer:   &stubWALInitializer{saveErr: saveFailure, closeErr: closeFailure},
			expectedError: []error{saveFailure, closeFailure},
			expectedReads: 1,
			expectedSaves: 1,
		},
		{
			name:          "final close failure",
			initializer:   &stubWALInitializer{closeErr: closeFailure},
			expectedError: []error{closeFailure},
			expectedReads: 1,
			expectedSaves: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := initializeNewWAL(test.initializer)
			if len(test.expectedError) == 0 {
				require.NoError(t, err)
			} else {
				for _, expected := range test.expectedError {
					require.ErrorIs(t, err, expected)
				}
			}
			require.Equal(t, test.expectedReads, test.initializer.readCalls)
			require.Equal(t, test.expectedSaves, test.initializer.saveCalls)
			require.Equal(t, 1, test.initializer.closeCalls)
			if test.expectedSaves == 1 {
				require.True(t, isCompactionMarker(test.initializer.savedSnapshot))
				require.Zero(t, test.initializer.savedSnapshot.GetIndex())
				require.Zero(t, test.initializer.savedSnapshot.GetTerm())
			} else {
				require.Nil(t, test.initializer.savedSnapshot)
			}
		})
	}
}

// withPurgeInterval is a test helper that returns an Option setting the purge interval.
func withPurgeInterval(d time.Duration) Option {
	return func(w *DefaultWAL) { w.purgeInterval = d }
}

func newTestWAL(t *testing.T, opts ...Option) *DefaultWAL {
	t.Helper()

	return newTestWALAt(t, t.TempDir(), opts...)
}

func newTestWALAt(t *testing.T, dir string, opts ...Option) *DefaultWAL {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	w, err := New(dir, logger, meter, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	return w
}

// newWALWithRetainedMargin reproduces the maintenance sequence behind EN-1925:
// an older compaction boundary remains in force while a newer snapshot is
// published. The subsequent Compact call is older than FirstIndex and is a
// no-op, as happens when the configured margin is wider than the entries
// accumulated since the previous snapshot.
func newWALWithRetainedMargin(t *testing.T) *DefaultWAL {
	t.Helper()

	return newWALWithRetainedMarginAt(t, t.TempDir())
}

func newWALWithRetainedMarginAt(t *testing.T, dir string) *DefaultWAL {
	t.Helper()

	w := newTestWALAt(t, dir)
	entries := make([]*raftpb.Entry, 5)
	for i := range entries {
		entries[i] = ent(uint64(i+1), 1, []byte{byte(i + 1)})
	}

	require.NoError(t, w.Append(hs(1, 1, 5), entries))
	cs := &raftpb.ConfState{Voters: []uint64{1, 2}}
	require.NoError(t, w.CreateSnapshot(2, cs, nil))
	require.NoError(t, w.Compact(2))
	require.NoError(t, w.CreateSnapshot(5, cs, nil))
	require.ErrorIs(t, w.Compact(1), raft.ErrCompacted)

	return w
}

func TestPurgeOldWALSegments(t *testing.T) {
	t.Parallel()

	t.Run("after normal compaction", func(t *testing.T) {
		assertOldWALSegmentsPurged(t, func(w *DefaultWAL, snapshotIndex uint64) {
			require.NoError(t, w.CreateSnapshot(snapshotIndex, &raftpb.ConfState{Voters: []uint64{1}}, nil))
			require.NoError(t, w.Compact(snapshotIndex))
		})
	})

	t.Run("after received snapshot", func(t *testing.T) {
		assertOldWALSegmentsPurged(t, func(w *DefaultWAL, snapshotIndex uint64) {
			require.NoError(t, w.ApplySnapshot(&raftpb.Snapshot{
				Metadata: snapshotMeta(snapshotIndex, 1, &raftpb.ConfState{Voters: []uint64{1}}),
			}))
		})
	})
}

func assertOldWALSegmentsPurged(t *testing.T, triggerReclamation func(*DefaultWAL, uint64)) {
	t.Helper()

	w := newTestWAL(t, withPurgeInterval(100*time.Millisecond))

	// Write enough data to force WAL segment rotation.
	// etcd WAL segments are 64MB, so writing ~200MB should create at least 3 segments.
	// ReleaseLockTo requires at least 3 segments to actually release the oldest lock,
	// because it always keeps the segment just before the target index locked.
	const numEntries = 10

	entryData := make([]byte, 20*1024*1024)
	for i := uint64(1); i <= numEntries; i++ {
		err := w.Append(
			hs(1, 1, i),
			[]*raftpb.Entry{ent(i, 1, entryData)},
		)
		require.NoError(t, err)
	}

	segmentsAfterWrite := countWALFiles(t, w.etcdWalDir)
	require.GreaterOrEqual(t, segmentsAfterWrite, 3, "writing ~200MB should create at least 3 WAL segments")

	triggerReclamation(w, numEntries)

	// The background purger should eventually delete old unlocked segments.
	require.Eventually(t, func() bool {
		return countWALFiles(t, w.etcdWalDir) < segmentsAfterWrite
	}, 10*time.Second, 200*time.Millisecond, "old WAL segments should be purged")
}

// --- EN-1525: fail-closed on a missing creation marker ---

// walDirWithMarkerRemoved builds a WAL under a fresh dataDir, applies mutate
// (may be nil for a pristine empty WAL), closes it, then deletes the creation
// marker so a subsequent New() takes the marker-missing branch. Returns the
// dataDir and its etcd WAL subdir.
func walDirWithMarkerRemoved(t *testing.T, mutate func(t *testing.T, w *DefaultWAL)) (string, string) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dir := t.TempDir()
	w, err := New(dir, logger, meter)
	require.NoError(t, err)
	if mutate != nil {
		mutate(t, w)
	}
	require.NoError(t, w.Close())
	require.NoError(t, os.Remove(filepath.Join(dir, walCreationCompletedFile)),
		"the creation marker must exist after a normal create")

	return dir, filepath.Join(dir, etcdWalDir)
}

// reopenWAL runs New() against an existing dataDir (the marker-missing branch)
// and returns the resulting error, closing the WAL on success.
func reopenWAL(t *testing.T, dir string) error {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	w, err := New(dir, logger, meter)
	if err == nil {
		t.Cleanup(func() { _ = w.Close() })
	}

	return err
}

// TestNew_MarkerMissing_EmptyWAL_Recreates covers the disposable
// empty-bootstrap remnant: a crash between wal.Create and the initial
// compaction-marker write leaves a fresh, state-free WAL. New() must clean it
// up and recreate the marker (existing bootstrap-recovery behaviour).
func TestNew_MarkerMissing_EmptyWAL_Recreates(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w, err := etcdwal.Create(zap.NewNop(), filepath.Join(dir, etcdWalDir), nil)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	require.NoError(t, reopenWAL(t, dir), "a verified-empty WAL must be recreated, not rejected")
	_, statErr := os.Stat(filepath.Join(dir, walCreationCompletedFile))
	require.NoError(t, statErr, "the creation marker must be rewritten after recreation")
}

// TestNew_MarkerMissing_InitialCompactionMarker_Recreates covers a crash after
// the initial replay cursor is durable but before WAL_CREATION_COMPLETED is
// published. The sentinel snapshot carries no consensus state, so the next
// startup must still recognize this prefix as an empty bootstrap remnant.
func TestNew_MarkerMissing_InitialCompactionMarker_Recreates(t *testing.T) {
	t.Parallel()

	dir, etcdDir := walDirWithMarkerRemoved(t, nil)
	walSnaps, err := etcdwal.ValidSnapshotEntries(zap.NewNop(), etcdDir)
	require.NoError(t, err)
	require.NotNil(t, latestCompactionMarker(walSnaps), "the interrupted prefix must contain the initial replay cursor")

	require.NoError(t, reopenWAL(t, dir), "an initialized but unpublished empty WAL must be recreated")
	_, statErr := os.Stat(filepath.Join(dir, walCreationCompletedFile))
	require.NoError(t, statErr, "the creation marker must be published after recreation")
}

func TestNew_CompletedWALWithoutCompactionMarkerFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w, err := etcdwal.Create(zap.NewNop(), filepath.Join(dir, etcdWalDir), nil)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, os.WriteFile(filepath.Join(dir, walCreationCompletedFile), nil, 0600))

	err = reopenWAL(t, dir)
	require.ErrorContains(t, err, "existing WAL has no persisted compaction marker")
}

func TestNew_CompactionMarkerAfterLatestSnapshotFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	require.NoError(t, w.Close())

	rawWAL, err := etcdwal.Open(zap.NewNop(), filepath.Join(dir, etcdWalDir), newCompactionMarker(0, 0))
	require.NoError(t, err)
	_, _, _, err = rawWAL.ReadAll()
	require.NoError(t, err)
	require.NoError(t, rawWAL.Save(hs(1, 1, 10), nil))
	require.NoError(t, rawWAL.SaveSnapshot(newCompactionMarker(10, 1)))
	require.NoError(t, rawWAL.Close())

	err = reopenWAL(t, dir)
	require.ErrorContains(t, err, "persisted compaction index 10 is after latest snapshot index 0")
}

// TestNew_MarkerMissing_PopulatedHardState_FailsClosed is the core EN-1525
// guarantee: a WAL holding a HardState (votes/commit) must never be deleted
// merely because the marker is absent. In the default test build assert is a
// no-op, so the mandatory return — not the assert — must stop the fall-through
// to os.RemoveAll: New() returns an error AND the WAL survives untouched.
func TestNew_MarkerMissing_PopulatedHardState_FailsClosed(t *testing.T) {
	t.Parallel()

	dir, etcdDir := walDirWithMarkerRemoved(t, func(t *testing.T, w *DefaultWAL) {
		require.NoError(t, w.Append(hs(2, 1, 5), []*raftpb.Entry{ent(1, 2, []byte("x"))}))
	})
	before := countWALFiles(t, etcdDir)
	require.Positive(t, before)

	err := reopenWAL(t, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "refusing to delete")
	require.Equal(t, before, countWALFiles(t, etcdDir), "populated WAL must be preserved")
}

// TestNew_MarkerMissing_EntriesOnly_FailsClosed pins that "populated" covers a
// WAL with log entries even when the HardState is still empty (term/vote/commit
// all zero) — it is not the disposable remnant and must not be deleted.
func TestNew_MarkerMissing_EntriesOnly_FailsClosed(t *testing.T) {
	t.Parallel()

	dir, etcdDir := walDirWithMarkerRemoved(t, func(t *testing.T, w *DefaultWAL) {
		// Empty HardState + a log entry: Append keeps the empty HardState and
		// persists the entry, yielding an entries-only WAL on reopen.
		require.NoError(t, w.Append(hs(0, 0, 0), []*raftpb.Entry{ent(1, 1, []byte("payload"))}))
	})
	before := countWALFiles(t, etcdDir)
	require.Positive(t, before)

	err := reopenWAL(t, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "refusing to delete")
	require.Equal(t, before, countWALFiles(t, etcdDir), "entries-only WAL must be preserved")
}

// TestNew_MarkerMissing_MalformedWAL_FailsClosed pins that an unreadable /
// ambiguous WAL is never coerced to "empty" and deleted: it fails startup with
// a contextual error and the files survive.
func TestNew_MarkerMissing_MalformedWAL_FailsClosed(t *testing.T) {
	t.Parallel()

	dir, etcdDir := walDirWithMarkerRemoved(t, func(t *testing.T, w *DefaultWAL) {
		require.NoError(t, w.Append(hs(3, 2, 9), []*raftpb.Entry{ent(1, 3, []byte("y"))}))
	})

	// Corrupt every WAL segment in place.
	segments, err := filepath.Glob(filepath.Join(etcdDir, "*.wal"))
	require.NoError(t, err)
	require.NotEmpty(t, segments)
	for _, seg := range segments {
		info, statErr := os.Stat(seg)
		require.NoError(t, statErr)
		garbage := make([]byte, info.Size())
		for i := range garbage {
			garbage[i] = 0xFF
		}
		require.NoError(t, os.WriteFile(seg, garbage, 0o600))
	}
	before := countWALFiles(t, etcdDir)

	err = reopenWAL(t, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "refusing to delete")
	require.Equal(t, before, countWALFiles(t, etcdDir), "unverifiable WAL must be preserved")
}

// tearWALTail simulates a torn write on the final segment: it locates the end of
// the valid records (etcd preallocates and zero-fills the segment, so the first
// zero-length frame header marks the tail), writes a frame header claiming a
// payload larger than the bytes that follow, and truncates so the claimed
// payload is missing. On the next read the decoder reads the length, cannot
// io.ReadFull the payload, and returns io.ErrUnexpectedEOF — exactly the
// crash-mid-Save signature that wal.ReadAll silently tolerates in read mode.
func tearWALTail(t *testing.T, etcdDir string) {
	t.Helper()

	segments, err := filepath.Glob(filepath.Join(etcdDir, "*.wal"))
	require.NoError(t, err)
	require.NotEmpty(t, segments)
	slices.Sort(segments)
	last := segments[len(segments)-1]

	data, err := os.ReadFile(last)
	require.NoError(t, err)

	// Walk framed records until the zeroed preallocated tail (length field 0).
	off := int64(0)
	for off+8 <= int64(len(data)) {
		l := int64(binary.LittleEndian.Uint64(data[off : off+8]))
		if l == 0 {
			break
		}
		recBytes := int64(uint64(l) & ^(uint64(0xff) << 56))
		var padBytes int64
		if l < 0 {
			padBytes = int64((uint64(l) >> 56) & 0x7)
		}
		off += 8 + recBytes + padBytes
	}
	require.Positive(t, off, "must have decoded at least one valid record before the tail")

	const claimedPayload = int64(256)
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint64(hdr, uint64(claimedPayload))
	f, err := os.OpenFile(last, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(hdr, off)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Keep only half the claimed payload on disk: a torn write.
	require.NoError(t, os.Truncate(last, off+8+claimedPayload/2))
}

// TestNew_MarkerMissing_TornTail_FailsClosed pins the torn-tail gap: a WAL whose
// consensus record was only partially written (crash/power-loss mid-Save) must
// fail closed even when every fully-decodable record before it is state-free.
// This is the case wal.ReadAll's read-mode path hides: it silently accepts
// io.ErrUnexpectedEOF and returns the (empty) records decoded so far, so the WAL
// reads as not-populated and would be deleted — discarding the torn but
// acknowledged consensus write. Starting from a fresh bootstrap WAL (no Append)
// isolates the torn record as the only thing distinguishing this from the
// disposable empty remnant, so the fail-closed decision rests entirely on the
// EN-1525 raw-decoder terminal-error check, not on any surviving state record.
func TestNew_MarkerMissing_TornTail_FailsClosed(t *testing.T) {
	t.Parallel()

	dir, etcdDir := walDirWithMarkerRemoved(t, nil)

	tearWALTail(t, etcdDir)
	before := countWALFiles(t, etcdDir)

	err := reopenWAL(t, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "refusing to delete")
	require.Equal(t, before, countWALFiles(t, etcdDir), "torn-tail WAL must be preserved")
}

// TestNew_MarkerMissing_SnapshotConfStateOnly_FailsClosed pins the snapshot-record
// gap: CreateSnapshot persists a walpb.Snapshot carrying the cluster ConfState
// but writes no HardState or log entry. wal.ReadAll drops snapshot records, so
// such a WAL — the initial CreateSnapshot(0, initialConfState) crashing before
// any consensus write — would read as empty and be deleted, discarding the
// persisted cluster membership. The raw-decoder scan classifies a snapshot record
// with voters as populated even at index 0.
func TestNew_MarkerMissing_SnapshotConfStateOnly_FailsClosed(t *testing.T) {
	t.Parallel()

	dir, etcdDir := walDirWithMarkerRemoved(t, func(t *testing.T, w *DefaultWAL) {
		// Index 0 initial snapshot carrying only the cluster membership.
		require.NoError(t, w.CreateSnapshot(0, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}, nil))
	})
	before := countWALFiles(t, etcdDir)
	require.Positive(t, before)

	err := reopenWAL(t, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "refusing to delete")
	require.Equal(t, before, countWALFiles(t, etcdDir), "snapshot-ConfState WAL must be preserved")
}

// --- InitialState tests ---

func TestInitialState_EmptyWAL(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	hs, cs, err := w.InitialState()
	require.NoError(t, err)
	require.True(t, raft.IsEmptyHardState(hs), "empty WAL should return empty hard state")
	// raft v3.7 dereferences the returned ConfState in confchange.Restore, so an
	// empty WAL must still return a non-nil (empty) ConfState, never nil.
	require.NotNil(t, cs, "empty WAL must return a non-nil empty conf state")
	require.Empty(t, cs.GetVoters(), "empty WAL should return empty conf state")
}

// TestNewRawNode_EmptyWAL is the end-to-end regression for the nil-ConfState
// panic: raft v3.7's newRaft feeds InitialState's ConfState straight into
// confchange.Restore, so a freshly created (never-snapshotted) DefaultWAL used
// as raft.Storage must not panic. Without EnsureConfState in InitialState this
// panics with a nil pointer dereference.
func TestNewRawNode_EmptyWAL(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NotPanics(t, func() {
		_, err := raft.NewRawNode(&raft.Config{
			ID:              1,
			ElectionTick:    10,
			HeartbeatTick:   1,
			Storage:         w,
			MaxSizePerMsg:   1024 * 1024,
			MaxInflightMsgs: 256,
		})
		require.NoError(t, err)
	}, "NewRawNode on a fresh empty WAL must not panic (raft v3.7 nil ConfState)")
}

func TestInitialState_AfterAppend(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	hs := hs(3, 1, 5)
	err := w.Append(hs, []*raftpb.Entry{
		ent(1, 1, []byte("data")),
	})
	require.NoError(t, err)

	gotHS, _, err := w.InitialState()
	require.NoError(t, err)
	require.Equal(t, hs.GetTerm(), gotHS.GetTerm())
	require.Equal(t, hs.GetVote(), gotHS.GetVote())
	require.Equal(t, hs.GetCommit(), gotHS.GetCommit())
}

func TestInitialState_AfterSnapshot(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Create initial snapshot on empty storage
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	require.NoError(t, w.CreateSnapshot(0, cs, nil))

	_, gotCS, err := w.InitialState()
	require.NoError(t, err)
	require.Equal(t, cs.GetVoters(), gotCS.GetVoters())
}

// TestInitialState_ConcurrentNoRace is the regression for the RLock data race:
// EnsureConfState mutates its argument in place (it fills a nil AutoLeave
// pointer). If InitialState passed s.snapshot's own ConfState to EnsureConfState,
// concurrent callers holding only an RLock would write through the shared pointer
// and race with each other. InitialState must instead work on a copy. Run under
// -race with many goroutines; the shared snapshot ConfState here deliberately has
// a nil AutoLeave (the exact field EnsureConfState mutates).
func TestInitialState_ConcurrentNoRace(t *testing.T) {
	t.Parallel()

	// Snapshot whose ConfState has a nil AutoLeave — this is what makes
	// EnsureConfState want to mutate the shared value.
	w := newTestWAL(t)
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	require.Nil(t, cs.AutoLeave, "test precondition: shared ConfState must have nil AutoLeave")
	require.NoError(t, w.CreateSnapshot(0, cs, nil))

	const goroutines = 64

	var (
		start sync.WaitGroup
		done  sync.WaitGroup
	)
	start.Add(1)
	done.Add(goroutines)

	for range goroutines {
		go func() {
			defer done.Done()
			start.Wait()

			_, gotCS, err := w.InitialState()
			require.NoError(t, err)
			require.NotNil(t, gotCS, "InitialState must return a non-nil ConfState")
			require.NotNil(t, gotCS.AutoLeave, "returned ConfState must have non-nil AutoLeave")
			require.Equal(t, []uint64{1, 2, 3}, gotCS.GetVoters())
		}()
	}

	start.Done()
	done.Wait()

	// The shared snapshot's ConfState must remain untouched (its AutoLeave still
	// nil): InitialState worked on a copy, it did not mutate shared state.
	require.Nil(t, w.snapshot.GetMetadata().GetConfState().AutoLeave,
		"InitialState must not mutate the shared snapshot ConfState")
}

// TestInitialState_ConcurrentEmptyWALNoRace runs many concurrent InitialState
// calls on a fresh, never-snapshotted WAL under -race. Even with no shared
// ConfState to copy, concurrent readers must not race.
func TestInitialState_ConcurrentEmptyWALNoRace(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	const goroutines = 64

	var (
		start sync.WaitGroup
		done  sync.WaitGroup
	)
	start.Add(1)
	done.Add(goroutines)

	for range goroutines {
		go func() {
			defer done.Done()
			start.Wait()

			_, gotCS, err := w.InitialState()
			require.NoError(t, err)
			require.NotNil(t, gotCS, "empty WAL must return a non-nil ConfState")
			require.NotNil(t, gotCS.AutoLeave, "returned ConfState must have non-nil AutoLeave")
		}()
	}

	start.Done()
	done.Wait()
}

// --- Entries tests ---

func TestEntries_Basic(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
		ent(4, 2, []byte("d")),
		ent(5, 2, []byte("e")),
	}
	require.NoError(t, w.Append(hs(2, 1, 5), entries))

	// Read all entries
	got, err := w.Entries(1, 6, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, got, 5)
	require.Equal(t, uint64(1), got[0].GetIndex())
	require.Equal(t, uint64(5), got[4].GetIndex())
}

func TestEntries_SubRange(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
	}
	require.NoError(t, w.Append(hs(1, 1, 3), entries))

	got, err := w.Entries(2, 4, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, uint64(2), got[0].GetIndex())
	require.Equal(t, uint64(3), got[1].GetIndex())
}

func TestEntries_MaxSizeLimit(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []*raftpb.Entry{
		ent(1, 1, make([]byte, 100)),
		ent(2, 1, make([]byte, 100)),
		ent(3, 1, make([]byte, 100)),
	}
	require.NoError(t, w.Append(hs(1, 1, 3), entries))

	// Use a very small maxSize that should still return at least the first entry
	got, err := w.Entries(1, 4, 1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(got), 1, "should return at least 1 entry even with small maxSize")
	require.LessOrEqual(t, len(got), 3)
}

func TestEntries_InvalidRange(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(hs(1, 1, 1), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
	}))

	// lo >= hi
	_, err := w.Entries(5, 3, math.MaxUint64)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid range")
}

func TestEntries_Compacted(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
	}
	require.NoError(t, w.Append(hs(1, 1, 3), entries))

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

	require.NoError(t, w.Append(hs(1, 1, 2), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
	}))

	_, err := w.Entries(1, 100, math.MaxUint64)
	require.Error(t, err)
}

// --- Term tests ---

func TestTerm_Basic(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 2, []byte("b")),
		ent(3, 3, []byte("c")),
	}
	require.NoError(t, w.Append(hs(3, 1, 3), entries))

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

	entries := []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 2, []byte("b")),
		ent(3, 3, []byte("c")),
	}
	require.NoError(t, w.Append(hs(3, 1, 3), entries))

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

	require.NoError(t, w.Append(hs(1, 1, 2), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
	}))

	// Beyond last entry
	_, err := w.Term(100)
	require.Error(t, err)
}

func TestTerm_Compacted(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	entries := []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 2, []byte("b")),
		ent(3, 3, []byte("c")),
	}
	require.NoError(t, w.Append(hs(3, 1, 3), entries))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(2, cs, nil))
	require.NoError(t, w.Compact(2))

	// Entry before snapshot should be compacted
	_, err := w.Term(1)
	require.ErrorIs(t, err, raft.ErrCompacted)
}

func TestTerm_GapBeforeCachedEntriesIsCompacted(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Exercise the defensive sparse-window branch: an index before the cached
	// entries is unavailable unless it is the persisted compaction boundary.
	require.NoError(t, w.Append(hs(1, 1, 3), []*raftpb.Entry{
		ent(3, 1, []byte("c")),
	}))

	_, err := w.Term(2)
	require.ErrorIs(t, err, raft.ErrCompacted)
}

func TestStorageRetainedMarginAfterSnapshotAdvance(t *testing.T) {
	t.Parallel()

	w := newWALWithRetainedMargin(t)

	firstIndex, err := w.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), firstIndex)

	boundaryTerm, err := w.Term(firstIndex - 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), boundaryTerm)

	retainedTerm, err := w.Term(firstIndex)
	require.NoError(t, err)
	require.Equal(t, uint64(1), retainedTerm)

	entries, err := w.Entries(firstIndex, 6, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, []uint64{3, 4, 5}, []uint64{
		entries[0].GetIndex(),
		entries[1].GetIndex(),
		entries[2].GetIndex(),
	})

	_, err = w.Term(firstIndex - 2)
	require.ErrorIs(t, err, raft.ErrCompacted)
	_, err = w.Entries(firstIndex-1, 6, math.MaxUint64)
	require.ErrorIs(t, err, raft.ErrCompacted)
}

func TestRaftFollowerWithinRetainedMarginReceivesAppend(t *testing.T) {
	t.Parallel()

	w := newWALWithRetainedMargin(t)
	assertFollowerWithinRetainedMarginReceivesAppend(t, w)
}

func TestRetainedMarginSurvivesRestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newWALWithRetainedMarginAt(t, dir)
	require.NoError(t, w.Close())

	reopened := newTestWALAt(t, dir)
	firstIndex, err := reopened.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), firstIndex)

	boundaryTerm, err := reopened.Term(2)
	require.NoError(t, err)
	require.Equal(t, uint64(1), boundaryTerm)

	entries, err := reopened.Entries(3, 6, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 4, 5}, []uint64{
		entries[0].GetIndex(),
		entries[1].GetIndex(),
		entries[2].GetIndex(),
	})

	assertFollowerWithinRetainedMarginReceivesAppend(t, reopened)
}

func assertFollowerWithinRetainedMarginReceivesAppend(t *testing.T, w *DefaultWAL) {
	t.Helper()

	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         w,
		Applied:         5,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
	})
	require.NoError(t, err)

	persistReady := func() raft.Ready {
		t.Helper()
		require.True(t, rawNode.HasReady())
		ready := rawNode.Ready()
		require.NoError(t, w.Append(ready.HardState, ready.Entries))
		rawNode.Advance(ready)

		return ready
	}

	require.NoError(t, rawNode.Campaign())
	persistReady()
	require.NoError(t, rawNode.Step(&raftpb.Message{
		Type: new(raftpb.MsgVoteResp),
		From: new(uint64(2)),
		To:   new(uint64(1)),
		Term: new(uint64(2)),
	}))
	persistReady()

	// The leader initially probes at index 5. The follower reports that its
	// last matching entry is index 3, which is still inside the retained WAL
	// window even though the latest snapshot is at index 5.
	require.NoError(t, rawNode.Step(&raftpb.Message{
		Type:       new(raftpb.MsgAppResp),
		From:       new(uint64(2)),
		To:         new(uint64(1)),
		Term:       new(uint64(2)),
		Index:      new(uint64(5)),
		Reject:     new(true),
		RejectHint: new(uint64(3)),
		LogTerm:    new(uint64(1)),
	}))
	require.True(t, rawNode.HasReady())
	ready := rawNode.Ready()

	var appendMessage *raftpb.Message
	for _, message := range ready.Messages {
		require.NotEqual(t, raftpb.MsgSnap, message.GetType(),
			"a follower inside the retained margin must not receive a snapshot")
		if message.GetType() == raftpb.MsgApp && message.GetTo() == 2 {
			appendMessage = message
		}
	}
	require.NotNil(t, appendMessage, "the follower must catch up through MsgApp")
	require.Equal(t, uint64(3), appendMessage.GetIndex())
	require.Equal(t, uint64(1), appendMessage.GetLogTerm())
	require.Len(t, appendMessage.GetEntries(), 3)
	require.Equal(t, []uint64{4, 5, 6}, []uint64{
		appendMessage.GetEntries()[0].GetIndex(),
		appendMessage.GetEntries()[1].GetIndex(),
		appendMessage.GetEntries()[2].GetIndex(),
	})
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

	require.NoError(t, w.Append(hs(1, 1, 3), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
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
	snap := &raftpb.Snapshot{
		Metadata: snapshotMeta(10, 2, cs),
		Data:     []byte("snapshot data"),
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

	require.NoError(t, w.Append(hs(1, 1, 3), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
	}))

	idx, err := w.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), idx)
}

func TestFirstIndex_AfterCompaction(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(hs(1, 1, 5), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
		ent(4, 1, []byte("d")),
		ent(5, 1, []byte("e")),
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(3, cs, nil))
	require.NoError(t, w.Compact(3))

	idx, err := w.FirstIndex()
	require.NoError(t, err)
	// Index 3 is retained only as the matching term at FirstIndex()-1.
	require.Equal(t, uint64(4), idx)
}

// --- Snapshot tests ---

func TestSnapshot_Empty(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	snap, err := w.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(0), snap.GetMetadata().GetIndex())
	require.Equal(t, uint64(0), snap.GetMetadata().GetTerm())
}

func TestSnapshot_AfterCreateSnapshot(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(hs(2, 1, 3), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 2, []byte("b")),
		ent(3, 2, []byte("c")),
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1, 2}}
	require.NoError(t, w.CreateSnapshot(3, cs, []byte("snap-data")))

	snap, err := w.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(3), snap.GetMetadata().GetIndex())
	require.Equal(t, uint64(2), snap.GetMetadata().GetTerm())
	require.Equal(t, []byte("snap-data"), snap.GetData())
	require.Equal(t, cs.GetVoters(), snap.GetMetadata().GetConfState().GetVoters())
}

func TestCreateSnapshot_CompactionMarkerSaveFailure(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)
	require.NoError(t, w.wal.Close())

	err := w.CreateSnapshot(0, &raftpb.ConfState{Voters: []uint64{1}}, nil)
	require.ErrorContains(t, err, "saving snapshot compaction marker")
}

func TestUpdateSnapshotConfStateSerializesSnapshotPersistence(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)
	require.NoError(t, w.Append(hs(1, 1, 1), []*raftpb.Entry{
		ent(1, 1, []byte("entry")),
	}))
	require.NoError(t, w.CreateSnapshot(
		1,
		&raftpb.ConfState{Voters: []uint64{1}},
		make([]byte, 4<<20),
	))

	const writers = 16
	ready := sync.WaitGroup{}
	ready.Add(writers)
	start := make(chan struct{})
	errs := make(chan error, writers)

	for i := range writers {
		go func() {
			ready.Done()
			<-start
			errs <- w.UpdateSnapshotConfState(&raftpb.ConfState{
				Voters:   []uint64{1},
				Learners: []uint64{uint64(i + 2)},
			})
		}()
	}

	ready.Wait()
	close(start)

	for range writers {
		require.NoError(t, <-errs)
	}
}

// --- ApplySnapshot tests ---

func TestApplySnapshot(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Append some entries first
	require.NoError(t, w.Append(hs(1, 1, 2), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
	}))

	snap := &raftpb.Snapshot{
		Metadata: snapshotMeta(10, 5, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}),
		Data:     []byte("full-snapshot"),
	}
	require.NoError(t, w.ApplySnapshot(snap))

	// Verify snapshot is applied
	gotSnap, err := w.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(10), gotSnap.GetMetadata().GetIndex())
	require.Equal(t, uint64(5), gotSnap.GetMetadata().GetTerm())
	require.Equal(t, []byte("full-snapshot"), gotSnap.GetData())

	// Entries should be cleared after applying snapshot
	idx, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(10), idx, "last index should be snapshot index after apply")
}

func TestAppliedSnapshotCompactionBoundarySurvivesRestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALAt(t, dir)
	require.NoError(t, w.Append(hs(1, 1, 2), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
	}))
	require.NoError(t, w.ApplySnapshot(&raftpb.Snapshot{
		Metadata: snapshotMeta(10, 5, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}),
		Data:     []byte("full-snapshot"),
	}))
	require.NoError(t, w.Close())

	reopened := newTestWALAt(t, dir)
	firstIndex, err := reopened.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(11), firstIndex)

	boundaryTerm, err := reopened.Term(10)
	require.NoError(t, err)
	require.Equal(t, uint64(5), boundaryTerm)

	_, err = reopened.Term(2)
	require.ErrorIs(t, err, raft.ErrCompacted)
	lastIndex, err := reopened.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(10), lastIndex)
}

func TestApplySnapshotUnlocksOnSnapshotSaveFailure(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)
	w.snapshotter.dir = filepath.Join(t.TempDir(), "missing", "snap")

	err := w.ApplySnapshot(&raftpb.Snapshot{
		Metadata: snapshotMeta(10, 2, &raftpb.ConfState{Voters: []uint64{1}}),
	})
	require.ErrorContains(t, err, "saving snapshot file")
	require.True(t, w.mu.TryLock(), "ApplySnapshot must unlock after a persistence failure")
	w.mu.Unlock()
}

// --- Append edge cases ---

func TestAppend_NoChangeNoOp(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Appending same hard state with no entries should be a no-op
	err := w.Append(&raftpb.HardState{}, nil)
	require.NoError(t, err)
}

func TestAppend_HardStateOnly(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	hs := hs(5, 2, 0)
	require.NoError(t, w.Append(hs, nil))

	gotHS, _, err := w.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(5), gotHS.GetTerm())
	require.Equal(t, uint64(2), gotHS.GetVote())
}

func TestAppend_MultipleAppends(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// First batch
	require.NoError(t, w.Append(hs(1, 1, 2), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
	}))

	// Second batch
	require.NoError(t, w.Append(hs(1, 1, 4), []*raftpb.Entry{
		ent(3, 1, []byte("c")),
		ent(4, 1, []byte("d")),
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
	require.NoError(t, w.Append(hs(1, 1, 3), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
	}))

	// Overwrite from index 2 with a different term (simulates leader change)
	require.NoError(t, w.Append(hs(2, 2, 2), []*raftpb.Entry{
		ent(2, 2, []byte("b-new")),
	}))

	idx, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), idx)

	ents, err := w.Entries(1, 3, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, ents, 2)
	require.Equal(t, uint64(1), ents[0].GetTerm())
	require.Equal(t, uint64(2), ents[1].GetTerm())
	require.Equal(t, []byte("b-new"), ents[1].GetData())
}

func TestAppend_GapCreatesAppend(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Append entries at index 1-2
	require.NoError(t, w.Append(hs(1, 1, 2), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
	}))

	// Append entries at index 5 (gap from 3-4 -- this shouldn't happen in valid Raft
	// but tests the code path where entries[0].Index > offset+len(s.entries))
	require.NoError(t, w.Append(hs(1, 1, 5), []*raftpb.Entry{
		ent(5, 1, []byte("e")),
	}))

	idx, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), idx)
}

// --- Compact tests ---

func TestCompact_Basic(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(hs(1, 1, 5), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
		ent(4, 1, []byte("d")),
		ent(5, 1, []byte("e")),
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(3, cs, nil))
	require.NoError(t, w.Compact(3))

	// After compaction, entries through index 3 should be unavailable. Index 3
	// remains only as the dummy matching term required by raft.Storage.
	_, err := w.Entries(1, 3, math.MaxUint64)
	require.ErrorIs(t, err, raft.ErrCompacted)
	_, err = w.Entries(3, 6, math.MaxUint64)
	require.ErrorIs(t, err, raft.ErrCompacted)

	// Entries after compaction point should still be available
	ents, err := w.Entries(4, 6, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, ents, 2)
}

func TestCompact_CompactionMarkerSaveFailureKeepsRetainedWindow(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)
	require.NoError(t, w.Append(hs(1, 1, 3), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
	}))
	require.NoError(t, w.CreateSnapshot(3, &raftpb.ConfState{Voters: []uint64{1}}, nil))
	require.NoError(t, w.wal.Close())

	err := w.Compact(2)
	require.ErrorContains(t, err, "saving compaction marker at index 2")
	firstIndex, firstIndexErr := w.FirstIndex()
	require.NoError(t, firstIndexErr)
	require.Equal(t, uint64(1), firstIndex, "failed persistence must leave the retained window unchanged")
}

func TestCompact_AfterSnapshot(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	// Attempt compact with index > snapshot.Metadata.Index should error
	require.NoError(t, w.Append(hs(1, 1, 3), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
	}))

	// Compact at index 5 without a snapshot at that index should fail
	err := w.Compact(5)
	require.Error(t, err)
}

func TestCompact_BeforeFirstIndex(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(hs(1, 1, 3), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
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
	snap := &raftpb.Snapshot{
		Metadata: snapshotMeta(10, 2, &raftpb.ConfState{Voters: []uint64{1}}),
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

	require.NoError(t, w.Append(hs(1, 1, 3), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
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
	require.Equal(t, uint64(0), snap.GetMetadata().GetIndex())
	require.Equal(t, uint64(0), snap.GetMetadata().GetTerm())
	require.Equal(t, []byte("initial"), snap.GetData())
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
	require.Equal(t, uint64(100), snap.GetMetadata().GetIndex())
	require.Equal(t, uint64(1), snap.GetMetadata().GetTerm(), "restore snapshot should use term 1")
}

func TestCreateSnapshot_OutOfDate(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(hs(1, 1, 3), []*raftpb.Entry{
		ent(1, 1, []byte("a")),
		ent(2, 1, []byte("b")),
		ent(3, 1, []byte("c")),
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(3, cs, nil))

	// Creating another snapshot at a lower or equal index should fail
	err := w.CreateSnapshot(2, cs, nil)
	require.ErrorIs(t, err, raft.ErrSnapOutOfDate)

	err = w.CreateSnapshot(3, cs, nil)
	require.ErrorIs(t, err, raft.ErrSnapOutOfDate)
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

	require.NoError(t, w.Append(hs(3, 1, 2), []*raftpb.Entry{
		ent(1, 1, []byte("first")),
		ent(2, 3, []byte("second")),
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
	require.Equal(t, uint64(3), hs.GetTerm())
	require.Equal(t, uint64(1), hs.GetVote())
	require.Equal(t, uint64(2), hs.GetCommit())
	require.Equal(t, []uint64{1, 2}, gotCS.GetVoters())

	// Verify snapshot persisted
	snap, err := w2.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(2), snap.GetMetadata().GetIndex())
	require.Equal(t, []byte("snap"), snap.GetData())

	// Snapshot publication alone does not compact the WAL. With no Compact call,
	// restart replays from the initial boundary and retains both entries.
	firstIdx, err := w2.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstIdx)
	entries, err := w2.Entries(1, 3, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, []uint64{
		entries[0].GetIndex(),
		entries[1].GetIndex(),
	})

	lastIdx, err := w2.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), lastIdx)
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

	require.NoError(t, w.Append(hs(1, 1, 5), []*raftpb.Entry{
		ent(1, 1, []byte("entry-1")),
		ent(2, 1, []byte("entry-2")),
		ent(3, 1, []byte("entry-3")),
		ent(4, 1, []byte("entry-4")),
		ent(5, 1, []byte("entry-5")),
	}))

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(3, cs, []byte("snapshot-data")))

	require.NoError(t, w.Append(hs(1, 1, 8), []*raftpb.Entry{
		ent(6, 1, []byte("entry-6")),
		ent(7, 1, []byte("entry-7")),
		ent(8, 1, []byte("entry-8")),
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
	require.Equal(t, uint64(3), snap.GetMetadata().GetIndex())
	require.Equal(t, []byte("snapshot-data"), snap.GetData())

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

// TestAppend_StaleEntriesBeforeCachedWindowDoesNotLeakLock pins the fix
// for #301. When Append receives entries that all fall before the cached
// window (last < offset), the in-memory-merge branch short-circuited with
// `return nil` while still holding s.mu. Every subsequent call to
// Append/Entries/Term/Snapshot — including the next one driven by the
// Raft Ready loop — would then block forever, freezing the node with no
// commits, no snapshot, no clean shutdown.
//
// We reproduce by:
//  1. Appending entries [3..5] so the cached window has offset=3.
//  2. Appending entries [1..2] (last=2 < offset=3) — the stale branch.
//  3. Running the next Append in a goroutine and asserting it returns
//     within a short timeout.
//
// Without the fix step 3 hangs and the test fails on the timeout.
func TestAppend_StaleEntriesBeforeCachedWindowDoesNotLeakLock(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t)

	require.NoError(t, w.Append(
		hs(1, 1, 5),
		[]*raftpb.Entry{
			ent(3, 1, []byte("c")),
			ent(4, 1, []byte("d")),
			ent(5, 1, []byte("e")),
		},
	))

	require.NoError(t, w.Append(
		hs(1, 1, 5),
		[]*raftpb.Entry{
			ent(1, 1, []byte("a")),
			ent(2, 1, []byte("b")),
		},
	))

	done := make(chan error, 1)
	go func() {
		done <- w.Append(
			hs(1, 1, 6),
			[]*raftpb.Entry{ent(6, 1, []byte("f"))},
		)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Append blocked after a stale prior Append — s.mu was not released (#301)")
	}

	got, err := w.Entries(3, 7, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, got, 4)
	require.Equal(t, uint64(3), got[0].GetIndex())
	require.Equal(t, uint64(6), got[3].GetIndex())
}

// TestAppend_StaleEntriesPiggybackedHardStateIsPersisted pins the second
// half of the #301 fix. The first iteration of this PR returned early
// from the stale-entry branch, which also bypassed HardState persistence.
// If a follower's Ready batch carries stale entries together with a new
// term/vote/commit on the same Append call (a common shape right after a
// snapshot install), the bumped HardState would be silently dropped and
// the node would forget the commit on the next restart.
//
// The fix drops the stale entries but falls through to the HardState
// update path. We assert that the new commit survives:
//  1. Prime the window with entries 3..5 and HardState commit=5.
//  2. Issue an Append carrying stale entries 1..2 AND HardState commit=10.
//  3. Reload the WAL from disk and assert InitialState reports commit=10.
func TestAppend_StaleEntriesPiggybackedHardStateIsPersisted(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWALDir(t, dir)

	// Seed a snapshot at index 2 so writing entries 3..5 is legal and
	// "stale" entries at index 1..2 fall before the cached window.
	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(2, cs, nil))

	require.NoError(t, w.Append(
		hs(1, 1, 5),
		[]*raftpb.Entry{
			ent(3, 1, []byte("c")),
			ent(4, 1, []byte("d")),
			ent(5, 1, []byte("e")),
		},
	))

	// Stale entries (1..2) + a piggy-backed Commit=10. The stale branch
	// must drop the entries but still flush the new HardState.
	require.NoError(t, w.Append(
		hs(1, 1, 10),
		[]*raftpb.Entry{
			ent(1, 1, []byte("a")),
			ent(2, 1, []byte("b")),
		},
	))

	require.NoError(t, w.Close())

	// Reopen and verify the persisted HardState carries the bumped commit.
	reopened := newTestWALDir(t, dir)

	hs, _, err := reopened.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(10), hs.GetCommit(),
		"piggy-backed HardState commit on a stale-entry Append must survive across restart (#301)")
}

// newTestWALDir is a variant of newTestWAL that lets the caller control
// the storage directory so a WAL can be closed and reopened in-place.
func newTestWALDir(t *testing.T, dir string) *DefaultWAL {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	w, err := New(dir, logger, meter)
	require.NoError(t, err)

	return w
}
