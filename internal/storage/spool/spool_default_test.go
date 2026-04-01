package spool

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func newTestSpool(t *testing.T) *Default {
	t.Helper()

	dir := t.TempDir()
	s, err := NewDefault(DefaultSpoolConfig{
		Dir:             dir,
		SegmentMaxBytes: 4096,
		WriteBufBytes:   512,
		SyncEvery:       2,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

func makeEntry(index uint64) raftpb.Entry {
	return raftpb.Entry{
		Index: index,
		Term:  1,
		Data:  []byte("test-data"),
	}
}

func collectEntries(t *testing.T, s *Default, end *Position, lastApplied uint64) []raftpb.Entry {
	t.Helper()

	var entries []raftpb.Entry

	err := s.ReplayUntil(context.Background(), *end, lastApplied, func(e raftpb.Entry) error {
		entries = append(entries, e)

		return nil
	})
	require.NoError(t, err)

	return entries
}

func TestDefaultSpoolFullCycle(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	// Append entries
	require.NoError(t, s.AppendCommittedEntries(context.Background(),
		makeEntry(1), makeEntry(2), makeEntry(3),
	))

	// Get end position
	end, err := s.End()
	require.NoError(t, err)

	// Replay all (lastApplied=0 means all entries are new)
	entries := collectEntries(t, s, end, 0)
	require.Len(t, entries, 3)
	require.Equal(t, uint64(1), entries[0].Index)
	require.Equal(t, uint64(2), entries[1].Index)
	require.Equal(t, uint64(3), entries[2].Index)
}

func TestDefaultSpoolReplayFiltersApplied(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	require.NoError(t, s.AppendCommittedEntries(context.Background(),
		makeEntry(1), makeEntry(2), makeEntry(3), makeEntry(4),
	))

	end, err := s.End()
	require.NoError(t, err)

	// Only entries with Index > 2 should be applied
	entries := collectEntries(t, s, end, 2)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(3), entries[0].Index)
	require.Equal(t, uint64(4), entries[1].Index)
}

func TestDefaultSpoolReplayUntilEmptySpool(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	end, err := s.End()
	require.NoError(t, err)

	entries := collectEntries(t, s, end, 0)
	require.Empty(t, entries)
}

func TestDefaultSpoolResetReadCache(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	require.NoError(t, s.AppendCommittedEntries(context.Background(),
		makeEntry(1), makeEntry(2), makeEntry(3),
	))

	end, err := s.End()
	require.NoError(t, err)

	// First replay: consume all entries
	entries := collectEntries(t, s, end, 0)
	require.Len(t, entries, 3)

	// Second replay without reset: cache is advanced, nothing new
	entries = collectEntries(t, s, end, 0)
	require.Empty(t, entries)

	// Reset read cache
	require.NoError(t, s.ResetReadCache())

	// Third replay: should see all entries again
	entries = collectEntries(t, s, end, 0)
	require.Len(t, entries, 3)
}

func TestDefaultSpoolResetReadCacheEmptySpool(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	require.NoError(t, s.ResetReadCache())

	end, err := s.End()
	require.NoError(t, err)

	entries := collectEntries(t, s, end, 0)
	require.Empty(t, entries)
}

func TestDefaultSpoolAdvanceReadCacheBetweenReplays(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	require.NoError(t, s.AppendCommittedEntries(context.Background(),
		makeEntry(1), makeEntry(2),
	))

	end1, err := s.End()
	require.NoError(t, err)

	// Replay up to end1
	entries := collectEntries(t, s, end1, 0)
	require.Len(t, entries, 2)

	// Append more entries
	require.NoError(t, s.AppendCommittedEntries(context.Background(),
		makeEntry(3), makeEntry(4),
	))

	end2, err := s.End()
	require.NoError(t, err)

	// Replay from where we left off
	entries = collectEntries(t, s, end2, 0)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(3), entries[0].Index)
	require.Equal(t, uint64(4), entries[1].Index)
}

func TestDefaultSpoolRotation(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	// Write enough entries to trigger at least one segment rotation.
	// Each entry is about 30+ bytes, SegmentMaxBytes is 4096,
	// so ~100 entries should be well beyond one segment.
	for i := uint64(1); i <= 200; i++ {
		require.NoError(t, s.AppendCommittedEntries(context.Background(), makeEntry(i)))
	}

	end, err := s.End()
	require.NoError(t, err)

	// Verify rotation happened (segID should be > 1)
	require.Greater(t, end.SegID, uint64(1), "should have rotated to a new segment")

	// Replay should still work across segment boundaries
	require.NoError(t, s.ResetReadCache())

	entries := collectEntries(t, s, end, 0)
	require.Len(t, entries, 200)
	require.Equal(t, uint64(1), entries[0].Index)
	require.Equal(t, uint64(200), entries[199].Index)
}

func TestDefaultSpoolPruneRemovesOldSegments(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	// Write entries across multiple segments
	for i := uint64(1); i <= 200; i++ {
		require.NoError(t, s.AppendCommittedEntries(context.Background(), makeEntry(i)))
	}

	// Close and reopen to write trailers (needed for Prune to read maxIndex)
	require.NoError(t, s.Close())

	s2, err := NewDefault(s.cfg)
	require.NoError(t, err)

	// Count segments before prune
	idsBefore, err := listSegments(s2.cfg.Dir)
	require.NoError(t, err)
	require.Greater(t, len(idsBefore), 1, "should have multiple segments")

	// Prune all entries up to 150
	require.NoError(t, s2.Prune(150))

	// Count segments after prune
	idsAfter, err := listSegments(s2.cfg.Dir)
	require.NoError(t, err)
	require.Less(t, len(idsAfter), len(idsBefore), "prune should remove at least one segment")

	// Remaining entries should still be replayable
	end, err := s2.End()
	require.NoError(t, err)

	var remaining []raftpb.Entry
	require.NoError(t, s2.ReplayUntil(context.Background(), *end, 150, func(e raftpb.Entry) error {
		remaining = append(remaining, e)

		return nil
	}))

	// All remaining entries should have Index > 150
	for _, e := range remaining {
		require.Greater(t, e.Index, uint64(150))
	}

	require.NoError(t, s2.Close())
}

func TestDefaultSpoolPruneNoOp(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	// Prune on empty spool should not error
	require.NoError(t, s.Prune(100))
}

func TestDefaultSpoolReplayRewindResetsCache(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	require.NoError(t, s.AppendCommittedEntries(context.Background(),
		makeEntry(1), makeEntry(2), makeEntry(3),
	))

	end, err := s.End()
	require.NoError(t, err)

	// First replay with lastApplied=1 (only entries > 1 applied)
	entries := collectEntries(t, s, end, 1)
	require.Len(t, entries, 2)

	// Rewind: replay with lower lastApplied (0 < 1) — should reset cache
	entries = collectEntries(t, s, end, 0)
	require.Len(t, entries, 3, "rewind should replay from the beginning")
}

func TestDefaultSpoolReplayContextCancellation(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	require.NoError(t, s.AppendCommittedEntries(context.Background(),
		makeEntry(1), makeEntry(2), makeEntry(3),
	))

	end, err := s.End()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err = s.ReplayUntil(ctx, *end, 0, func(_ raftpb.Entry) error {
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestDefaultSpoolAppendContextCancellation(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.AppendCommittedEntries(ctx, makeEntry(1))
	require.ErrorIs(t, err, context.Canceled)
}

func TestDefaultSpoolReplayApplyError(t *testing.T) {
	t.Parallel()

	s := newTestSpool(t)

	require.NoError(t, s.AppendCommittedEntries(context.Background(),
		makeEntry(1), makeEntry(2),
	))

	end, err := s.End()
	require.NoError(t, err)

	applyErr := errors.New("apply failed")
	err = s.ReplayUntil(context.Background(), *end, 0, func(_ raftpb.Entry) error {
		return applyErr
	})
	require.ErrorIs(t, err, applyErr)
}

func TestDefaultSpoolCloseAndReopen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := DefaultSpoolConfig{
		Dir:             dir,
		SegmentMaxBytes: 4096,
		WriteBufBytes:   512,
		SyncEvery:       2,
	}

	s1, err := NewDefault(cfg)
	require.NoError(t, err)

	require.NoError(t, s1.AppendCommittedEntries(context.Background(),
		makeEntry(1), makeEntry(2), makeEntry(3),
	))
	require.NoError(t, s1.Close())

	// Reopen and append more
	s2, err := NewDefault(cfg)
	require.NoError(t, err)

	require.NoError(t, s2.AppendCommittedEntries(context.Background(),
		makeEntry(4), makeEntry(5),
	))

	end, err := s2.End()
	require.NoError(t, err)

	entries := collectEntries(t, s2, end, 0)
	require.Len(t, entries, 5)
	require.Equal(t, uint64(1), entries[0].Index)
	require.Equal(t, uint64(5), entries[4].Index)

	require.NoError(t, s2.Close())
}
