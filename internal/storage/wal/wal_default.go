package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

const (
	walCreationCompletedFile = "WAL_CREATION_COMPLETED"
	etcdWalDir               = "etcd"
	snapDir                  = "snap"

	defaultPurgeInterval = 30 * time.Second
)

// Option configures a DefaultWAL instance.
type Option func(*DefaultWAL)

// WithPurgeInterval sets the interval at which the background purger checks
// for old WAL segment files to delete. Defaults to 30s.
func WithPurgeInterval(d time.Duration) Option {
	return func(w *DefaultWAL) {
		w.purgeInterval = d
	}
}

// DefaultWAL implements raft.Storage interface for etcd/raft using etcd/wal.
type DefaultWAL struct {
	// mu protects all mutable state (entries, snapshot, hardState)
	mu sync.RWMutex

	// HardState contains the current term and commit index
	hardState raftpb.HardState

	// Snapshot stores the most recent snapshot
	snapshot raftpb.Snapshot

	// DefaultWAL for storing log entries
	wal *wal.WAL

	// In-memory cache of entries (for fast access)
	// This is rebuilt from DefaultWAL on startup
	entries []raftpb.Entry

	logger      logging.Logger
	meter       metric.Meter
	dataDir     string
	snapshotter *Snapshotter
	etcdWalDir  string

	// Purger for old WAL segment files
	stopPurge     chan struct{}
	purgeDone     <-chan struct{}
	purgeInterval time.Duration

	// Zap logger for etcd WAL and purger
	zapLogger *zap.Logger

	// Metrics
	appendSaveHistogram      metric.Int64Histogram
	appendBatchSizeHistogram metric.Int64Histogram
}

// New creates a new DefaultWAL instance.
func New(dataDir string, logger logging.Logger, meter metric.Meter, opts ...Option) (*DefaultWAL, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}

	logger = logger.WithFields(map[string]any{"cmp": "wal"})

	snapshotter, err := NewSnapshotter(filepath.Join(dataDir, snapDir), logger)
	if err != nil {
		return nil, err
	}

	s := &DefaultWAL{
		entries:       make([]raftpb.Entry, 0),
		logger:        logger,
		meter:         meter,
		dataDir:       dataDir,
		snapshotter:   snapshotter,
		etcdWalDir:    filepath.Join(dataDir, etcdWalDir),
		purgeInterval: defaultPurgeInterval,
	}

	for _, opt := range opts {
		opt(s)
	}

	// Create metrics
	s.appendSaveHistogram, err = meter.Int64Histogram(
		"wal.append.save.duration",
		metric.WithDescription("Time spent saving entries to DefaultWAL"),
		metric.WithUnit("us"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating append save histogram: %w", err)
	}

	s.appendBatchSizeHistogram, err = meter.Int64Histogram(
		"wal.append.batch_size",
		metric.WithDescription("Number of entries appended at once"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating append batch size histogram: %w", err)
	}

	type zapProvider interface {
		Zap() *zap.Logger
	}
	if zp, ok := logger.(zapProvider); ok {
		s.zapLogger = zp.Zap()
	} else {
		s.zapLogger = zap.NewNop()
	}

	zapLogger := s.zapLogger

	markerFilePath := filepath.Join(s.dataDir, walCreationCompletedFile)

	_, err = os.Stat(markerFilePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("checking DefaultWAL creation completion marker: %w", err)
	}

	var snap walpb.Snapshot

	if err == nil {
		s.logger.Infof("WAL creation completed, opening existing DefaultWAL")

		// List valid snapshot records from the etcd WAL (source of truth).
		walSnaps, err := wal.ValidSnapshotEntries(zapLogger, s.etcdWalDir)
		if err != nil {
			return nil, fmt.Errorf("reading valid snapshot entries from WAL: %w", err)
		}

		// Load the newest snap file that matches a WAL snapshot record.
		// This ensures orphaned snap files (written before a crash, without
		// a corresponding WAL record) are not used.
		loadedSnap, err := s.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil {
			return nil, fmt.Errorf("loading snapshot matching WAL: %w", err)
		}

		if loadedSnap != nil {
			s.snapshot = *loadedSnap
			s.logger.
				WithFields(map[string]any{
					"index": s.snapshot.Metadata.Index,
					"term":  s.snapshot.Metadata.Term,
				}).
				Infof("Loaded snapshot from disk")
			snap = walpb.Snapshot{
				Index: s.snapshot.Metadata.Index,
				Term:  s.snapshot.Metadata.Term,
			}
		}

		s.wal, err = wal.Open(zapLogger, s.etcdWalDir, snap)
		if err != nil {
			return nil, fmt.Errorf("opening existing DefaultWAL: %w", err)
		}
	} else {
		s.logger.Infof("DefaultWAL creation not completed, creating new DefaultWAL")

		if err := os.RemoveAll(s.etcdWalDir); err != nil {
			return nil, fmt.Errorf("removing existing DefaultWAL directory: %w", err)
		}

		w, err := wal.Create(zapLogger, s.etcdWalDir, nil)
		if err != nil {
			return nil, fmt.Errorf("creating new DefaultWAL: %w", err)
		}

		// Close the DefaultWAL created by wal.Create() and reopen it with wal.Open()
		// This is necessary because wal.Create() returns a DefaultWAL in write mode,
		// and ReadAll() requires a DefaultWAL opened with wal.Open()
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("closing newly created DefaultWAL: %w", err)
		}

		f, err := os.Create(markerFilePath)
		if err != nil {
			return nil, fmt.Errorf("creating DefaultWAL creation completion marker: %w", err)
		}

		if err := f.Sync(); err != nil {
			return nil, fmt.Errorf("syncing DefaultWAL creation completion marker: %w", err)
		}

		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("closing DefaultWAL creation completion marker: %w", err)
		}

		s.wal, err = wal.Open(zapLogger, s.etcdWalDir, snap)
		if err != nil {
			return nil, fmt.Errorf("opening newly created DefaultWAL: %w", err)
		}
	}

	_, s.hardState, s.entries, err = s.wal.ReadAll()
	if err != nil {
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("reading DefaultWAL entries: %w", err)
		}

		// WAL has a partially written record from a crash (OOMKill, SIGKILL, power loss).
		// The incomplete entries were never committed by Raft, so truncating is safe.
		s.logger.Errorf("========================================")
		s.logger.Errorf("WAL CORRUPTED: unexpected EOF detected")
		s.logger.Errorf("Attempting automatic repair by truncating incomplete records...")
		s.logger.Errorf("========================================")

		closeErr := s.wal.Close()
		if closeErr != nil {
			s.logger.WithFields(map[string]any{"error": closeErr}).Errorf("Failed to close corrupted WAL before repair")
		}

		if !wal.Repair(zapLogger, s.etcdWalDir) {
			return nil, errors.New("WAL repair failed after unexpected EOF — manual intervention required")
		}

		s.logger.Errorf("WAL repair succeeded — re-opening WAL")

		s.wal, err = wal.Open(zapLogger, s.etcdWalDir, snap)
		if err != nil {
			return nil, fmt.Errorf("opening repaired WAL: %w", err)
		}

		_, s.hardState, s.entries, err = s.wal.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("reading repaired WAL entries: %w", err)
		}

		s.logger.Errorf("========================================")
		s.logger.Errorf("WAL recovery complete — %d entries recovered", len(s.entries))
		s.logger.Errorf("========================================")
	}

	s.logger.
		WithFields(map[string]any{
			"entries":          len(s.entries),
			"hardState.Term":   s.hardState.Term,
			"hardState.Commit": s.hardState.Commit,
			"snapshot.Index":   s.snapshot.Metadata.Index,
			"snapshot.Term":    s.snapshot.Metadata.Term,
		}).Infof("WAL replay completed")

	// Start background purger to delete old WAL segment files that have been
	// unlocked by ReleaseLockTo during compaction.
	s.stopPurge = make(chan struct{})
	// Use a nop logger for the purger to suppress the benign "failed to lock file"
	// warning that occurs when ReleaseLockTo keeps one extra segment locked.
	// The purger retries on the next cycle and eventually succeeds.
	s.purgeDone, _ = fileutil.PurgeFileWithDoneNotify(zap.NewNop(), s.etcdWalDir, ".wal", 1, s.purgeInterval, s.stopPurge)

	return s, nil
}

// InitialState returns the saved HardState and ConfState information.
func (s *DefaultWAL) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.hardState, s.snapshot.Metadata.ConfState, nil
}

// Entries returns a slice of log entries in the range [lo, hi).
func (s *DefaultWAL) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lo >= hi {
		return nil, fmt.Errorf("invalid range: lo=%d, hi=%d", lo, hi)
	}

	firstIndex := s.firstIndexLocked()
	lastIndex := s.lastIndexLocked()

	if lo < firstIndex {
		return nil, raft.ErrCompacted
	}

	if hi > lastIndex+1 {
		return nil, fmt.Errorf("entries[%d:%d) is out of bound [%d:%d]", lo, hi, firstIndex, lastIndex+1)
	}

	// Only contains dummy entries.
	if len(s.entries) == 0 {
		return nil, raft.ErrUnavailable
	}

	offset := s.entries[0].Index
	if lo < offset {
		return nil, raft.ErrCompacted
	}

	if hi > offset+uint64(len(s.entries)) {
		return nil, raft.ErrUnavailable
	}

	// Slice the entries
	ents := s.entries[lo-offset : hi-offset]

	// Limit size
	size := uint64(0)
	for i := range ents {
		size += uint64(ents[i].Size())
		if size > maxSize {
			ents = ents[:i+1]

			break
		}
	}

	return ents, nil
}

// Term returns the term of entry i.
func (s *DefaultWAL) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.termLocked(i)
}

// LastIndex returns the index of the last entry in the log.
func (s *DefaultWAL) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lastIndexLocked(), nil
}

// lastIndexLocked returns the last index without acquiring lock (caller must hold lock).
func (s *DefaultWAL) lastIndexLocked() uint64 {
	if len(s.entries) == 0 {
		return s.snapshot.Metadata.Index
	}

	return s.entries[len(s.entries)-1].Index
}

// FirstIndex returns the index of the first log entry.
func (s *DefaultWAL) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.firstIndexLocked(), nil
}

// firstIndexLocked returns the first index without acquiring lock (caller must hold lock).
func (s *DefaultWAL) firstIndexLocked() uint64 {
	if len(s.entries) == 0 {
		return s.snapshot.Metadata.Index + 1
	}

	return s.entries[0].Index
}

// Snapshot returns the most recent snapshot.
func (s *DefaultWAL) Snapshot() (raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.snapshot, nil
}

// Append appends entries to the log.
func (s *DefaultWAL) Append(hardState raftpb.HardState, entries []raftpb.Entry) error {
	s.mu.Lock()

	if hardState == s.hardState && len(entries) == 0 {
		s.mu.Unlock()

		return nil
	}

	var firstIdx, lastIdx uint64
	if len(entries) > 0 {
		firstIdx = entries[0].Index
		lastIdx = entries[len(entries)-1].Index
	}

	if s.logger.Enabled(logging.DebugLevel) {
		logger := s.logger.WithFields(map[string]any{
			"entries":          len(entries),
			"firstIndex":       firstIdx,
			"lastIndex":        lastIdx,
			"hardState.Term":   hardState.Term,
			"hardState.Vote":   hardState.Vote,
			"hardState.Commit": hardState.Commit,
			"prevCommit":       s.hardState.Commit,
			"cachedEntries":    len(s.entries),
		})
		logger.Debugf("WAL Append")
	}

	// Update in-memory cache
	if len(entries) > 0 {
		if len(s.entries) > 0 {
			offset := s.entries[0].Index

			last := entries[0].Index + uint64(len(entries)) - 1
			if last < offset {
				return nil
			}

			if entries[0].Index > offset+uint64(len(s.entries)) {
				s.entries = append(s.entries, entries...)
			} else {
				truncateIndex := entries[0].Index
				if truncateIndex > offset {
					s.entries = s.entries[:truncateIndex-offset]
				}

				s.entries = append(s.entries, entries...)
			}
		} else {
			s.entries = append(s.entries, entries...)
		}
	}

	newHardState := s.hardState
	if !raft.IsEmptyHardState(hardState) {
		s.hardState = hardState
		newHardState = hardState
	}
	s.mu.Unlock()

	// Save to DefaultWAL
	saveStart := time.Now()
	err := s.wal.Save(newHardState, entries)
	s.appendSaveHistogram.Record(context.Background(), time.Since(saveStart).Microseconds())
	s.appendBatchSizeHistogram.Record(context.Background(), int64(len(entries)))

	if err != nil {
		s.logger.WithFields(map[string]any{
			"error":            err,
			"hardState.Commit": newHardState.Commit,
		}).Errorf("WAL Save failed")
	}

	return err
}

// CreateSnapshot creates a snapshot at the given index.
func (s *DefaultWAL) CreateSnapshot(index uint64, cs *raftpb.ConfState, data []byte) error {
	s.mu.Lock()

	s.logger.WithFields(map[string]any{"index": index}).Infof("Creating snapshot")

	// Allow creating snapshot on empty storage (for initial cluster setup or restore).
	// Otherwise, prevent creating snapshot at same or lower index.
	isEmptyStorage := s.snapshot.Metadata.Index == 0 &&
		len(s.snapshot.Metadata.ConfState.Voters) == 0 &&
		len(s.entries) == 0
	if !isEmptyStorage && index <= s.snapshot.Metadata.Index {
		s.mu.Unlock()

		return raft.ErrSnapOutOfDate
	}

	// Get term directly without taking another lock
	// For initial snapshot (index 0 on empty storage), use term 0
	// For restore snapshot (empty storage, index > 0), use term 1
	var (
		term uint64
		err  error
	)

	if s.snapshot.Metadata.Index == 0 && len(s.entries) == 0 {
		if index == 0 {
			// Initial snapshot at index 0 - use term 0
			term = 0
		} else {
			// Restore snapshot: WAL is empty but we have restored data at a non-zero index.
			// Use term 1 to start a new Raft term for the restored cluster.
			term = 1
		}
	} else {
		term, err = s.termLocked(index)
		if err != nil {
			s.mu.Unlock()

			return err
		}
	}

	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     index,
			Term:      term,
			ConfState: *cs,
		},
		Data: data,
	}
	s.snapshot = snap
	s.mu.Unlock()

	if err := s.snapshotter.Save(snap); err != nil {
		return fmt.Errorf("saving snapshot file: %w", err)
	}

	// Write the WAL snapshot record BEFORE cleaning up old snap files.
	// If a crash occurs between Save and SaveSnapshot, the old snap file
	// is still on disk and LoadNewestAvailable will fall back to it.
	// Without this ordering, a crash would leave zero matching snap files,
	// causing the node to enter the fresh-start branch and lose cache state.
	if err := s.wal.SaveSnapshot(walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: cs,
	}); err != nil {
		return fmt.Errorf("saving snapshot record: %w", err)
	}

	// Safe to clean up old snap files now — the WAL record guarantees that
	// LoadNewestAvailable will match the new snap file on restart.
	s.snapshotter.CleanupOlderThan(snap.Metadata.Index)

	s.logger.WithFields(map[string]any{"index": index}).Infof("Snapshot created")

	return nil
}

// UpdateSnapshotConfState updates the ConfState of the latest snapshot without
// changing the snapshot data or index. This is used when cluster membership
// changes (e.g. a learner is added) so that etcd/raft sends snapshots with
// the correct ConfState to newly added nodes.
func (s *DefaultWAL) UpdateSnapshotConfState(cs *raftpb.ConfState) error {
	s.mu.Lock()

	// Nothing to update if there is no snapshot yet.
	if s.snapshot.Metadata.Index == 0 && len(s.snapshot.Metadata.ConfState.Voters) == 0 {
		s.mu.Unlock()

		return nil
	}

	snap := s.snapshot
	snap.Metadata.ConfState = *cs
	s.snapshot = snap
	s.mu.Unlock()

	err := s.snapshotter.Save(snap)
	if err != nil {
		return fmt.Errorf("saving snapshot: %w", err)
	}

	err = s.wal.SaveSnapshot(walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: cs,
	})
	if err != nil {
		return fmt.Errorf("saving snapshot: %w", err)
	}

	s.logger.WithFields(map[string]any{
		"index": snap.Metadata.Index,
	}).Infof("Snapshot ConfState updated")

	return nil
}

// termLocked returns the term of entry i without taking a lock (assumes lock is already held).
func (s *DefaultWAL) termLocked(i uint64) (uint64, error) {
	firstIndex := s.snapshot.Metadata.Index + 1

	var lastIndex uint64
	if len(s.entries) == 0 {
		lastIndex = s.snapshot.Metadata.Index
	} else {
		lastIndex = s.entries[len(s.entries)-1].Index
	}

	if i < firstIndex-1 {
		return 0, raft.ErrCompacted
	}

	if i > lastIndex {
		return 0, fmt.Errorf("term of index %d is out of bound", i)
	}

	if i == firstIndex-1 {
		return s.snapshot.Metadata.Term, nil
	}

	if len(s.entries) == 0 {
		return 0, raft.ErrUnavailable
	}

	offset := s.entries[0].Index
	if i < offset {
		return 0, raft.ErrCompacted
	}

	if i >= offset+uint64(len(s.entries)) {
		return 0, raft.ErrUnavailable
	}

	return s.entries[i-offset].Term, nil
}

// ApplySnapshot applies a snapshot to the storage.
func (s *DefaultWAL) ApplySnapshot(snap raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.WithFields(map[string]any{
		"snapIndex":      snap.Metadata.Index,
		"snapTerm":       snap.Metadata.Term,
		"prevSnapIndex":  s.snapshot.Metadata.Index,
		"prevHardCommit": s.hardState.Commit,
		"cachedEntries":  len(s.entries),
	}).Infof("WAL ApplySnapshot")

	s.snapshot = snap
	s.entries = nil // Clear entries after applying snapshot

	// Ensure the HardState commit index is at least as high as the snapshot.
	// A snapshot at index N implies all entries up to N are committed.
	if s.hardState.Commit < snap.Metadata.Index {
		s.hardState.Commit = snap.Metadata.Index
		s.hardState.Term = snap.Metadata.Term
	}

	// Write the snapshot record + updated HardState to the etcd WAL first.
	// This is the source of truth for crash safety: on restart, ReadAll()
	// returns a HardState with Commit >= snapshot.Index. The etcd WAL fsyncs
	// both records together in the Save call.
	//
	// Order matters: WAL before state file. If we crash between the two,
	// the state file has the old snapshot (stale Data) but the WAL has the
	// correct {Index, Term, HardState}. On restart the node will be marked
	// out-of-sync and re-fetch the checkpoint from the leader.
	// Save the full snapshot file first, then the WAL record + HardState.
	// Order follows etcd's pattern: an orphaned snap file is harmless (cleaned
	// up on the next snapshot), but a WAL snapshot record without a corresponding
	// snap file would cause issues at restart.
	if err := s.snapshotter.Save(snap); err != nil {
		return fmt.Errorf("saving snapshot file: %w", err)
	}

	walSnap := walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}
	if err := s.wal.SaveSnapshot(walSnap); err != nil {
		return fmt.Errorf("saving snapshot to WAL: %w", err)
	}

	if err := s.wal.Save(s.hardState, nil); err != nil {
		return fmt.Errorf("saving HardState after snapshot to WAL: %w", err)
	}

	// Safe to clean up old snap files now — the WAL record is persisted.
	s.snapshotter.CleanupOlderThan(snap.Metadata.Index)

	return nil
}

// Compact compacts the log up to the given index.
func (s *DefaultWAL) Compact(compactIndex uint64) error {
	s.mu.Lock()

	if compactIndex > s.snapshot.Metadata.Index {
		s.mu.Unlock()

		return fmt.Errorf(
			"index (%d) after last snapshot index(%d): %w",
			compactIndex,
			s.snapshot.Metadata.Index,
			raft.ErrCompacted,
		)
	}

	firstIndex := s.firstIndexLocked()
	if compactIndex < firstIndex {
		s.mu.Unlock()

		return fmt.Errorf("index before first index: %w", raft.ErrCompacted)
	}

	if len(s.entries) == 0 {
		s.mu.Unlock()

		return nil
	}

	// Truncate entries before compactIndex
	truncateIndex := compactIndex - firstIndex
	if truncateIndex < uint64(len(s.entries)) {
		// IMPORTANT: Create a new slice to release memory of old entries.
		// Simply re-slicing with s.entries[truncateIndex:] keeps a reference
		// to the original backing array, preventing GC from reclaiming memory.
		remaining := len(s.entries) - int(truncateIndex)
		newEntries := make([]raftpb.Entry, remaining)
		copy(newEntries, s.entries[truncateIndex:])
		s.entries = newEntries
	} else {
		// Set to nil instead of s.entries[:0] to release the backing array
		s.entries = nil
	}

	// Release s.mu before the I/O-bound ReleaseLockTo call.
	// The in-memory compaction is done; holding s.mu during file cleanup
	// would block Append (which also needs s.mu), stalling the Ready pipeline.
	s.mu.Unlock()

	// IMPORTANT: Release WAL file locks up to compactIndex.
	// This allows the etcd WAL to release memory associated with old log entries
	// and potentially remove old WAL segment files.
	// Without this call, the etcd WAL keeps file handles and memory indefinitely.
	err := s.wal.ReleaseLockTo(compactIndex)
	if err != nil {
		s.logger.WithFields(map[string]any{
			"compactIndex": compactIndex,
			"error":        err,
		}).Errorf("Failed to release WAL lock")
		// Don't return error - the in-memory compaction succeeded
	}

	return nil
}

// Close closes the DefaultWAL. Safe to call multiple times.
func (s *DefaultWAL) Close() error {
	select {
	case <-s.stopPurge:
		// Already closed
	default:
		close(s.stopPurge)
		<-s.purgeDone
	}

	return s.wal.Close()
}
