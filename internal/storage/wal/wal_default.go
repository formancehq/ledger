package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

const (
	walCreationCompletedFile = "WAL_CREATION_COMPLETED"
	etcdWalDir               = "etcd"
	stateFile                = "raft-state.pb"

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

// DefaultWAL implements raft.Storage interface for etcd/raft using etcd/wal
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

	logger     logging.Logger
	meter      metric.Meter
	dataDir    string
	stateFile  string
	etcdWalDir string

	// Purger for old WAL segment files
	stopPurge     chan struct{}
	purgeDone     <-chan struct{}
	purgeInterval time.Duration

	// Zap logger for etcd WAL and purger
	zapLogger *zap.Logger

	// Metrics
	appendCacheHistogram     metric.Int64Histogram
	appendSaveHistogram      metric.Int64Histogram
	appendBatchSizeHistogram metric.Int64Histogram
}

// New creates a new DefaultWAL instance
func New(dataDir string, logger logging.Logger, meter metric.Meter, opts ...Option) (*DefaultWAL, error) {

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}

	logger = logger.WithFields(map[string]any{"cmp": "wal"})

	s := &DefaultWAL{
		entries:       make([]raftpb.Entry, 0),
		logger:        logger,
		meter:         meter,
		dataDir:       dataDir,
		stateFile:     filepath.Join(dataDir, stateFile),
		etcdWalDir:    filepath.Join(dataDir, etcdWalDir),
		purgeInterval: defaultPurgeInterval,
	}

	for _, opt := range opts {
		opt(s)
	}

	// Create metrics
	var err error
	s.appendCacheHistogram, err = meter.Int64Histogram(
		"wal.append.cache.duration",
		metric.WithDescription("Time spent updating in-memory cache"),
		metric.WithUnit("us"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating append cache histogram: %w", err)
	}

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

	s.zapLogger, err = zap.NewDevelopment()
	if err != nil {
		return nil, fmt.Errorf("creating zap logger: %w", err)
	}
	zapLogger := s.zapLogger

	markerFilePath := filepath.Join(s.dataDir, walCreationCompletedFile)

	_, err = os.Stat(markerFilePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("checking DefaultWAL creation completion marker: %w", err)
	}
	if err == nil {
		s.logger.Infof("DefaultWAL creation completed, opening existing DefaultWAL")
		data, err := os.ReadFile(s.stateFile)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}

		var snap walpb.Snapshot
		if err == nil {
			if err := unmarshalStateFile(data, &s.snapshot); err != nil {
				return nil, err
			}
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

		s.wal, err = wal.Open(zapLogger, s.etcdWalDir, walpb.Snapshot{})
		if err != nil {
			return nil, fmt.Errorf("opening newly created DefaultWAL: %w", err)
		}
	}

	_, s.hardState, s.entries, err = s.wal.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("reading DefaultWAL entries: %w", err)
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

func unmarshalStateFile(data []byte, to *raftpb.Snapshot) error {
	if len(data) < 8 {
		return fmt.Errorf("state file too short")
	}

	// Format: [snapshotLength (8 bytes)][snapshotData]
	snapshotLen := binary.BigEndian.Uint64(data[0:8])
	if len(data) < int(8+snapshotLen) {
		return fmt.Errorf("state file truncated at snapshot")
	}

	return to.Unmarshal(data[8 : 8+snapshotLen])
}

// saveSnapshot saves snapshot to disk
// Format: [snapshotLength (8 bytes)][snapshotData]
func (s *DefaultWAL) saveSnapshot(snap raftpb.Snapshot) error {
	// Marshal Snapshot
	snapshotData, err := snap.Marshal()
	if err != nil {
		return fmt.Errorf("marshaling snapshot: %w", err)
	}

	// Create file with length-prefixed format
	// Format: [snapshotLength (8 bytes)][snapshotData]
	totalSize := 8 + len(snapshotData)
	fileData := make([]byte, totalSize)

	// Write snapshot length
	binary.BigEndian.PutUint64(fileData[0:8], uint64(len(snapshotData)))

	// Write snapshot data
	copy(fileData[8:8+len(snapshotData)], snapshotData)

	stateFile, err := os.Create(s.stateFile + ".tmp")
	if err != nil {
		return fmt.Errorf("creating state file: %w", err)
	}
	defer func() {
		_ = stateFile.Close()
	}()

	// Write file
	if _, err := stateFile.Write(fileData); err != nil {
		return fmt.Errorf("writing state file: %w", err)
	}

	if err := stateFile.Sync(); err != nil {
		return fmt.Errorf("syncing state file: %w", err)
	}

	if err := stateFile.Close(); err != nil {
		return fmt.Errorf("closing state file: %w", err)
	}

	if err := os.Rename(s.stateFile+".tmp", s.stateFile); err != nil {
		return fmt.Errorf("renaming state file: %w", err)
	}

	return nil
}

// InitialState returns the saved HardState and ConfState information
func (s *DefaultWAL) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hardState, s.snapshot.Metadata.ConfState, nil
}

// Entries returns a slice of log entries in the range [lo, hi)
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

// Term returns the term of entry i
func (s *DefaultWAL) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.termLocked(i)
}

// LastIndex returns the index of the last entry in the log
func (s *DefaultWAL) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndexLocked(), nil
}

// lastIndexLocked returns the last index without acquiring lock (caller must hold lock)
func (s *DefaultWAL) lastIndexLocked() uint64 {
	if len(s.entries) == 0 {
		return s.snapshot.Metadata.Index
	}
	return s.entries[len(s.entries)-1].Index
}

// FirstIndex returns the index of the first log entry
func (s *DefaultWAL) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.firstIndexLocked(), nil
}

// firstIndexLocked returns the first index without acquiring lock (caller must hold lock)
func (s *DefaultWAL) firstIndexLocked() uint64 {
	if len(s.entries) == 0 {
		return s.snapshot.Metadata.Index + 1
	}
	return s.entries[0].Index
}

// Snapshot returns the most recent snapshot
func (s *DefaultWAL) Snapshot() (raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot, nil
}

// Append appends entries to the log
func (s *DefaultWAL) Append(hardState raftpb.HardState, entries []raftpb.Entry) error {
	s.mu.Lock()

	if hardState == s.hardState && len(entries) == 0 {
		s.mu.Unlock()
		return nil
	}

	logger := s.logger.WithFields(map[string]any{
		"entries":          len(entries),
		"hardState.Term":   hardState.Term,
		"hardState.Vote":   hardState.Vote,
		"hardState.Commit": hardState.Commit,
	})
	logger.Debugf("Appending entries")

	// Update in-memory cache
	cacheStart := time.Now()
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
	s.appendCacheHistogram.Record(context.Background(), time.Since(cacheStart).Microseconds())

	s.logger.
		WithFields(map[string]any{
			"hardState.Term":   newHardState.Term,
			"hardState.Vote":   newHardState.Vote,
			"hardState.Commit": newHardState.Commit,
		}).
		Debug("Saving DefaultWAL entries to disk")

	// Save to DefaultWAL
	saveStart := time.Now()
	err := s.wal.Save(newHardState, entries)
	s.appendSaveHistogram.Record(context.Background(), time.Since(saveStart).Microseconds())
	s.appendBatchSizeHistogram.Record(context.Background(), int64(len(entries)))

	return err
}

// CreateSnapshot creates a snapshot at the given index
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
	var term uint64
	var err error
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

	if err := s.saveSnapshot(snap); err != nil {
		return fmt.Errorf("saving snapshot: %w", err)
	}

	if err := s.wal.SaveSnapshot(walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: cs,
	}); err != nil {
		return fmt.Errorf("saving snapshot: %w", err)
	}

	s.logger.WithFields(map[string]any{"index": index}).Infof("Snapshot created")

	return nil
}

// termLocked returns the term of entry i without taking a lock (assumes lock is already held)
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

// ApplySnapshot applies a snapshot to the storage
func (s *DefaultWAL) ApplySnapshot(snap raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshot = snap
	s.entries = nil // Clear entries after applying snapshot

	// Save to disk
	if err := s.saveSnapshot(snap); err != nil {
		s.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to save snapshot to disk")
	}

	return nil
}

// Compact compacts the log up to the given index
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
	if err := s.wal.ReleaseLockTo(compactIndex); err != nil {
		s.logger.WithFields(map[string]any{
			"compactIndex": compactIndex,
			"error":        err,
		}).Errorf("Failed to release WAL lock")
		// Don't return error - the in-memory compaction succeeded
	}

	return nil
}

// Close closes the DefaultWAL
func (s *DefaultWAL) Close() error {
	close(s.stopPurge)
	<-s.purgeDone
	return s.wal.Close()
}
