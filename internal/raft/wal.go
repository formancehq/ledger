package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
)

const (
	walCreationCompletedFile = "WAL_CREATION_COMPLETED"
	etcdWalDir               = "etcd"
	stateFile                = "raft-state.pb"
)

// WAL implements raft.Storage interface for etcd/raft using etcd/wal
type WAL struct {
	mu sync.RWMutex

	// HardState contains the current term and commit index
	hardState raftpb.HardState

	// Snapshot stores the most recent snapshot
	snapshot raftpb.Snapshot

	// WAL for storing log entries
	wal *wal.WAL

	// In-memory cache of entries (for fast access)
	// This is rebuilt from WAL on startup
	entries []raftpb.Entry

	logger     logging.Logger
	dataDir    string
	stateFile  string
	etcdWalDir string
}

// NewWAL creates a new WAL instance
func NewWAL(dataDir string, logger logging.Logger) (*WAL, error) {

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}

	s := &WAL{
		entries:    make([]raftpb.Entry, 0),
		logger:     logger,
		dataDir:    dataDir,
		stateFile:  filepath.Join(dataDir, stateFile),
		etcdWalDir: filepath.Join(dataDir, etcdWalDir),
	}

	zapLogger, err := zap.NewDevelopment()
	if err != nil {
		return nil, fmt.Errorf("creating zap logger: %w", err)
	}

	markerFilePath := filepath.Join(s.dataDir, walCreationCompletedFile)

	_, err = os.Stat(markerFilePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("checking WAL creation completion marker: %w", err)
	}
	if err == nil {
		s.logger.Infof("WAL creation completed, opening existing WAL")
		data, err := os.ReadFile(s.stateFile)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}

		var snap walpb.Snapshot
		if err == nil {
			if err := unmarshalStateFile(data, &s.snapshot); err != nil {
				return nil, err
			}

			snap = walpb.Snapshot{
				Index: s.snapshot.Metadata.Index,
				Term:  s.snapshot.Metadata.Term,
			}
		}

		s.wal, err = wal.Open(zapLogger, s.etcdWalDir, snap)
		if err != nil {
			return nil, fmt.Errorf("opening existing WAL: %w", err)
		}

	} else {
		s.logger.Infof("WAL creation not completed, creating new WAL")
		if err := os.RemoveAll(s.etcdWalDir); err != nil {
			return nil, fmt.Errorf("removing existing WAL directory: %w", err)
		}

		w, err := wal.Create(zapLogger, s.etcdWalDir, nil)
		if err != nil {
			return nil, fmt.Errorf("creating new WAL: %w", err)
		}

		// Close the WAL created by wal.Create() and reopen it with wal.Open()
		// This is necessary because wal.Create() returns a WAL in write mode,
		// and ReadAll() requires a WAL opened with wal.Open()
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("closing newly created WAL: %w", err)
		}

		f, err := os.Create(markerFilePath)
		if err != nil {
			return nil, fmt.Errorf("creating WAL creation completion marker: %w", err)
		}

		if err := f.Sync(); err != nil {
			return nil, fmt.Errorf("syncing WAL creation completion marker: %w", err)
		}

		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("closing WAL creation completion marker: %w", err)
		}

		s.wal, err = wal.Open(zapLogger, s.etcdWalDir, walpb.Snapshot{})
		if err != nil {
			return nil, fmt.Errorf("opening newly created WAL: %w", err)
		}
	}

	_, s.hardState, s.entries, err = s.wal.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("reading WAL entries: %w", err)
	}

	s.logger.WithFields(map[string]any{"entries": len(s.entries)}).Infof("WAL replay completed")

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
func (s *WAL) saveSnapshot() error {
	// Marshal Snapshot
	snapshotData, err := s.snapshot.Marshal()
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
func (s *WAL) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	// todo: is the store accessed sequentially?
	// if yes, we can avoid locking
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.hardState, s.snapshot.Metadata.ConfState, nil
}

// Entries returns a slice of log entries in the range [lo, hi)
func (s *WAL) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lo >= hi {
		return nil, fmt.Errorf("invalid range: lo=%d, hi=%d", lo, hi)
	}

	firstIndex, err := s.firstIndexLocked()
	if err != nil {
		return nil, err
	}
	lastIndex, err := s.lastIndexLocked()
	if err != nil {
		return nil, err
	}

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
func (s *WAL) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.termLocked(i)
}

// LastIndex returns the index of the last entry in the log
func (s *WAL) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lastIndexLocked()
}

// FirstIndex returns the index of the first log entry
func (s *WAL) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.firstIndexLocked()
}

// Snapshot returns the most recent snapshot
func (s *WAL) Snapshot() (raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.snapshot, nil
}

// Append appends entries to the log
func (s *WAL) Append(hardState raftpb.HardState, entries []raftpb.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

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

	if !raft.IsEmptyHardState(hardState) {
		s.hardState = hardState
	}

	if raft.MustSync(hardState, s.hardState, len(entries)) {
		return s.wal.Save(s.hardState, entries)
	}

	return nil
}

// CreateSnapshot creates a snapshot at the given index
func (s *WAL) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Allow creating snapshot at index 0 if storage is empty (for initial cluster setup)
	// Otherwise, prevent creating snapshot at same or lower index
	isEmptyInitial := i == 0 &&
		s.snapshot.Metadata.Index == 0 &&
		len(s.snapshot.Metadata.ConfState.Voters) == 0 &&
		len(s.entries) == 0
	if !isEmptyInitial && i <= s.snapshot.Metadata.Index {
		return raft.ErrSnapOutOfDate
	}

	// Get term directly without taking another lock
	// For initial snapshot (index 0 on empty storage), use term 0
	var term uint64
	var err error
	if s.snapshot.Metadata.Index == 0 && len(s.entries) == 0 && i == 0 {
		// Initial snapshot at index 0 - use term 0
		term = 0
	} else {
		term, err = s.termLocked(i)
		if err != nil {
			return err
		}
	}

	s.snapshot = raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     i,
			Term:      term,
			ConfState: *cs,
		},
		Data: data,
	}

	return s.saveSnapshot()
}

// termLocked returns the term of entry i without taking a lock (assumes lock is already held)
func (s *WAL) termLocked(i uint64) (uint64, error) {
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
func (s *WAL) ApplySnapshot(snap raftpb.Snapshot) error {
	s.mu.Lock()
	s.snapshot = snap
	s.entries = nil // Clear entries after applying snapshot
	s.mu.Unlock()

	// Save to disk (outside of lock to avoid blocking)
	if err := s.saveSnapshot(); err != nil {
		s.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to save snapshot to disk")
	}

	return nil
}

// Compact compacts the log up to the given index
func (s *WAL) Compact(compactIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if compactIndex > s.snapshot.Metadata.Index {
		return fmt.Errorf(
			"index (%d) after last snapshot index(%d): %w",
			compactIndex,
			s.snapshot.Metadata.Index,
			raft.ErrCompacted,
		)
	}

	firstIndex, err := s.firstIndexLocked()
	if err != nil {
		return err
	}
	if compactIndex < firstIndex {
		return fmt.Errorf("index before first index: %w", raft.ErrCompacted)
	}

	if len(s.entries) == 0 {
		return nil
	}

	// Truncate entries before compactIndex
	truncateIndex := compactIndex - firstIndex
	if truncateIndex < uint64(len(s.entries)) {
		s.entries = s.entries[truncateIndex:]
	} else {
		s.entries = s.entries[:0]
	}

	// Note: WAL compaction is handled by etcd/wal itself when we create snapshots
	// We just update our in-memory cache here

	return nil
}

// Close closes the WAL
func (s *WAL) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			return fmt.Errorf("closing WAL: %w", err)
		}
		s.wal = nil
	}
	return nil
}

func (s *WAL) firstIndexLocked() (uint64, error) {
	if len(s.entries) == 0 {
		return s.snapshot.Metadata.Index + 1, nil
	}

	return s.entries[0].Index, nil
}

func (s *WAL) lastIndexLocked() (uint64, error) {
	if len(s.entries) == 0 {
		return s.snapshot.Metadata.Index, nil
	}

	return s.entries[len(s.entries)-1].Index, nil
}
