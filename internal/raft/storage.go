package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

var (
	ErrCompacted     = errors.New("requested index is unavailable due to compaction")
	ErrSnapOutOfDate = errors.New("snapshot is out of date")
	ErrUnavailable   = errors.New("requested entry at index is unavailable")
)

// Storage implements raft.Storage interface for etcd/raft
type Storage struct {
	mu sync.RWMutex

	// HardState contains the current term and commit index
	hardState raftpb.HardState

	// Entries stores the Raft log entries
	entries []raftpb.Entry

	// Snapshot stores the most recent snapshot
	snapshot raftpb.Snapshot

	logger        *zap.Logger
	dataDir       string
	hardStateFile string
	entriesFile   string
	snapshotFile  string
}

// NewStorage creates a new Storage instance
func NewStorage(dataDir string, logger *zap.Logger) (*Storage, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}

	s := &Storage{
		entries:       make([]raftpb.Entry, 0),
		logger:        logger,
		dataDir:       dataDir,
		hardStateFile: filepath.Join(dataDir, "raft-hardstate.json"),
		entriesFile:   filepath.Join(dataDir, "raft-entries.json"),
		snapshotFile:  filepath.Join(dataDir, "raft-snapshot.json"),
	}

	// Try to restore from disk
	if err := s.restore(); err != nil {
		logger.Warn("Failed to restore storage from disk, starting fresh", zap.Error(err))
	}

	return s, nil
}

// InitialState returns the saved HardState and ConfState information
func (s *Storage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.hardState, s.snapshot.Metadata.ConfState, nil
}

// IsEmpty checks if the storage is empty (no HardState, no Entries, no Snapshot)
func (s *Storage) IsEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if HardState is empty (term 0 and commit 0)
	if s.hardState.Term != 0 || s.hardState.Commit != 0 {
		return false
	}

	// Check if there are any entries
	if len(s.entries) > 0 {
		return false
	}

	// Check if there's a snapshot
	if s.snapshot.Metadata.Index != 0 {
		return false
	}

	return true
}

// Entries returns a slice of log entries in the range [lo, hi)
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (s *Storage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lo >= hi {
		return nil, fmt.Errorf("invalid range: lo=%d, hi=%d", lo, hi)
	}

	firstIndex, err := s.FirstIndex()
	if err != nil {
		return nil, err
	}
	lastIndex, err := s.LastIndex()
	if err != nil {
		return nil, err
	}

	if lo < firstIndex {
		return nil, ErrCompacted
	}
	if hi > lastIndex+1 {
		return nil, fmt.Errorf("entries[%d:%d) is out of bound [%d:%d]", lo, hi, firstIndex, lastIndex+1)
	}

	// Only contains dummy entries.
	if len(s.entries) == 0 {
		return nil, ErrUnavailable
	}

	offset := s.entries[0].Index
	if lo < offset {
		return nil, ErrCompacted
	}
	if hi > offset+uint64(len(s.entries)) {
		return nil, ErrUnavailable
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

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (s *Storage) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	firstIndex, err := s.FirstIndex()
	if err != nil {
		return 0, err
	}
	lastIndex, err := s.LastIndex()
	if err != nil {
		return 0, err
	}

	if i < firstIndex-1 {
		return 0, ErrCompacted
	}
	if i > lastIndex {
		return 0, fmt.Errorf("term of index %d is out of bound", i)
	}

	if i == firstIndex-1 {
		return s.snapshot.Metadata.Term, nil
	}

	if len(s.entries) == 0 {
		return 0, ErrUnavailable
	}

	offset := s.entries[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if i >= offset+uint64(len(s.entries)) {
		return 0, ErrUnavailable
	}

	return s.entries[i-offset].Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (s *Storage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.entries) == 0 {
		return s.snapshot.Metadata.Index, nil
	}

	return s.entries[len(s.entries)-1].Index, nil
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot; if storage only contains the dummy entry the
// first log entry is not available).
func (s *Storage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.snapshot.Metadata.Index + 1, nil
}

// Snapshot returns the most recent snapshot.
// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// snapshot and call Snapshot later.
func (s *Storage) Snapshot() (raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.snapshot, nil
}

// SetHardState sets the hard state
func (s *Storage) SetHardState(st raftpb.HardState) {
	s.mu.Lock()
	s.hardState = st
	s.mu.Unlock()

	// Save to disk (outside of lock to avoid blocking)
	if err := s.save(); err != nil {
		s.logger.Error("Failed to save storage to disk", zap.Error(err))
	}
}

// Append appends entries to the log
func (s *Storage) Append(entries []raftpb.Entry) error {
	s.mu.Lock()

	if len(entries) == 0 {
		s.mu.Unlock()
		return nil
	}

	// Shorten the log if there are conflicting entries
	if len(s.entries) > 0 {
		offset := s.entries[0].Index
		last := entries[0].Index + uint64(len(entries)) - 1
		if last < offset {
			// All entries are before the first entry, nothing to do
			s.mu.Unlock()
			return nil
		}
		if entries[0].Index > offset+uint64(len(s.entries)) {
			// All entries are after the last entry, just append
			s.entries = append(s.entries, entries...)
		} else {
			// There's overlap, truncate and append
			truncateIndex := entries[0].Index
			if truncateIndex > offset {
				s.entries = s.entries[:truncateIndex-offset]
			}
			s.entries = append(s.entries, entries...)
		}
	} else {
		// No existing entries, just append
		s.entries = append(s.entries, entries...)
	}
	s.mu.Unlock()

	// Save to disk (outside of lock to avoid blocking)
	if err := s.save(); err != nil {
		s.logger.Error("Failed to save storage to disk", zap.Error(err))
	}

	return nil
}

// CreateSnapshot creates a snapshot at the given index
func (s *Storage) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	s.mu.Lock()

	if i <= s.snapshot.Metadata.Index {
		return raftpb.Snapshot{}, ErrSnapOutOfDate
	}

	// Get term directly without taking another lock (we already have the write lock)
	term, err := s.termLocked(i)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	s.snapshot = raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     i,
			Term:      term,
			ConfState: *cs,
		},
		Data: data,
	}
	snap := s.snapshot
	s.mu.Unlock()

	// Save to disk (outside of lock to avoid blocking)
	if err := s.save(); err != nil {
		s.logger.Error("Failed to save storage to disk", zap.Error(err))
	}

	return snap, nil
}

// termLocked returns the term of entry i without taking a lock (assumes lock is already held)
func (s *Storage) termLocked(i uint64) (uint64, error) {
	firstIndex := s.snapshot.Metadata.Index + 1
	var lastIndex uint64
	if len(s.entries) == 0 {
		lastIndex = s.snapshot.Metadata.Index
	} else {
		lastIndex = s.entries[len(s.entries)-1].Index
	}

	if i < firstIndex-1 {
		return 0, ErrCompacted
	}
	if i > lastIndex {
		return 0, fmt.Errorf("term of index %d is out of bound", i)
	}

	if i == firstIndex-1 {
		return s.snapshot.Metadata.Term, nil
	}

	if len(s.entries) == 0 {
		return 0, ErrUnavailable
	}

	offset := s.entries[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if i >= offset+uint64(len(s.entries)) {
		return 0, ErrUnavailable
	}

	return s.entries[i-offset].Term, nil
}

// ApplySnapshot applies a snapshot to the storage
func (s *Storage) ApplySnapshot(snap raftpb.Snapshot) error {
	s.mu.Lock()
	s.snapshot = snap
	s.entries = nil // Clear entries after applying snapshot
	s.mu.Unlock()

	// Save to disk (outside of lock to avoid blocking)
	if err := s.save(); err != nil {
		s.logger.Error("Failed to save storage to disk", zap.Error(err))
	}

	return nil
}

// Compact compacts the log up to the given index
func (s *Storage) Compact(compactIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if compactIndex <= s.snapshot.Metadata.Index {
		return ErrCompacted
	}

	firstIndex := s.snapshot.Metadata.Index + 1
	if compactIndex < firstIndex {
		return ErrCompacted
	}

	if len(s.entries) == 0 {
		return nil
	}

	offset := s.entries[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}

	// Truncate entries before compactIndex
	truncateIndex := compactIndex - offset
	if truncateIndex < uint64(len(s.entries)) {
		s.entries = s.entries[truncateIndex:]
	} else {
		s.entries = s.entries[:0]
	}

	return nil
}

// save persists the storage state to disk
func (s *Storage) save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Save HardState
	hardStateData, err := json.Marshal(s.hardState)
	if err != nil {
		return fmt.Errorf("marshaling hardstate: %w", err)
	}
	if err := os.WriteFile(s.hardStateFile, hardStateData, 0644); err != nil {
		return fmt.Errorf("writing hardstate file: %w", err)
	}

	// Save Entries
	entriesData, err := json.Marshal(s.entries)
	if err != nil {
		return fmt.Errorf("marshaling entries: %w", err)
	}
	if err := os.WriteFile(s.entriesFile, entriesData, 0644); err != nil {
		return fmt.Errorf("writing entries file: %w", err)
	}

	// Save Snapshot
	snapshotData, err := json.Marshal(s.snapshot)
	if err != nil {
		return fmt.Errorf("marshaling snapshot: %w", err)
	}
	if err := os.WriteFile(s.snapshotFile, snapshotData, 0644); err != nil {
		return fmt.Errorf("writing snapshot file: %w", err)
	}

	return nil
}

// restore restores the storage state from disk
func (s *Storage) restore() error {
	// Restore HardState
	if data, err := os.ReadFile(s.hardStateFile); err == nil {
		if err := json.Unmarshal(data, &s.hardState); err != nil {
			return fmt.Errorf("unmarshaling hardstate: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("reading hardstate file: %w", err)
	}

	// Restore Entries
	if data, err := os.ReadFile(s.entriesFile); err == nil {
		if err := json.Unmarshal(data, &s.entries); err != nil {
			return fmt.Errorf("unmarshaling entries: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("reading entries file: %w", err)
	}

	// Restore Snapshot
	if data, err := os.ReadFile(s.snapshotFile); err == nil {
		if err := json.Unmarshal(data, &s.snapshot); err != nil {
			return fmt.Errorf("unmarshaling snapshot: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("reading snapshot file: %w", err)
	}

	return nil
}
