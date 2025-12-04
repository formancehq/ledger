package raft

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/formancehq/go-libs/v3/pointer"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

// FSM implements the raft.FSM interface
type FSM struct {
	logger    *zap.Logger
	store     service.LogWriter
	logReader service.LogReader // Needed for restore
	lastID    uint64            // Last assigned log ID
	logs      []ledger.Log      // In-memory logs waiting to be persisted
}

func NewFSM(logger *zap.Logger, store service.LogWriter, logReader service.LogReader) *FSM {
	return &FSM{
		logger:    logger,
		store:     store,
		logReader: logReader,
		lastID:    0, // Start at 0, first log will be 1
		logs:      make([]ledger.Log, 0),
	}
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.logger.Debug("Applying log entry", zap.Uint64("index", log.Index))

	// Decode the array of ledger logs from the Raft log data
	var ledgerLogs []ledger.Log
	if err := json.Unmarshal(log.Data, &ledgerLogs); err != nil {
		f.logger.Error("Failed to unmarshal ledger logs", zap.Error(err))
		return fmt.Errorf("unmarshaling ledger logs: %w", err)
	}

	// Assign IDs to each log and store them in memory
	for i := range ledgerLogs {
		f.lastID++
		ledgerLogs[i].ID = pointer.For(f.lastID)
		f.logs = append(f.logs, ledgerLogs[i])
	}

	f.logger.Debug("Logs stored in memory", zap.Uint64("index", log.Index), zap.Int("count", len(ledgerLogs)), zap.Uint64("lastID", f.lastID), zap.Int("totalLogsInMemory", len(f.logs)))
	return nil
}

// Snapshot returns a snapshot of the FSM state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.logger.Debug("Creating snapshot", zap.Uint64("lastID", f.lastID), zap.Int("logsToPersist", len(f.logs)))

	// Create a copy of logs to pass to snapshot (will be persisted in Persist method)
	logsCopy := make([]ledger.Log, len(f.logs))
	copy(logsCopy, f.logs)

	// Clear in-memory logs - they will be persisted in snapshot.Persist()
	// If Persist() fails, Raft will replay the logs anyway
	f.logs = f.logs[:0]

	return &snapshot{
		lastID: f.lastID,
		logs:   logsCopy,
		store:  f.store,
		logger: f.logger,
	}, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(reader io.ReadCloser) error {
	f.logger.Debug("Restoring from snapshot")
	defer reader.Close()

	// Read the last ID from the snapshot
	var lastID uint64
	if err := binary.Read(reader, binary.BigEndian, &lastID); err != nil {
		// If we can't read (empty snapshot or error), start from 0
		lastID = 0
		f.logger.Warn("Could not read lastID from snapshot, starting from 0", zap.Error(err))
	}

	f.lastID = lastID
	// Clear in-memory logs - they will be replayed from Raft logs after restore
	f.logs = make([]ledger.Log, 0)

	f.logger.Info("FSM restored from snapshot", zap.Uint64("lastID", lastID))
	return nil
}

// snapshot implements raft.FSMSnapshot
type snapshot struct {
	lastID uint64
	logs   []ledger.Log
	store  service.LogWriter
	logger *zap.Logger
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	// Write all logs to the store before persisting the snapshot
	if len(s.logs) > 0 {
		ctx := context.Background()
		if err := s.store.InsertLogs(ctx, s.logs...); err != nil {
			s.logger.Error("Failed to persist logs to store during snapshot persist", zap.Error(err))
			return fmt.Errorf("persisting logs to store: %w", err)
		}

		s.logger.Debug("Logs persisted to store during snapshot persist", zap.Int("count", len(s.logs)))
	}

	// Write the last ID to the snapshot
	if err := binary.Write(sink, binary.BigEndian, s.lastID); err != nil {
		return fmt.Errorf("writing lastID to snapshot: %w", err)
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
