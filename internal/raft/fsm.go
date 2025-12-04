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
	logger *zap.Logger
	store  service.LogWriter
	lastID uint64 // Last assigned log ID
}

func NewFSM(logger *zap.Logger, store service.LogWriter) *FSM {
	return &FSM{
		logger: logger,
		store:  store,
		lastID: 0, // Start at 0, first log will be 1
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

	// Assign IDs to each log
	for i := range ledgerLogs {
		f.lastID++
		ledgerLogs[i].ID = pointer.For(f.lastID)
	}
	lastID := f.lastID

	// Persist all logs to the store
	ctx := context.Background() // FSM doesn't have context, use background
	if err := f.store.InsertLogs(ctx, ledgerLogs...); err != nil {
		f.logger.Error("Failed to insert logs into store", zap.Error(err))
		return fmt.Errorf("inserting logs: %w", err)
	}

	f.logger.Debug("Logs persisted successfully", zap.Uint64("index", log.Index), zap.Int("count", len(ledgerLogs)), zap.Uint64("lastID", lastID))
	return nil
}

// Snapshot returns a snapshot of the FSM state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.logger.Debug("Creating snapshot", zap.Uint64("lastID", f.lastID))
	return &snapshot{lastID: f.lastID}, nil
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

	f.logger.Info("FSM restored from snapshot", zap.Uint64("lastID", lastID))
	return nil
}

// snapshot implements raft.FSMSnapshot
type snapshot struct {
	lastID uint64
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	// Write the last ID to the snapshot
	if err := binary.Write(sink, binary.BigEndian, s.lastID); err != nil {
		return fmt.Errorf("writing lastID to snapshot: %w", err)
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
