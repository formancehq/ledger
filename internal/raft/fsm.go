package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

// FSM implements the raft.FSM interface
type FSM struct {
	logger *zap.Logger
	store  service.LogWriter
}

func NewFSM(logger *zap.Logger, store service.LogWriter) *FSM {
	return &FSM{
		logger: logger,
		store:  store,
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

	// Persist all logs to the store
	ctx := context.Background() // FSM doesn't have context, use background
	if err := f.store.InsertLogs(ctx, ledgerLogs...); err != nil {
		f.logger.Error("Failed to insert logs into store", zap.Error(err))
		return fmt.Errorf("inserting logs: %w", err)
	}

	f.logger.Debug("Logs persisted successfully", zap.Uint64("index", log.Index), zap.Int("count", len(ledgerLogs)))
	return nil
}

// Snapshot returns a snapshot of the FSM state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.logger.Debug("Creating snapshot")
	return &snapshot{}, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(reader io.ReadCloser) error {
	f.logger.Debug("Restoring from snapshot")
	defer reader.Close()
	return nil
}

// snapshot implements raft.FSMSnapshot
type snapshot struct{}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (s *snapshot) Release() {}
