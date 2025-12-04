package raft

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

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
	logReader service.LogReader             // Needed for restore
	lastID    uint64                        // Last assigned log ID
	logs      []ledger.Log                  // In-memory logs waiting to be persisted
	ledgers   map[string]service.LedgerInfo // Map of ledger name -> ledger info
}

func NewFSM(logger *zap.Logger, store service.LogWriter, logReader service.LogReader) *FSM {
	return &FSM{
		logger:    logger,
		store:     store,
		logReader: logReader,
		lastID:    0, // Start at 0, first log will be 1
		logs:      make([]ledger.Log, 0),
		ledgers:   make(map[string]service.LedgerInfo),
	}
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.logger.Debug("Applying log entry", zap.Uint64("index", log.Index))

	// Decode the command from the Raft log data
	var cmd service.Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error("Failed to unmarshal command", zap.Error(err))
		return fmt.Errorf("unmarshaling command: %w", err)
	}

	// Route to the appropriate command handler
	switch cmd.Type {
	case service.CommandTypeInsertLogs:
		return f.handleInsertLogs(cmd.Data, log.Index)
	case service.CommandTypeCreateLedger:
		return f.handleCreateLedger(cmd.Data, log.Index)
	default:
		f.logger.Error("Unknown command type", zap.String("type", string(cmd.Type)))
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

// handleInsertLogs handles the insert logs command
func (f *FSM) handleInsertLogs(data json.RawMessage, index uint64) error {
	var insertCmd service.InsertLogsCommand
	if err := json.Unmarshal(data, &insertCmd); err != nil {
		f.logger.Error("Failed to unmarshal insert logs command", zap.Error(err))
		return fmt.Errorf("unmarshaling insert logs command: %w", err)
	}

	// Assign IDs to each log and store them in memory
	for i := range insertCmd.Logs {
		f.lastID++
		insertCmd.Logs[i].ID = pointer.For(f.lastID)
		f.logs = append(f.logs, insertCmd.Logs[i])
	}

	f.logger.Debug("Logs stored in memory", zap.Uint64("index", index), zap.Int("count", len(insertCmd.Logs)), zap.Uint64("lastID", f.lastID), zap.Int("totalLogsInMemory", len(f.logs)))
	return nil
}

// handleCreateLedger handles the create ledger command
func (f *FSM) handleCreateLedger(data json.RawMessage, index uint64) error {
	var createCmd service.CreateLedgerCommand
	if err := json.Unmarshal(data, &createCmd); err != nil {
		f.logger.Error("Failed to unmarshal create ledger command", zap.Error(err))
		return fmt.Errorf("unmarshaling create ledger command: %w", err)
	}

	// Check if ledger already exists
	if _, exists := f.ledgers[createCmd.Name]; exists {
		f.logger.Warn("Ledger already exists", zap.String("name", createCmd.Name))
		return fmt.Errorf("ledger already exists: %s", createCmd.Name)
	}

	// Create ledger info
	ledgerInfo := service.LedgerInfo{
		Name:      createCmd.Name,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Metadata:  createCmd.Metadata,
	}

	// Store the ledger
	f.ledgers[createCmd.Name] = ledgerInfo

	f.logger.Info("Ledger created", zap.Uint64("index", index), zap.String("name", createCmd.Name))
	return nil
}

// GetLedger returns the ledger info for a given name
func (f *FSM) GetLedger(name string) (service.LedgerInfo, bool) {
	info, ok := f.ledgers[name]
	return info, ok
}

// GetAllLedgers returns all ledgers
func (f *FSM) GetAllLedgers() map[string]service.LedgerInfo {
	// Return a copy to avoid external modifications
	result := make(map[string]service.LedgerInfo, len(f.ledgers))
	for k, v := range f.ledgers {
		result[k] = v
	}
	return result
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

	// Create a copy of ledgers to pass to snapshot
	ledgersCopy := make(map[string]service.LedgerInfo, len(f.ledgers))
	for k, v := range f.ledgers {
		ledgersCopy[k] = v
	}

	return &snapshot{
		lastID:  f.lastID,
		logs:    logsCopy,
		ledgers: ledgersCopy,
		store:   f.store,
		logger:  f.logger,
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

	// Try to read ledgers from snapshot (optional, for backward compatibility)
	decoder := json.NewDecoder(reader)
	var ledgers map[string]service.LedgerInfo
	if err := decoder.Decode(&ledgers); err != nil {
		// If we can't read ledgers (old snapshot format), start with empty map
		ledgers = make(map[string]service.LedgerInfo)
		f.logger.Debug("Could not read ledgers from snapshot, starting with empty map", zap.Error(err))
	} else {
		f.ledgers = ledgers
	}

	f.logger.Info("FSM restored from snapshot", zap.Uint64("lastID", lastID), zap.Int("ledgersCount", len(f.ledgers)))
	return nil
}

// snapshot implements raft.FSMSnapshot
type snapshot struct {
	lastID  uint64
	logs    []ledger.Log
	ledgers map[string]service.LedgerInfo
	store   service.LogWriter
	logger  *zap.Logger
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

	// Write ledgers to the snapshot as JSON
	encoder := json.NewEncoder(sink)
	if err := encoder.Encode(s.ledgers); err != nil {
		return fmt.Errorf("writing ledgers to snapshot: %w", err)
	}

	return sink.Close()
}

func (s *snapshot) Release() {}
