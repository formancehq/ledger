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

// Note: With etcd/raft, we don't have an Apply method on FSM.
// The entries are applied directly in the readyLoop of Cluster.
// The handleInsertLogs and handleCreateLedger methods are called from Cluster.applyEntry.
// These methods are exported so they can be called from Cluster.

// HandleInsertLogs handles the insert logs command
func (f *FSM) HandleInsertLogs(data json.RawMessage, index uint64) error {
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

// HandleCreateLedger handles the create ledger command
func (f *FSM) HandleCreateLedger(data json.RawMessage, index uint64) error {
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
// CreateSnapshot creates a snapshot for etcd/raft
func (f *FSM) CreateSnapshot(index uint64) ([]byte, error) {
	f.logger.Info("FSM: Creating snapshot", zap.Uint64("index", index), zap.Uint64("lastID", f.lastID), zap.Int("logsToPersist", len(f.logs)))

	// Write all logs to the store before persisting the snapshot
	if len(f.logs) > 0 {
		f.logger.Info("FSM: Persisting logs to store", zap.Int("count", len(f.logs)))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := f.store.InsertLogs(ctx, f.logs...); err != nil {
			f.logger.Error("Failed to persist logs to store during snapshot", zap.Error(err))
			return nil, fmt.Errorf("persisting logs to store: %w", err)
		}

		f.logger.Info("FSM: Logs persisted to store during snapshot", zap.Int("count", len(f.logs)))
		// Clear in-memory logs after persisting
		f.logs = f.logs[:0]
	} else {
		f.logger.Debug("FSM: No logs to persist")
	}

	// Create snapshot data
	snapshotData := map[string]interface{}{
		"lastID":  f.lastID,
		"ledgers": f.ledgers,
	}

	// Marshal to JSON
	data, err := json.Marshal(snapshotData)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot data: %w", err)
	}

	return data, nil
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

// RestoreSnapshot restores FSM state from snapshot data
func (f *FSM) RestoreSnapshot(data []byte) error {
	var snapshotData struct {
		LastID  uint64                        `json:"lastID"`
		Ledgers map[string]service.LedgerInfo `json:"ledgers"`
	}

	if err := json.Unmarshal(data, &snapshotData); err != nil {
		return fmt.Errorf("unmarshaling snapshot data: %w", err)
	}

	f.lastID = snapshotData.LastID
	f.ledgers = snapshotData.Ledgers
	f.logs = make([]ledger.Log, 0)

	f.logger.Info("FSM restored from snapshot", zap.Uint64("lastID", f.lastID), zap.Int("ledgersCount", len(f.ledgers)))
	return nil
}
