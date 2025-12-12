package bucket

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

// FSM represents the finite state machine for a bucket Raft group
// It manages ledgers within a specific bucket
type FSM struct {
	ledgers    map[string]ledger.LedgerInfo // Map of ledger name -> ledger info
	lastLogIDs map[string]uint64            // Map of ledger name -> last log ID
	logger     logging.Logger
	logWriter  service.LogWriter
}

// newFSM creates a new bucket FSM
func newFSM(logger logging.Logger, logStore service.LogWriter) *FSM {
	return &FSM{
		ledgers:    make(map[string]ledger.LedgerInfo),
		lastLogIDs: make(map[string]uint64),
		logger:     logger,
		logWriter:  logStore,
	}
}

// handleCreateLedger handles the create ledger command for this bucket
func (f *FSM) handleCreateLedger(cmd raft.Command) (*ledger.LedgerInfo, error) {
	var createCmd CreateLedgerCommand
	if err := UnmarshalCommandData(cmd.Data, &createCmd); err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal create ledger command")
		return nil, fmt.Errorf("unmarshaling create ledger command: %w", err)
	}

	// Check if ledger already exists in this bucket
	if _, exists := f.ledgers[createCmd.Name]; exists {
		f.logger.WithFields(map[string]any{"name": createCmd.Name}).Infof("WARN: Ledger already exists in bucket")
		return nil, fmt.Errorf("ledger already exists in bucket: %s", createCmd.Name)
	}

	// Assign sequential ID to the ledger (IDs start at 1, so next ID is len(ledgers) + 1)
	ledgerID := uint64(len(f.ledgers) + 1)

	// Convert protobuf Struct to metadata.Metadata
	var md metadata.Metadata
	if createCmd.Metadata != nil {
		md = structToMetadata(createCmd.Metadata)
	}

	// Create ledger info using the command date
	ledgerInfo := ledger.LedgerInfo{
		ID:        ledgerID,
		Name:      createCmd.Name,
		CreatedAt: cmd.Date,
		Metadata:  md,
	}

	// Store the ledger
	f.ledgers[createCmd.Name] = ledgerInfo

	f.logger.Infof("Ledger created in bucket")
	return &ledgerInfo, nil
}

// handleInsertLog handles the insert log command by storing the log in memory and persisting it to the store
func (f *FSM) handleInsertLog(ctx context.Context, cmd raft.Command) error {
	var insertCmd InsertLogCommand
	if err := UnmarshalCommandData(cmd.Data, &insertCmd); err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal insert log command")
		return fmt.Errorf("unmarshaling insert log command: %w", err)
	}

	// Convert protobuf Log to ledger.Log
	log, err := logFromProto(insertCmd.Log)
	if err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to convert log from proto")
		return fmt.Errorf("converting log from proto: %w", err)
	}

	// Write log to store immediately
	if err := f.logWriter.InsertLogs(ctx, log); err != nil {
		f.logger.WithFields(map[string]any{"error": err, "ledger": log.Ledger}).Errorf("Failed to write log to store")
		return fmt.Errorf("writing log to store: %w", err)
	}

	// Update lastLogID for this ledger if log has an ID
	if log.ID != nil {
		currentLastID := f.lastLogIDs[log.Ledger]
		if *log.ID > currentLastID {
			f.lastLogIDs[log.Ledger] = *log.ID
		}
	}

	f.logger.WithFields(map[string]any{"ledger": log.Ledger}).Infof("Log stored in memory and persisted to store via FSM")
	return nil
}

func (f *FSM) ApplyEntry(ctx context.Context, command raft.Command) (any, error) {
	switch command.Type {
	case CommandTypeCreateLedger:
		return f.handleCreateLedger(command)
	case CommandTypeInsertLog:
		return nil, f.handleInsertLog(ctx, command)
	}
	return nil, fmt.Errorf("unknown command type: %s", command.Type)
}

// GetLedger returns the ledger info for a given name in this bucket
func (f *FSM) GetLedger(name string) (*ledger.LedgerInfo, error) {
	info, ok := f.ledgers[name]
	if !ok {
		return nil, fmt.Errorf("ledger does not exist: %s", name)
	}
	return &info, nil
}

// GetAllLedgers returns all ledgers in this bucket
func (f *FSM) GetAllLedgers() []ledger.LedgerInfo {
	// Return a copy to avoid external modifications
	result := make([]ledger.LedgerInfo, len(f.ledgers))
	for _, v := range f.ledgers {
		result = append(result, v)
	}
	return result
}

func (f *FSM) CreateSnapshot(ctx context.Context) ([]byte, error) {

	snapshotData := map[string]interface{}{
		"ledgers":    f.ledgers,
		"lastLogIDs": f.lastLogIDs,
	}

	// Marshal to JSON
	data, err := json.Marshal(snapshotData)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot data: %w", err)
	}

	return data, nil
}

// RestoreSnapshot restores the bucket FSM from a snapshot
func (f *FSM) RestoreSnapshot(ctx context.Context, data []byte) error {
	var snapshotData struct {
		Ledgers    map[string]ledger.LedgerInfo `json:"ledgers"`
		LastLogIDs map[string]uint64            `json:"lastLogIDs"`
	}

	if err := json.Unmarshal(data, &snapshotData); err != nil {
		return fmt.Errorf("unmarshaling snapshot data: %w", err)
	}

	f.ledgers = snapshotData.Ledgers
	if f.ledgers == nil {
		f.ledgers = make(map[string]ledger.LedgerInfo)
	}

	f.lastLogIDs = snapshotData.LastLogIDs
	if f.lastLogIDs == nil {
		f.lastLogIDs = make(map[string]uint64)
	}

	f.logger.WithFields(map[string]any{"ledgerCount": len(f.ledgers)}).Infof("BucketCluster FSM restored from snapshot")
	return nil
}
