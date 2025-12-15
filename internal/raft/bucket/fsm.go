package bucket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

// FSM represents the finite state machine for a bucket Raft group
// It manages ledgers within a specific bucket
type FSM struct {
	ledgers      map[string]ledger.LedgerInfo // Map of ledger name -> ledger info
	lastSequence uint64                       // Last global sequence number
	logger       logging.Logger
	logWriter    service.LogWriter
	logReader    service.LogReader // LogReader to catch up logs from leader via gRPC
}

// newFSM creates a new bucket FSM
func newFSM(logger logging.Logger, logStore service.LogWriter, logReader service.LogReader) *FSM {
	return &FSM{
		ledgers:      make(map[string]ledger.LedgerInfo),
		lastSequence: 0,
		logger:       logger,
		logWriter:    logStore,
		logReader:    logReader,
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

// processInsertLog handles the insert log command by storing the log in memory and persisting it to the store
func (f *FSM) processInsertLog(cmd raft.Command) (*ledger.Log, error) {
	var insertCmd InsertLogCommand
	if err := UnmarshalCommandData(cmd.Data, &insertCmd); err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal insert log command")
		return nil, err
	}

	// Convert protobuf Log to ledger.Log
	log, err := logFromProto(insertCmd.Log)
	if err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to convert log from proto")
		return nil, err
	}

	// Generate sequence number in FSM
	f.lastSequence++
	log.Sequence = f.lastSequence

	f.logger.WithFields(map[string]any{"ledger": log.Ledger, "sequence": log.Sequence}).Infof("Log stored in memory and persisted to store via FSM")
	return &log, nil
}

func (f *FSM) ApplyEntries(ctx context.Context, commands ...raft.Command) []raft.ApplyResult {
	// Assume the majority of commands are logs insertion while allocating
	ret := make([]raft.ApplyResult, 0, len(commands))
	logs := make([]ledger.Log, 0, len(commands))
	for _, command := range commands {
		switch command.Type {
		case CommandTypeCreateLedger:
			info, err := f.handleCreateLedger(command)
			if err != nil {
				ret = append(ret, raft.ApplyResult{
					Error: err,
				})
				continue
			}
			ret = append(ret, raft.ApplyResult{
				Result: info,
			})
		case CommandTypeInsertLog:
			log, err := f.processInsertLog(command)
			if err != nil {
				ret = append(ret, raft.ApplyResult{
					Error: err,
				})
				continue
			}
			ret = append(ret, raft.ApplyResult{
				Result: log,
			})
			logs = append(logs, *log)
		}
	}
	if len(logs) > 0 {
		if err := f.logWriter.InsertLogs(ctx, logs...); err != nil {
			// Well, the panic is a bit brutal.
			// But fundamentally, this is what we want.
			// A raft node should be considered corrupted if it cannot persist in its state.
			panic(err)
		}
	}

	return ret
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
		"ledgers":      f.ledgers,
		"lastSequence": f.lastSequence,
	}

	// Marshal to JSON
	data, err := json.Marshal(snapshotData)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot data: %w", err)
	}

	return data, nil
}

// RestoreSnapshot restores the bucket FSM from a snapshot
func (f *FSM) RestoreSnapshot(ctx context.Context, data []byte) {
	var snapshotData struct {
		Ledgers      map[string]ledger.LedgerInfo `json:"ledgers"`
		LastSequence uint64                       `json:"lastSequence"`
	}

	if err := json.Unmarshal(data, &snapshotData); err != nil {
		panic(err)
	}

	f.ledgers = snapshotData.Ledgers
	if f.ledgers == nil {
		f.ledgers = make(map[string]ledger.LedgerInfo)
	}

	f.lastSequence = snapshotData.LastSequence

	// Compare snapshot's lastSequence with the log store's lastSequenceID
	storeLastSequence, err := f.logWriter.GetLastSequenceID(ctx)
	if err != nil {
		panic(fmt.Errorf("getting last sequence ID from log store: %w", err))
	}

	// If the log store is ahead of the snapshot, catch up by reading missing logs from the reader
	if storeLastSequence < snapshotData.LastSequence {
		f.logger.WithFields(map[string]any{
			"snapshotSequence": snapshotData.LastSequence,
			"storeSequence":    storeLastSequence,
		}).Infof("Log store is ahead of snapshot, catching up logs")

		// Read all logs from the reader starting from the sequence after the snapshot
		fromSequence := storeLastSequence
		cursor, err := f.logReader.GetAllLogs(ctx, fromSequence)
		if err != nil {
			panic(fmt.Errorf("getting logs from reader for catch-up: %w", err))
		}
		defer func() {
			_ = cursor.Close()
		}()

		var (
			// todo: flush regularly
			logsToWrite []ledger.Log
			maxSequence = snapshotData.LastSequence
		)

		// Collect all logs that need to be written
		for {
			log, err := cursor.Next(ctx)
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(fmt.Errorf("reading log during catch-up: %w", err))
			}

			logsToWrite = append(logsToWrite, log)
			maxSequence = log.Sequence
		}

		// Write all collected logs to the writer
		if len(logsToWrite) > 0 {
			if err := f.logWriter.InsertLogs(ctx, logsToWrite...); err != nil {
				panic(fmt.Errorf("writing catch-up logs to store: %w", err))
			}
			f.logger.WithFields(map[string]any{
				"logsWritten":  len(logsToWrite),
				"fromSequence": fromSequence,
				"toSequence":   maxSequence,
			}).Infof("Caught up logs from reader to writer")
		}

		// Update lastSequence to match the store's last sequence
		f.lastSequence = storeLastSequence
	} else {
		// Snapshot is up to date or ahead, use snapshot's sequence
		f.lastSequence = snapshotData.LastSequence
	}

	f.logger.WithFields(map[string]any{
		"ledgerCount":   len(f.ledgers),
		"lastSequence":  f.lastSequence,
		"storeSequence": storeLastSequence,
	}).Infof("BucketCluster FSM restored from snapshot")
}
