package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// FSM represents the finite state machine for a ledger Raft group
// It manages a single ledger
type FSM struct {
	mu                sync.RWMutex       // Protects access to state
	state             ledger.LedgerState // FSM state
	logger            logging.Logger
	logWriter         service.LogWriter
	logReaderProvider func(uint64) service.LogReader // LogReader to catch up logs from leader via gRPC
}

// newFSM creates a new ledger FSM
func newFSM(logger logging.Logger, logStore service.LogWriter, logReaderProvider func(uint64) service.LogReader, ledgerInfo ledger.LedgerInfo) *FSM {
	return &FSM{
		state: ledger.LedgerState{
			LedgerInfo:   ledgerInfo,
			LastSequence: 0,
		},
		logger: logger.WithFields(map[string]any{
			"service": "ledger.fsm",
			"ledger":  ledgerInfo.Name,
		}),
		logWriter:         logStore,
		logReaderProvider: logReaderProvider,
	}
}

// GetState returns a copy of the FSM state
func (f *FSM) GetState() ledger.LedgerState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return ledger.LedgerState{
		LedgerInfo:   f.state.LedgerInfo,
		LastSequence: f.state.LastSequence,
	}
}

// processInsertLog handles the insert log command by storing the log in memory and persisting it to the store
func (f *FSM) processInsertLog(cmd raft.Command) (*ledger.Log, error) {
	var insertCmd InsertLogCommand
	if err := UnmarshalCommandData(cmd.Data, &insertCmd); err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal insert log command")
		return nil, err
	}

	// Convert protobuf Log to ledger.Log
	log, err := service.LogFromProto(insertCmd.Log)
	if err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to convert log from proto")
		return nil, err
	}

	f.mu.Lock()
	f.state.LastSequence++
	log.Sequence = f.state.LastSequence
	f.mu.Unlock()

	f.logger.
		WithFields(map[string]any{"sequence": log.Sequence}).
		Infof("Log stored in memory and persisted to store via FSM")
	return &log, nil
}

func (f *FSM) ApplyEntries(ctx context.Context, commands ...raft.Command) ([]raft.ApplyResult, error) {
	// Assume the majority of commands are logs insertion while allocating
	ret := make([]raft.ApplyResult, 0, len(commands))
	logs := make([]ledger.Log, 0, len(commands))
	for _, command := range commands {
		switch command.Type {
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
		default:
			ret = append(ret, raft.ApplyResult{
				Error: fmt.Errorf("unknown command type: %s", command.Type),
			})
		}
	}
	if len(logs) > 0 {
		if err := f.logWriter.InsertLogs(ctx, logs...); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func (f *FSM) CreateSnapshot(ctx context.Context) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	snapshotData := map[string]interface{}{
		"ledgerInfo":   f.state.LedgerInfo,
		"lastSequence": f.state.LastSequence,
	}

	// Marshal to JSON
	data, err := json.Marshal(snapshotData)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot data: %w", err)
	}

	return data, nil
}

// RestoreSnapshot restores the ledger FSM from a snapshot
func (f *FSM) RestoreSnapshot(ctx context.Context, leader uint64, snapshot raftpb.Snapshot) {
	var snapshotData ledger.LedgerState

	if err := json.Unmarshal(snapshot.Data, &snapshotData); err != nil {
		panic(err)
	}

	f.mu.Lock()
	f.state.LedgerInfo = snapshotData.LedgerInfo
	f.mu.Unlock()

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
		cursor, err := f.logReaderProvider(leader).GetAllLogs(ctx, storeLastSequence, snapshotData.LastSequence) // 0 = no limit
		if err != nil {
			panic(fmt.Errorf("getting logs from reader for catch-up: %w", err))
		}
		defer func() {
			_ = cursor.Close()
		}()

		var (
			// todo: flush regularly
			logsToWrite []ledger.Log
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
		}

		// Write all collected logs to the writer
		if len(logsToWrite) > 0 {
			if err := f.logWriter.InsertLogs(ctx, logsToWrite...); err != nil {
				panic(fmt.Errorf("writing catch-up logs to store: %w", err))
			}
			f.logger.WithFields(map[string]any{
				"logsWritten": len(logsToWrite),
			}).Infof("Caught up logs from reader to writer")

			// Update lastSequence to match the store's last sequence
			f.mu.Lock()
			f.state.LastSequence = logsToWrite[len(logsToWrite)-1].Sequence
			f.mu.Unlock()
		}
	} else {
		// Snapshot is up to date or ahead, use snapshot's sequence
		f.mu.Lock()
		f.state.LastSequence = snapshotData.LastSequence
		f.mu.Unlock()
	}

	f.mu.RLock()
	lastSequence := f.state.LastSequence
	f.mu.RUnlock()

	f.logger.WithFields(map[string]any{
		"ledger":        f.state.LedgerInfo.Name,
		"storeSequence": lastSequence,
	}).Infof("Ledger FSM restored from snapshot")
}
