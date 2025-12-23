package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// FSM represents the finite state machine for a ledger Raft group
// It manages a single ledger
type FSM struct {
	mu                sync.RWMutex         // Protects access to state
	state             ledgerpb.LedgerState // FSM state
	logger            logging.Logger
	logWriter         service.LogWriter
	logReaderProvider func(uint64) service.LogReader // LogReader to catch up logs from leader via gRPC
}

// newFSM creates a new ledger FSM
func newFSM(logger logging.Logger, logStore service.LogWriter, logReaderProvider func(uint64) service.LogReader, ledgerInfo *ledgerpb.LedgerInfo) *FSM {
	return &FSM{
		state: ledgerpb.LedgerState{
			LedgerInfo: ledgerInfo,
			LastLogID:  0,
		},
		logger: logger.WithFields(map[string]any{
			"service": "ledgerpb.fsm",
			"ledger":  ledgerInfo.GetName(),
		}),
		logWriter:         logStore,
		logReaderProvider: logReaderProvider,
	}
}

// GetState returns a copy of the FSM state
func (f *FSM) GetState() ledgerpb.LedgerState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return ledgerpb.LedgerState{
		LedgerInfo: f.state.LedgerInfo,
		LastLogID:  f.state.LastLogID,
	}
}

// processInsertLog handles the insert log command by storing the log in memory and persisting it to the store
func (f *FSM) processInsertLog(cmd raft.Command) (*ledgerpb.Log, error) {
	var insertCmd InsertLogCommand
	if err := UnmarshalCommandData(cmd.Data, &insertCmd); err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal insert log command")
		return nil, err
	}

	f.mu.Lock()
	f.state.LastLogID++
	insertCmd.Log.Id = f.state.LastLogID
	f.mu.Unlock()

	return insertCmd.Log, nil
}

func (f *FSM) ApplyEntries(ctx context.Context, commands ...raft.Command) ([]raft.ApplyResult, error) {
	// Assume the majority of commands are logs insertion while allocating
	ret := make([]raft.ApplyResult, 0, len(commands))
	logs := make([]*ledgerpb.Log, 0, len(commands))
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
			logs = append(logs, log)
		default:
			ret = append(ret, raft.ApplyResult{
				Error: fmt.Errorf("unknown command type: %s", command.Type),
			})
		}
	}
	if len(logs) > 0 {
		f.logger.
			WithFields(map[string]any{"count": len(logs)}).
			Infof("Log stored in memory and persisted to store via FSM")
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
		"ledgerInfo": f.state.LedgerInfo,
		"lastLogID":  f.state.LastLogID,
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
	var snapshotData ledgerpb.LedgerState

	if err := json.Unmarshal(snapshot.Data, &snapshotData); err != nil {
		panic(err)
	}

	f.mu.Lock()
	f.state.LedgerInfo = snapshotData.LedgerInfo
	f.mu.Unlock()

	storeLastLogID, err := f.logWriter.GetLastLogID(ctx)
	if err != nil {
		panic(fmt.Errorf("getting last log ID from log store: %w", err))
	}

	if storeLastLogID < snapshotData.LastLogID {
		f.logger.WithFields(map[string]any{
			"snapshotLogID": snapshotData.LastLogID,
			"storeLogID":    storeLastLogID,
		}).Infof("Log store is ahead of snapshot, catching up logs")

		cursor, err := f.logReaderProvider(leader).GetAllLogs(ctx, storeLastLogID, snapshotData.LastLogID) // 0 = no limit
		if err != nil {
			panic(fmt.Errorf("getting logs from reader for catch-up: %w", err))
		}
		defer func() {
			_ = cursor.Close()
		}()

		var (
			// todo: flush regularly
			logsToWrite []*ledgerpb.Log
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

			f.mu.Lock()
			f.state.LastLogID = logsToWrite[len(logsToWrite)-1].Id
			f.mu.Unlock()
		}
	} else {
		f.mu.Lock()
		f.state.LastLogID = snapshotData.LastLogID
		f.mu.Unlock()
	}

	f.mu.RLock()
	lastLogID := f.state.LastLogID
	f.mu.RUnlock()

	f.logger.WithFields(map[string]any{
		"ledger":     f.state.LedgerInfo.Name,
		"storeLogID": lastLogID,
	}).Infof("Ledger FSM restored from snapshot")
}
