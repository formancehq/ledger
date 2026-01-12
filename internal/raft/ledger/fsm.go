package ledger

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"sync"

	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type RuntimeStoreWithMetrics interface {
	service.RuntimeStore
	service.MetricsAware
}

// FSM represents the finite state machine for a ledger Raft group
// It manages a single ledger
type FSM struct {
	mu                sync.RWMutex          // Protects access to state
	state             *ledgerpb.LedgerState // FSM state
	logger            logging.Logger
	runtimeStore      RuntimeStoreWithMetrics
	logReaderProvider func(uint64) service.LogReader // LogReader to catch up logs from leader via gRPC
}

// newFSM creates a new ledger FSM
func newFSM(
	logger logging.Logger,
	runtimeStore RuntimeStoreWithMetrics,
	logReaderProvider func(uint64) service.LogReader,
	initialState *ledgerpb.LedgerState,
) *FSM {
	return &FSM{
		state: initialState,
		logger: logger.WithFields(map[string]any{
			"service": "ledger.fsm",
			"ledger":  initialState.LedgerInfo.GetName(),
		}),
		runtimeStore:      runtimeStore,
		logReaderProvider: logReaderProvider,
	}
}

// GetState returns a copy of the FSM state
func (f *FSM) GetState() *ledgerpb.LedgerState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	rawStoreMetrics, err := json.Marshal(f.runtimeStore.Metrics())
	if err != nil {
		panic(err)
	}
	mapStoreMetrics := make(map[string]interface{})
	if err := json.Unmarshal(rawStoreMetrics, &mapStoreMetrics); err != nil {
		panic(err)
	}

	storeMetrics, err := structpb.NewStruct(mapStoreMetrics)
	if err != nil {
		panic(err)
	}

	return &ledgerpb.LedgerState{
		LedgerInfo:        f.state.LedgerInfo,
		LastLogId:         f.state.LastLogId,
		LastTransactionId: f.state.LastTransactionId,
		StoreMetrics:      storeMetrics,
	}
}

// processInsertLog handles the insert log command by building the log entry
func (f *FSM) processCreateLog(ctx context.Context, raftCommand *raft.Command) (*ledgerpb.Log, error) {
	var createCmd CreateLogCommand
	if err := UnmarshalCommandData(raftCommand.Data, &createCmd); err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal insert log command")
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var logPayload *ledgerpb.LogPayload
	switch cmd := createCmd.Input.Command.(type) {
	case *ledgerpb.CommandInput_AppendTransaction:
		f.state.LastTransactionId++
		timestamp := cmd.AppendTransaction.Timestamp
		if timestamp == nil || timestamp.Data == 0 {
			timestamp = raftCommand.Date
		}
		logPayload = &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_CreatedTransaction{
				CreatedTransaction: &ledgerpb.CreatedTransaction{
					Transaction: &ledgerpb.Transaction{
						Postings:   cmd.AppendTransaction.Postings,
						Metadata:   cmd.AppendTransaction.Metadata,
						Timestamp:  timestamp,
						Reference:  cmd.AppendTransaction.Reference,
						Id:         f.state.LastTransactionId,
						InsertedAt: raftCommand.Date,
						UpdatedAt:  raftCommand.Date,
					},
					AccountMetadata: cmd.AppendTransaction.AccountMetadata,
				},
			},
		}
	case *ledgerpb.CommandInput_SaveMetadata:
		logPayload = &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_SavedMetadata{
				SavedMetadata: &ledgerpb.SavedMetadata{
					Target:   cmd.SaveMetadata.Target,
					Metadata: cmd.SaveMetadata.Metadata,
				},
			},
		}
	case *ledgerpb.CommandInput_DeleteMetadata:
		logPayload = &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_DeletedMetadata{
				DeletedMetadata: &ledgerpb.DeletedMetadata{
					Target: cmd.DeleteMetadata.Target,
					Key:    cmd.DeleteMetadata.Key,
				},
			},
		}
	case *ledgerpb.CommandInput_RevertTransaction:
		revertCmd := cmd.RevertTransaction
		if revertCmd == nil {
			return nil, fmt.Errorf("revert transaction command is nil")
		}
		if revertCmd.RevertTransaction == nil {
			return nil, fmt.Errorf("revert transaction is nil")
		}

		// Get the log ID for the transaction ID
		logID, err := f.runtimeStore.GetLogIDForTransactionID(ctx, revertCmd.TransactionId)
		if err != nil {
			return nil, fmt.Errorf("getting log ID for transaction %d: %w", revertCmd.TransactionId, err)
		}
		if logID == 0 {
			return nil, fmt.Errorf("transaction %d not found", revertCmd.TransactionId)
		}

		// Get the log containing the original transaction
		originalLog, err := f.runtimeStore.GetLogByID(ctx, logID)
		if err != nil {
			return nil, fmt.Errorf("getting log %d: %w", logID, err)
		}
		if originalLog == nil {
			return nil, fmt.Errorf("log %d not found", logID)
		}

		// Extract the original transaction from the log
		var originalTx *ledgerpb.Transaction
		switch payload := originalLog.Data.Payload.(type) {
		case *ledgerpb.LogPayload_CreatedTransaction:
			if payload.CreatedTransaction == nil || payload.CreatedTransaction.Transaction == nil {
				return nil, fmt.Errorf("invalid log payload: missing transaction")
			}
			originalTx = payload.CreatedTransaction.Transaction
		case *ledgerpb.LogPayload_RevertedTransaction:
			return nil, fmt.Errorf("transaction %d is already reverted", revertCmd.TransactionId)
		default:
			return nil, fmt.Errorf("log %d does not contain a transaction", logID)
		}

		// Check if transaction is already reverted
		if originalTx.Reverted || originalTx.RevertedAt != nil {
			return nil, fmt.Errorf("transaction %d is already reverted", revertCmd.TransactionId)
		}

		// Use the revert transaction provided in the command
		revertTx := proto.Clone(revertCmd.RevertTransaction).(*ledgerpb.Transaction)
		
		// Set timestamp if not provided (use current date)
		if revertTx.Timestamp == nil || revertTx.Timestamp.Data == 0 {
			revertTx.Timestamp = raftCommand.Date
		}

		// Assign transaction ID and timestamps
		f.state.LastTransactionId++
		revertTx.Id = f.state.LastTransactionId
		revertTx.InsertedAt = raftCommand.Date
		revertTx.UpdatedAt = raftCommand.Date

		// Mark original transaction as reverted
		revertedTxCopy := proto.Clone(originalTx).(*ledgerpb.Transaction)
		revertedTxCopy.Reverted = true
		revertedTxCopy.RevertedAt = raftCommand.Date

		logPayload = &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_RevertedTransaction{
				RevertedTransaction: &ledgerpb.RevertedTransaction{
					RevertedTransaction: revertedTxCopy,
					RevertTransaction:   revertTx,
				},
			},
		}
	default:
		return nil, fmt.Errorf("unhandled command type: %T", cmd)
	}

	f.state.LastLogId++
	log := &ledgerpb.Log{
		Data:        logPayload,
		Date:        raftCommand.Date,
		Idempotency: createCmd.Idempotency,
		Id:          f.state.LastLogId,
	}

	return log, nil
}

func (f *FSM) ApplyEntries(ctx context.Context, commands ...*raft.Command) ([]raft.ApplyResult, error) {
	// Assume the majority of commands are logs insertion while allocating
	ret := make([]raft.ApplyResult, 0, len(commands))
	logs := make([]*ledgerpb.Log, 0, len(commands))
	for _, command := range commands {
		switch command.Type {
		case raft.CommandType_InsertLog:
			log, err := f.processCreateLog(ctx, command)
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
	logs = collectionutils.Filter(logs, func(log *ledgerpb.Log) bool {
		return log.Id > f.state.LastAppliedId
	})

	if len(logs) > 0 {

		now := time.Now()
		f.state.LastAppliedId = logs[len(logs)-1].Id

		if err := f.runtimeStore.InsertLogs(ctx, logs...); err != nil {
			return nil, fmt.Errorf("writing logs to runtime store: %w", err)
		}

		f.logger.
			WithFields(map[string]any{
				"count":   len(logs),
				"latency": time.Since(now),
			}).
			Debugf("Runtime store updated via FSM")
	}

	return ret, nil
}

func (f *FSM) CreateSnapshot(ctx context.Context) ([]byte, error) {
	f.mu.RLock()
	// todo: create dedicated snapshot struct as LedgerState container LastAppliedId which is not suitable for the snapshot
	snapshotData := &ledgerpb.LedgerState{
		LedgerInfo:        f.state.LedgerInfo,
		LastLogId:         f.state.LastLogId,
		LastTransactionId: f.state.LastTransactionId,
	}
	f.mu.RUnlock()

	// Marshal to protobuf
	data, err := proto.Marshal(snapshotData)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot data: %w", err)
	}

	// todo: create snapshot at store level

	return data, nil
}

// RestoreSnapshot restores the ledger FSM from a remote snapshot
func (f *FSM) RestoreSnapshot(ctx context.Context, leader uint64, snapshot raftpb.Snapshot) error {
	var snapshotData ledgerpb.LedgerState

	if err := proto.Unmarshal(snapshot.Data, &snapshotData); err != nil {
		panic(err)
	}

	f.logger.WithFields(map[string]any{
		"snapshot": snapshot,
	}).Debugf("Restoring snapshot...")

	f.mu.Lock()
	f.state = &snapshotData
	f.mu.Unlock()

	var err error
	f.state.LastAppliedId, err = f.runtimeStore.GetLastProcessedLogID(ctx)
	if err != nil {
		return err
	}

	// todo: need to check the real state of the log history in the runtime store
	if f.state.LastAppliedId < snapshotData.LastLogId {
		f.logger.WithFields(map[string]any{
			"snapshotLogID": snapshotData.LastLogId,
			"storeLogID":    f.state.LastAppliedId,
		}).Infof("Runtime store log history is ahead of snapshot, catching up logs")

		cursor, err := f.logReaderProvider(leader).GetAllLogs(ctx, f.state.LastAppliedId, snapshotData.LastLogId)
		if err != nil {
			return fmt.Errorf("getting logs from reader for catch-up: %w", err)
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
				return fmt.Errorf("reading log during catch-up: %w", err)
			}

			f.logger.Debugf("Catching up log %d", log.Id)

			logsToWrite = append(logsToWrite, log)
		}

		// Write all collected logs to the writer
		if len(logsToWrite) > 0 {
			if err := f.runtimeStore.InsertLogs(ctx, logsToWrite...); err != nil {
				return fmt.Errorf("writing catch-up logs to runtime store: %w", err)
			}

			f.logger.WithFields(map[string]any{
				"logsWritten": len(logsToWrite),
			}).Infof("Caught up logs from reader to writer")
		}
	}

	f.logger.WithFields(map[string]any{
		"ledger": f.state.LedgerInfo.Name,
	}).Infof("Ledger FSM restored from snapshot")

	return nil
}
