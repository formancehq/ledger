package ledger

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"sync"

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

type LogWriterWithMetrics interface {
	service.LogWriter
	service.MetricsAware
}

type LogStoreWithMetrics interface {
	service.LogStore
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
	logs              []*ledgerpb.Log
	logWriter         LogWriterWithMetrics
}

// newFSM creates a new ledger FSM
func newFSM(
	logger logging.Logger,
	logWriter LogWriterWithMetrics,
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
		logWriter:         logWriter,
		logReaderProvider: logReaderProvider,
	}
}

// GetState returns a copy of the FSM state
func (f *FSM) GetState() *ledgerpb.LedgerState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	rawLogWriterMetrics, err := json.Marshal(f.logWriter.Metrics())
	if err != nil {
		panic(err)
	}
	mapLogWriterMetrics := make(map[string]interface{})
	if err := json.Unmarshal(rawLogWriterMetrics, &mapLogWriterMetrics); err != nil {
		panic(err)
	}

	logStoreMetrics, err := structpb.NewStruct(mapLogWriterMetrics)
	if err != nil {
		panic(err)
	}

	rawRuntimeStoreMetrics, err := json.Marshal(f.runtimeStore.Metrics())
	if err != nil {
		panic(err)
	}
	mapRuntimeStoreMetrics := make(map[string]interface{})
	if err := json.Unmarshal(rawRuntimeStoreMetrics, &mapRuntimeStoreMetrics); err != nil {
		panic(err)
	}

	runtimeStoreMetrics, err := structpb.NewStruct(mapRuntimeStoreMetrics)
	if err != nil {
		panic(err)
	}

	return &ledgerpb.LedgerState{
		LedgerInfo:          f.state.LedgerInfo,
		LastLogId:           f.state.LastLogId,
		LogStoreMetrics:     logStoreMetrics,
		RuntimeStoreMetrics: runtimeStoreMetrics,
	}
}

// processInsertLog handles the insert log command by storing the log in memory
func (f *FSM) processCreateLog(raftCommand *raft.Command) (*ledgerpb.Log, error) {
	var createCmd CreateLogCommand
	if err := UnmarshalCommandData(raftCommand.Data, &createCmd); err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal insert log command")
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.state.LastLogId++

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
	default:
		return nil, fmt.Errorf("unhandled command type: %T")
	}

	log := &ledgerpb.Log{
		Data:        logPayload,
		Date:        raftCommand.Date,
		Idempotency: createCmd.Idempotency,
		Id:          f.state.LastLogId,
	}
	f.logs = append(f.logs, log)

	return log, nil
}

func (f *FSM) ApplyEntries(ctx context.Context, commands ...*raft.Command) ([]raft.ApplyResult, error) {
	// Assume the majority of commands are logs insertion while allocating
	ret := make([]raft.ApplyResult, 0, len(commands))
	logs := make([]*ledgerpb.Log, 0, len(commands))
	for _, command := range commands {
		switch command.Type {
		case raft.CommandType_InsertLog:
			log, err := f.processCreateLog(command)
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
		now := time.Now()

		// Update runtime store (balances and metadata) using batch operations
		if err := f.updateRuntimeStore(ctx, logs); err != nil {
			return nil, fmt.Errorf("updating runtime store: %w", err)
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

// updateRuntimeStore aggregates logs and updates runtime store using Update()
func (f *FSM) updateRuntimeStore(ctx context.Context, logs []*ledgerpb.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// Convert logs to RuntimeUpdate using the shared function
	update, err := service.LogsToRuntimeUpdate(logs)
	if err != nil {
		return fmt.Errorf("converting logs to runtime update: %w", err)
	}

	// Apply all updates atomically
	if err := f.runtimeStore.Update(ctx, update); err != nil {
		return fmt.Errorf("updating runtime store: %w", err)
	}

	return nil
}

func (f *FSM) CreateSnapshot(ctx context.Context) ([]byte, error) {
	f.mu.RLock()
	snapshotData := &ledgerpb.LedgerState{
		LedgerInfo:        f.state.LedgerInfo,
		LastLogId:         f.state.LastLogId,
		LastTransactionId: f.state.LastTransactionId,
	}
	logs := f.logs
	f.mu.RUnlock()

	err := f.logWriter.InsertLogs(ctx, logs...)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	f.logs = f.logs[len(logs):]
	f.mu.Unlock()

	// Marshal to protobuf
	data, err := proto.Marshal(snapshotData)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot data: %w", err)
	}

	return data, nil
}

// RestoreSnapshot restores the ledger FSM from a remote snapshot
func (f *FSM) RestoreSnapshot(ctx context.Context, leader uint64, snapshot raftpb.Snapshot) error {
	var snapshotData ledgerpb.LedgerState

	if err := proto.Unmarshal(snapshot.Data, &snapshotData); err != nil {
		panic(err)
	}

	f.mu.Lock()
	f.state = &snapshotData
	f.mu.Unlock()

	// todo: we need to restore both stores from the snapshot
	lastProcessedLogID, err := f.runtimeStore.GetLastProcessedLogID(ctx)
	if err != nil {
		return err
	}

	// todo: need to check the real state of the log store
	if lastProcessedLogID < snapshotData.LastLogId {
		f.logger.WithFields(map[string]any{
			"snapshotLogID": snapshotData.LastLogId,
			"storeLogID":    lastProcessedLogID,
		}).Infof("Log store is ahead of snapshot, catching up logs")

		cursor, err := f.logReaderProvider(leader).GetAllLogs(ctx, lastProcessedLogID, snapshotData.LastLogId)
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
			// Write logs to log store
			if err := f.logWriter.InsertLogs(ctx, logsToWrite...); err != nil {
				return fmt.Errorf("writing catch-up logs to log store: %w", err)
			}

			// Update runtime store (balances and metadata) using batch operations
			if err := f.updateRuntimeStore(ctx, logsToWrite); err != nil {
				return fmt.Errorf("updating runtime store during catch-up: %w", err)
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
