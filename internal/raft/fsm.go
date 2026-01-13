package raft

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// FSM implements the raft.FSM interface
type defaultFSM struct {
	mu           sync.RWMutex    // Protects access to state
	state        *ledgerpb.State // FSM state
	logger       logging.Logger
	runtimeStore store.Runtime
	transport    *GRPCTransport
}

func newFSM(logger logging.Logger, runtimeStore store.Runtime, transport *GRPCTransport) *defaultFSM {
	return &defaultFSM{
		state: &ledgerpb.State{
			Ledgers: make(map[string]*ledgerpb.LedgerState),
		},
		logger:       logger,
		runtimeStore: runtimeStore,
		transport:    transport,
	}
}

// GetState returns a copy of the FSM state
func (fsm *defaultFSM) GetState() *ledgerpb.State {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	return proto.CloneOf(fsm.state)
}

// Note: With etcd/raft, we don't have an Apply method on FSM.
// The entries are applied directly in the readyLoop of Node.
// Ledgers and logs are now managed by ledger Raft groups.

// handleCreateLedger handles the create ledger command
func (fsm *defaultFSM) handleCreateLedger(cmd *ledgerpb.Command) (*ledgerpb.LedgerInfo, error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	var createCmd ledgerpb.CreateLedgerCommand
	if err := UnmarshalCommandData(cmd.Data, &createCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal create ledger command")
		return nil, fmt.Errorf("unmarshaling create ledger command: %w", err)
	}

	if _, exists := fsm.state.Ledgers[createCmd.Name]; exists {
		return nil, fmt.Errorf("ledger already exists: %s", createCmd.Name)
	}

	// Create ledger info using protobuf types directly
	ledgerInfo := &ledgerpb.LedgerInfo{
		Name:      createCmd.Name,
		Metadata:  createCmd.Metadata,
		CreatedAt: cmd.Date,
	}
	fsm.state.Ledgers[ledgerInfo.Name] = &ledgerpb.LedgerState{
		LedgerInfo:        ledgerInfo,
		NextLogId:         1,
		NextTransactionId: 1,
	}

	fsm.logger.Infof("Ledger created")
	return ledgerInfo, nil
}

// handleDeleteLedger handles the delete ledger command (hard delete)
func (fsm *defaultFSM) handleDeleteLedger(cmd *ledgerpb.Command) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	var deleteCmd ledgerpb.DeleteLedgerCommand
	if err := UnmarshalCommandData(cmd.Data, &deleteCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal delete ledger command")
		return fmt.Errorf("unmarshaling delete ledger command: %w", err)
	}

	// Check if ledger exists
	_, ok := fsm.state.Ledgers[deleteCmd.Name]
	if !ok {
		return ledgerpb.NewNotFoundError("ledger %s does not exist", deleteCmd.Name)
	}

	delete(fsm.state.Ledgers, deleteCmd.Name)

	return nil
}

// processInsertLog handles the insert log command by building the log entry
func (fsm *defaultFSM) handleCreateLog(ctx context.Context, raftCommand *ledgerpb.Command) (*ledgerpb.Log, error) {
	var createCmd ledgerpb.CreateLogCommand
	if err := UnmarshalCommandData(raftCommand.Data, &createCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal insert log command")
		return nil, err
	}

	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	var logPayload *ledgerpb.LogPayload
	switch cmd := createCmd.Input.Command.(type) {
	case *ledgerpb.CommandInput_AppendTransaction:
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
						Id:         fsm.state.Ledgers[createCmd.Ledger].GetNextTransactionID(),
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
	case *ledgerpb.CommandInput_RevertTransaction: // todo: should not need to read the original data
		revertCmd := cmd.RevertTransaction
		if revertCmd == nil {
			return nil, fmt.Errorf("revert transaction command is nil")
		}
		if revertCmd.RevertTransaction == nil {
			return nil, fmt.Errorf("revert transaction is nil")
		}

		// Get the log ID for the transaction ID
		logID, err := fsm.runtimeStore.GetLogIDForTransactionID(ctx, createCmd.Ledger, revertCmd.TransactionId)
		if err != nil {
			return nil, fmt.Errorf("getting log ID for transaction %d: %w", revertCmd.TransactionId, err)
		}
		if logID == 0 {
			return nil, fmt.Errorf("transaction %d not found", revertCmd.TransactionId)
		}

		// Get the log containing the original transaction
		originalLog, err := fsm.runtimeStore.GetLogByID(ctx, createCmd.Ledger, logID)
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
		revertTx.Id = fsm.state.Ledgers[createCmd.Ledger].GetNextTransactionID()
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

	return &ledgerpb.Log{
		Data:        logPayload,
		Date:        raftCommand.Date,
		Idempotency: createCmd.Idempotency,
		Id:          fsm.state.Ledgers[createCmd.Ledger].GetNextLogID(),
		Ledger:      createCmd.Ledger,
	}, nil
}

func (fsm *defaultFSM) ApplyEntries(ctx context.Context, commands ...*ledgerpb.Command) ([]ApplyResult, error) {
	ret := make([]ApplyResult, 0, len(commands))
	logs := make([]*ledgerpb.Log, 0, len(commands))
	for _, cmd := range commands {
		switch cmd.Type {
		case ledgerpb.CommandType_CreateLedger:
			info, err := fsm.handleCreateLedger(cmd)
			if err != nil {
				ret = append(ret, ApplyResult{
					Error: err,
				})
				continue
			}
			ret = append(ret, ApplyResult{
				Result: info,
			})
		case ledgerpb.CommandType_DeleteLedger:
			ret = append(ret, ApplyResult{
				Error: fsm.handleDeleteLedger(cmd),
			})
		case ledgerpb.CommandType_CreateLog:
			log, err := fsm.handleCreateLog(ctx, cmd)
			if err != nil {
				ret = append(ret, ApplyResult{
					Error: err,
				})
				continue
			}
			ret = append(ret, ApplyResult{
				Result: log,
			})
			logs = append(logs, log)
		default:
			ret = append(ret, ApplyResult{
				Error: fmt.Errorf("unknown command type: %s", cmd.Type),
			})
		}
	}

	logs = collectionutils.Filter(logs, func(log *ledgerpb.Log) bool {
		return log.Id > fsm.state.Ledgers[log.Ledger].LastAppliedLogId
	})

	if len(logs) > 0 {
		// todo: we can probably optimize that
		for _, log := range logs {
			fsm.state.Ledgers[log.Ledger].LastAppliedLogId = log.Id
		}

		now := time.Now()
		if err := fsm.runtimeStore.InsertLogs(ctx, logs...); err != nil {
			return nil, fmt.Errorf("writing logs to runtime store: %w", err)
		}

		fsm.logger.
			WithFields(map[string]any{
				"count":   len(logs),
				"latency": time.Since(now),
			}).
			Debugf("Runtime store updated via FSM")
	}

	return ret, nil
}

// GetLedger returns the ledger node for a given name (including deleted ledgers)
func (fsm *defaultFSM) GetLedger(name string) (*ledgerpb.LedgerInfo, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	ledger, ok := fsm.state.Ledgers[name]
	if !ok {
		return nil, ledgerpb.NewNotFoundError("ledger %s does not exist", name)
	}
	return proto.CloneOf(ledger.LedgerInfo), nil
}

// GetAllLedgers returns all ledgers (including deleted ones)
func (fsm *defaultFSM) GetAllLedgers() map[string]*ledgerpb.LedgerInfo {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	return collectionutils.ConvertMap(fsm.state.Ledgers, func(v *ledgerpb.LedgerState) *ledgerpb.LedgerInfo {
		return proto.CloneOf(v.LedgerInfo)
	})
}

// CreateSnapshot creates a snapshot of the FSM state
func (fsm *defaultFSM) CreateSnapshot(ctx context.Context) ([]byte, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	// todo: create snapshot on storage (pebble)

	return proto.Marshal(fsm.state)
}

// RestoreSnapshot restores the FSM from a snapshot
func (fsm *defaultFSM) RestoreSnapshot(snapshot raftpb.Snapshot) error {
	return proto.Unmarshal(snapshot.Data, fsm.state)
}

func (fsm *defaultFSM) SyncSnapshot(ctx context.Context, leader uint64, snapshot raftpb.Snapshot) error {
	fsm.logger.WithFields(map[string]any{
		"snapshot": snapshot,
	}).Infof("Syncing snapshot...")

	oldState := proto.CloneOf(fsm.state)
	if err := proto.Unmarshal(snapshot.Data, fsm.state); err != nil {
		return fmt.Errorf("unmarshaling snapshot: %w", err)
	}

	for ledgerName, oldLedgerState := range oldState.Ledgers {
		if fsm.state.Ledgers[ledgerName].LastAppliedLogId > oldLedgerState.LastAppliedLogId {
			client := service.NewLedgerGrpcClient(
				ledgerpb.NewLedgerServiceClient(
					fsm.transport.GetPeerConnection(leader),
				),
			)
			logStream, err := client.GetAllLogs(
				ctx,
				ledgerName,
				oldLedgerState.LastAppliedLogId,
				fsm.state.Ledgers[ledgerName].LastAppliedLogId,
			)
			if err != nil {
				return fmt.Errorf("streaming logs from peer %d: %w", leader, err)
			}

			var (
				// todo: flush regularly
				logsToWrite []*ledgerpb.Log
			)

			// Collect all logs that need to be written
			for {
				log, err := logStream.Next(ctx)
				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("reading log during catch-up: %w", err)
				}

				fsm.logger.Debugf("Catching up log %d", log.Id)

				logsToWrite = append(logsToWrite, log)
			}

			// Write all collected logs to the writer
			if len(logsToWrite) > 0 {
				if err := fsm.runtimeStore.InsertLogs(ctx, logsToWrite...); err != nil {
					return fmt.Errorf("writing catch-up logs to runtime store: %w", err)
				}

				fsm.logger.WithFields(map[string]any{
					"logsWritten": len(logsToWrite),
					"leader":      leader,
				}).Infof("Synced logs from leader")
			}
		}
	}

	return nil
}

type ApplyResult struct {
	Result any
	Error  error
}
