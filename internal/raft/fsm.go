package raft

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source fsm.go -destination fsm_generated_test.go -package raft . FSM
type FSM interface {
	CreateSnapshot(ctx context.Context) ([]byte, error)
	SyncSnapshot(ctx context.Context, leader uint64, snapshot raftpb.Snapshot) error
	ApplyEntries(ctx context.Context, entries ...raftpb.Entry) ([]ApplyResult, error)
}

// FSM implements the raft.FSM interface
type defaultFSM struct {
	// todo: don't expose the state, to avoid the lock
	// use the store directly
	mu               sync.RWMutex    // Protects access to state
	state            *ledgerpb.State // FSM state
	logger           logging.Logger
	store            store.Store
	transport        Transport
	lastAppliedIndex uint64
}

func newFSM(logger logging.Logger, store store.Store, transport Transport) (*defaultFSM, error) {
	lastAppliedIndex, err := store.GetLastAppliedIndex()
	if err != nil {
		return nil, err
	}
	return &defaultFSM{
		state: &ledgerpb.State{
			Ledgers:      make(map[uint32]*ledgerpb.LedgerState),
			NextLedgerId: 1,
		},
		logger:           logger,
		store:            store,
		transport:        transport,
		lastAppliedIndex: lastAppliedIndex,
	}, nil
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
func (fsm *defaultFSM) handleCreateLedger(ctx context.Context, batch store.Batch, cmd *ledgerpb.Command) (*ledgerpb.LedgerInfo, error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	var createCmd ledgerpb.CreateLedgerCommand
	if err := UnmarshalCommandData(cmd.Data, &createCmd); err != nil {
		fsm.logger.
			WithFields(map[string]any{"error": err}).
			Errorf("Failed to unmarshal create ledger command")
		return nil, fmt.Errorf("unmarshaling create ledger command: %w", err)
	}

	for _, state := range fsm.state.Ledgers {
		if state.LedgerInfo.Name == createCmd.Name {
			return nil, fmt.Errorf("ledger already exists: %s", createCmd.Name)
		}
	}

	// Assign a numeric ID and increment the counter
	ledgerID := fsm.state.NextLedgerId
	fsm.state.NextLedgerId++

	// Create ledger info using protobuf types directly
	ledgerInfo := &ledgerpb.LedgerInfo{
		Name:      createCmd.Name,
		Metadata:  createCmd.Metadata,
		CreatedAt: cmd.Date,
		Id:        ledgerID,
	}
	fsm.state.Ledgers[ledgerID] = &ledgerpb.LedgerState{
		LedgerInfo:        ledgerInfo,
		NextLogId:         1,
		NextTransactionId: 1,
	}

	fsm.logger.WithFields(map[string]any{
		"ledger": ledgerInfo.Name,
	}).Infof("Ledger created")

	if err := batch.RegisterLedger(ctx, ledgerInfo); err != nil {
		return nil, err
	}

	return ledgerInfo, nil
}

// handleDeleteLedger handles the delete ledger command (hard delete)
func (fsm *defaultFSM) handleDeleteLedger(ctx context.Context, batch store.Batch, cmd *ledgerpb.Command) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	var deleteCmd ledgerpb.DeleteLedgerCommand
	if err := UnmarshalCommandData(cmd.Data, &deleteCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal delete ledger command")
		return fmt.Errorf("unmarshaling delete ledger command: %w", err)
	}

	return fsm.deleteLedger(ctx, batch, deleteCmd.Id)
}

func (fsm *defaultFSM) deleteLedger(ctx context.Context, batch store.Batch, id uint32) error {
	// Check if ledger exists
	_, ok := fsm.state.Ledgers[id]
	if !ok {
		return ledgerpb.NewNotFoundError("ledger %d does not exist", id)
	}

	if err := batch.DeleteLedger(ctx, id); err != nil {
		return fmt.Errorf("deleting ledger from runtime store: %w", err)
	}

	delete(fsm.state.Ledgers, id)

	return nil
}

// processInsertLog handles the insert log command by building the log entry
func (fsm *defaultFSM) createLog(ctx context.Context, raftCommand *ledgerpb.Command) (*ledgerpb.Log, error) {
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
						Id:         fsm.state.Ledgers[createCmd.LedgerId].GetNextTransactionID(),
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
		// Use the revert transaction provided in the command
		revertTx := cmd.RevertTransaction.RevertTransaction

		// Set timestamp if not provided (use current date)
		if revertTx.Timestamp == nil || revertTx.Timestamp.Data == 0 {
			revertTx.Timestamp = raftCommand.Date
		}

		// Assign transaction ID and timestamps
		revertTx.Id = fsm.state.Ledgers[createCmd.LedgerId].GetNextTransactionID()
		revertTx.InsertedAt = raftCommand.Date
		revertTx.UpdatedAt = raftCommand.Date

		logPayload = &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_RevertedTransaction{
				RevertedTransaction: &ledgerpb.RevertedTransaction{
					RevertedTransactionId: cmd.RevertTransaction.TransactionId,
					RevertTransaction:     revertTx,
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
		Id:          fsm.state.Ledgers[createCmd.LedgerId].GetNextLogID(),
		LedgerId:    createCmd.LedgerId,
	}, nil
}

func (fsm *defaultFSM) ApplyEntries(ctx context.Context, entries ...raftpb.Entry) ([]ApplyResult, error) {

	batch := fsm.store.NewBatch(entries[len(entries)-1].Index)
	defer func() {
		_ = batch.Cancel(ctx)
	}()
	cmd := &ledgerpb.Command{}

	ret := make([]ApplyResult, 0, len(entries))
	logs := make([]*ledgerpb.Log, 0, len(entries))
	for _, entry := range entries {
		if entry.Index <= fsm.lastAppliedIndex {
			ret = append(ret, ApplyResult{})
			continue
		}
		// Well, in a perfect world, we wouldn't have to check this
		// But, as error is human, this adds a small safeguard to avoid corrupting the runtime store
		if entry.Index > fsm.lastAppliedIndex+1 {
			return nil, fmt.Errorf("invalid index, got %d, expected %d", entry.Index, fsm.lastAppliedIndex+1)
		}
		fsm.lastAppliedIndex++

		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 { // Ignore conf changes
			continue
		}

		if err := proto.Unmarshal(entry.Data, cmd); err != nil {
			return nil, err
		}

		switch cmd.Type {
		case ledgerpb.CommandType_CreateLedger:
			info, err := fsm.handleCreateLedger(ctx, batch, cmd)
			if err != nil {
				ret = append(ret, ApplyResult{
					Error:     err,
					CommandID: cmd.Id,
				})
				continue
			}
			ret = append(ret, ApplyResult{
				Result:    info,
				CommandID: cmd.Id,
			})
		case ledgerpb.CommandType_DeleteLedger:
			ret = append(ret, ApplyResult{
				Error:     fsm.handleDeleteLedger(ctx, batch, cmd),
				CommandID: cmd.Id,
			})
		case ledgerpb.CommandType_CreateLog:
			log, err := fsm.createLog(ctx, cmd)
			if err != nil {
				ret = append(ret, ApplyResult{
					Error:     err,
					CommandID: cmd.Id,
				})
				continue
			}
			ret = append(ret, ApplyResult{
				Result:    log,
				CommandID: cmd.Id,
			})
			logs = append(logs, log)

			if err := fsm.projectLog(ctx, batch, log); err != nil {
				return nil, fmt.Errorf("projecting log %d: %w", log.Id, err)
			}

			if err := batch.AppendLogs(ctx, log); err != nil {
				return nil, fmt.Errorf("writing log to runtime store: %w", err)
			}
		default:
			ret = append(ret, ApplyResult{
				Error:     fmt.Errorf("unknown command type: %s", cmd.Type),
				CommandID: cmd.Id,
			})
		}
	}

	if err := batch.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing batch: %w", err)
	}

	return ret, nil
}

// GetLedgerByName returns the ledger node for a given name (including deleted ledgers)
func (fsm *defaultFSM) GetLedgerByName(name string) (*ledgerpb.LedgerInfo, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	for _, state := range fsm.state.Ledgers {
		if state.LedgerInfo.Name == name {
			return proto.CloneOf(state.LedgerInfo), nil
		}
	}

	return nil, ledgerpb.NewNotFoundError("ledger %s does not exist", name)
}

func (fsm *defaultFSM) GetLedgerInfo(id uint32) (*ledgerpb.LedgerInfo, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	ledger, ok := fsm.state.Ledgers[id]
	if !ok {
		return nil, ledgerpb.NewNotFoundError("ledger %d does not exist", id)
	}

	return proto.CloneOf(ledger.LedgerInfo), nil
}

// GetAllLedgers returns all ledgers (including deleted ones)
func (fsm *defaultFSM) GetAllLedgers() map[string]*ledgerpb.LedgerInfo {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	ret := make(map[string]*ledgerpb.LedgerInfo, len(fsm.state.Ledgers))
	for _, state := range fsm.state.Ledgers {
		ret[state.LedgerInfo.Name] = proto.CloneOf(state.LedgerInfo)
	}

	return ret
}

// CreateSnapshot creates a snapshot of the FSM state
func (fsm *defaultFSM) CreateSnapshot(ctx context.Context) ([]byte, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	if err := fsm.store.CreateSnapshot(ctx); err != nil {
		return nil, fmt.Errorf("creating snapshot: %w", err)
	}

	return proto.Marshal(fsm.state)
}

// RestoreSnapshot restores the FSM from a snapshot
// todo: should be able to sync logs from peers at this point, since we can recover from a valid snapshot but with incomplete db
// ie if the code ```
//
//	 if err := fsm.store.AppendLogs(ctx, snapshot.Metadata.Index); err != nil {
//			return fmt.Errorf("writing catch-up logs to runtime store: %w", err)
//		}
//
//		if err := fsm.store.CreateSnapshot(ctx); err != nil {
//			return fmt.Errorf("creating snapshot: %w", err)
//		}
//
// ``` fails between both instructions
func (fsm *defaultFSM) RestoreSnapshot(snapshot raftpb.Snapshot) error {
	return proto.Unmarshal(snapshot.Data, fsm.state)
}

func (fsm *defaultFSM) SyncSnapshot(ctx context.Context, leader uint64, snapshot raftpb.Snapshot) error {
	oldState := proto.CloneOf(fsm.state)
	if err := proto.Unmarshal(snapshot.Data, fsm.state); err != nil {
		return fmt.Errorf("unmarshaling snapshot: %w", err)
	}
	fsm.logger.WithFields(map[string]any{
		"state": fsm.state,
	}).Infof("Syncing snapshot...")

	batch := fsm.store.NewBatch(snapshot.Metadata.Index)
	defer func() {
		_ = batch.Cancel(ctx)
	}()

	// todo: add soft delete
	for ledgerID := range oldState.Ledgers {
		_, ok := fsm.state.Ledgers[ledgerID]
		if !ok {
			if err := fsm.deleteLedger(ctx, batch, ledgerID); err != nil {
				return fmt.Errorf("deleting ledger %d from runtime store: %w", ledgerID, err)
			}
		}
	}

	for ledgerID, ledgerState := range fsm.state.Ledgers {
		lastLogID, err := fsm.store.GetLastLogID(ctx, ledgerID)
		if err != nil {
			return err
		}

		if lastLogID < ledgerState.NextLogId-1 {
			fsm.logger.WithFields(map[string]any{
				"ledgerID":     ledgerID,
				"lastLogID":    lastLogID,
				"newNextLogId": ledgerState.NextLogId,
			}).Infof("Syncing logs from leader")
			client := service.NewLedgerGrpcClient(
				ledgerpb.NewLedgerServiceClient(
					fsm.transport.GetPeerConnection(leader),
				),
			)
			logStream, err := client.GetAllLogs(ctx, ledgerID, lastLogID, ledgerState.NextLogId)
			if err != nil {
				return fmt.Errorf("streaming logs from peer %d: %w", leader, err)
			}

			count := 0
			for {
				log, err := logStream.Next(ctx)
				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("reading log during catch-up: %w", err)
				}
				count++

				if err := fsm.projectLog(ctx, batch, log); err != nil {
					return fmt.Errorf("projecting log %d: %w", log.Id, err)
				}

				if err := batch.AppendLogs(ctx, log); err != nil {
					return fmt.Errorf("writing catch-up logs to runtime store: %w", err)
				}
			}

			fsm.logger.WithFields(map[string]any{
				"logsWritten": count,
				"leader":      leader,
			}).Infof("Synced logs from leader")
		}
	}

	if err := batch.Commit(ctx); err != nil {
		return fmt.Errorf("committing batch: %w", err)
	}

	fsm.lastAppliedIndex = snapshot.Metadata.Index

	if err := fsm.store.CreateSnapshot(ctx); err != nil {
		return fmt.Errorf("creating snapshot: %w", err)
	}

	return nil
}

func (fsm *defaultFSM) projectLog(ctx context.Context, batch store.Batch, log *ledgerpb.Log) error {
	projectTransaction := func(ctx context.Context, batch store.Batch, tx *ledgerpb.Transaction) error {
		for _, posting := range tx.Postings {
			if err := batch.AppendBalanceDiff(ctx, log.LedgerId, posting.Source, posting.Asset, new(big.Int).Neg(posting.Amount.Value())); err != nil {
				return fmt.Errorf("appending balance diff for posting %s: %w", posting.String(), err)
			}
			if err := batch.AppendBalanceDiff(ctx, log.LedgerId, posting.Destination, posting.Asset, posting.Amount.Value()); err != nil {
				return fmt.Errorf("appending balance diff for posting %s: %w", posting.String(), err)
			}
		}

		return nil
	}

	switch payload := log.Data.Payload.(type) {
	case *ledgerpb.LogPayload_CreatedTransaction:
		if err := projectTransaction(ctx, batch, payload.CreatedTransaction.Transaction); err != nil {
			return fmt.Errorf("projecting transaction %d: %w", payload.CreatedTransaction.Transaction.Id, err)
		}
		if err := batch.StoreTransactionID(ctx, log.LedgerId, payload.CreatedTransaction.Transaction.Id, log.Id); err != nil {
			return err
		}
		if payload.CreatedTransaction.AccountMetadata != nil {
			for account, metadata := range payload.CreatedTransaction.AccountMetadata {
				err := batch.SaveAccountMetadata(ctx, log.LedgerId, account, metadata)
				if err != nil {
					return err
				}
			}
		}

	case *ledgerpb.LogPayload_RevertedTransaction:
		if err := projectTransaction(ctx, batch, payload.RevertedTransaction.RevertTransaction); err != nil {
			return fmt.Errorf("projecting transaction %d: %w", payload.RevertedTransaction.RevertTransaction.Id, err)
		}
		if err := batch.StoreRevertedTransactionID(ctx, log.LedgerId, payload.RevertedTransaction.RevertedTransactionId, log.Id); err != nil {
			return err
		}
	case *ledgerpb.LogPayload_SavedMetadata:
		if account := payload.SavedMetadata.Target.GetAccount(); account != nil {
			if err := batch.SaveAccountMetadata(ctx, log.LedgerId, account.Addr, payload.SavedMetadata.Metadata); err != nil {
				return err
			}
		}
	case *ledgerpb.LogPayload_DeletedMetadata:
		if account := payload.DeletedMetadata.Target.GetAccount(); account != nil {
			if err := batch.DeleteAccountMetadata(ctx, log.LedgerId, account.Addr, []string{
				payload.DeletedMetadata.Key,
			}); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unhandled log payload type: %T", payload)
	}

	return nil
}

type ApplyResult struct {
	CommandID uint64
	Result    any
	Error     error
}
