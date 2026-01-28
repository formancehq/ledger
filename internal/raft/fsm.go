package raft

import (
	"context"
	"fmt"
	"io"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type FSM struct {
	// todo: don't expose the state, to avoid the lock
	// use the store directly
	state     *ledgerpb.State // FSM state
	logger    logging.Logger
	store     store.Store
	transport Transport

	storeLastAppliedIndex uint64
	snapshotIndex         uint64
}

func newFSM(logger logging.Logger, store store.Store, transport Transport) (*FSM, error) {
	lastAppliedIndex, err := store.GetLastAppliedIndex()
	if err != nil {
		return nil, err
	}
	return &FSM{
		state: &ledgerpb.State{
			Ledgers:      make(map[uint32]*ledgerpb.LedgerState),
			NextLedgerId: 1,
		},
		logger:                logger,
		store:                 store,
		transport:             transport,
		storeLastAppliedIndex: lastAppliedIndex,
	}, nil
}

// GetState returns a copy of the FSM state
func (fsm *FSM) GetState() *ledgerpb.State {
	return proto.CloneOf(fsm.state)
}

// Note: With etcd/raft, we don't have an Apply method on FSM.
// The entries are applied directly in the readyLoop of Node.
// Ledgers and logs are now managed by ledger Raft groups.

// handleCreateLedger handles the create ledger action
func (fsm *FSM) handleCreateLedger(ctx context.Context, batch store.Batch, action *ledgerpb.Action, cmdDate *ledgerpb.Timestamp) (*ledgerpb.LedgerInfo, error) {
	var createCmd ledgerpb.CreateLedgerCommand
	if err := UnmarshalCommandData(action.Data, &createCmd); err != nil {
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
		CreatedAt: cmdDate,
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

// handleDeleteLedger handles the delete ledger action (hard delete)
func (fsm *FSM) handleDeleteLedger(ctx context.Context, batch store.Batch, action *ledgerpb.Action) error {
	var deleteCmd ledgerpb.DeleteLedgerCommand
	if err := UnmarshalCommandData(action.Data, &deleteCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal delete ledger command")
		return fmt.Errorf("unmarshaling delete ledger command: %w", err)
	}

	return fsm.deleteLedger(ctx, batch, deleteCmd.Id)
}

func (fsm *FSM) deleteLedger(ctx context.Context, batch store.Batch, id uint32) error {
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

// createLog handles the insert log action by building the log entry
func (fsm *FSM) createLog(ctx context.Context, action *ledgerpb.Action, cmdDate *ledgerpb.Timestamp) (*ledgerpb.Log, error) {
	var createCmd ledgerpb.CreateLogCommand
	if err := UnmarshalCommandData(action.Data, &createCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal insert log command")
		return nil, err
	}

	var logPayload *ledgerpb.LogPayload
	switch cmd := createCmd.Input.Command.(type) {
	case *ledgerpb.CommandInput_AppendTransaction:
		timestamp := cmd.AppendTransaction.Timestamp
		if timestamp == nil || timestamp.Data == 0 {
			timestamp = cmdDate
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
						InsertedAt: cmdDate,
						UpdatedAt:  cmdDate,
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
			revertTx.Timestamp = cmdDate
		}

		// Assign transaction ID and timestamps
		revertTx.Id = fsm.state.Ledgers[createCmd.LedgerId].GetNextTransactionID()
		revertTx.InsertedAt = cmdDate
		revertTx.UpdatedAt = cmdDate

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
		Date:        cmdDate,
		Idempotency: createCmd.Idempotency,
		Id:          fsm.state.Ledgers[createCmd.LedgerId].GetNextLogID(),
		LedgerId:    createCmd.LedgerId,
	}, nil
}

func (fsm *FSM) ApplyEntries(ctx context.Context, entries ...raftpb.Entry) ([]ApplyResult, error) {

	if fsm.snapshotIndex > fsm.storeLastAppliedIndex {
		return nil, fmt.Errorf("last snapshot index is %d, expecting lower than %d, node out of sync", fsm.snapshotIndex, fsm.storeLastAppliedIndex)
	}

	batch := fsm.store.NewBatch(entries[len(entries)-1].Index)
	defer func() {
		_ = batch.Cancel(ctx)
	}()
	cmd := &ledgerpb.Command{}

	ret := make([]ApplyResult, 0, len(entries))
	for _, entry := range entries {
		if entry.Index <= fsm.storeLastAppliedIndex {
			ret = append(ret, ApplyResult{})
			continue
		}
		// Well, in a perfect world, we wouldn't have to check this
		// But, as error is human, this adds a small safeguard to avoid corrupting the runtime store
		if entry.Index > fsm.storeLastAppliedIndex+1 {
			return nil, fmt.Errorf("invalid index, got %d, expected %d", entry.Index, fsm.storeLastAppliedIndex+1)
		}
		fsm.storeLastAppliedIndex++

		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 { // Ignore conf changes
			continue
		}

		if err := proto.Unmarshal(entry.Data, cmd); err != nil {
			return nil, err
		}

		// Process all actions in the command
		result, err := fsm.applyCommand(ctx, batch, cmd)
		if err != nil {
			return nil, err
		}
		ret = append(ret, result)
	}

	if err := batch.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing batch: %w", err)
	}

	return ret, nil
}

// applyCommand processes all actions in a command atomically
func (fsm *FSM) applyCommand(ctx context.Context, batch store.Batch, cmd *ledgerpb.Command) (ApplyResult, error) {
	var results []any

	for _, action := range cmd.Actions {
		result, err := fsm.applyAction(ctx, batch, action, cmd.Date)
		if err != nil {
			return ApplyResult{
				Error:     err,
				CommandID: cmd.Id,
			}, nil
		}
		if result != nil {
			results = append(results, result)
		}
	}

	return ApplyResult{
		Result:    results,
		CommandID: cmd.Id,
	}, nil
}

// applyAction processes a single action
func (fsm *FSM) applyAction(ctx context.Context, batch store.Batch, action *ledgerpb.Action, cmdDate *ledgerpb.Timestamp) (any, error) {
	switch action.ActionType {
	case ledgerpb.ActionType_CreateLedger:
		return fsm.handleCreateLedger(ctx, batch, action, cmdDate)

	case ledgerpb.ActionType_DeleteLedger:
		return nil, fsm.handleDeleteLedger(ctx, batch, action)

	case ledgerpb.ActionType_CreateLog:
		log, err := fsm.createLog(ctx, action, cmdDate)
		if err != nil {
			return nil, err
		}

		if err := fsm.projectLog(ctx, batch, log); err != nil {
			return nil, fmt.Errorf("projecting log %d: %w", log.Id, err)
		}

		if err := batch.AppendLogs(ctx, log); err != nil {
			return nil, fmt.Errorf("writing log to runtime store: %w", err)
		}

		return log, nil

	default:
		return nil, fmt.Errorf("unknown action type: %s", action.ActionType)
	}
}

// GetLedgerByName returns the ledger node for a given name (including deleted ledgers)
func (fsm *FSM) GetLedgerByName(name string) (*ledgerpb.LedgerInfo, error) {
	for _, state := range fsm.state.Ledgers {
		if state.LedgerInfo.Name == name {
			return proto.CloneOf(state.LedgerInfo), nil
		}
	}

	return nil, ledgerpb.NewNotFoundError("ledger %s does not exist", name)
}

func (fsm *FSM) GetLedgerInfo(id uint32) (*ledgerpb.LedgerInfo, error) {
	ledger, ok := fsm.state.Ledgers[id]
	if !ok {
		return nil, ledgerpb.NewNotFoundError("ledger %d does not exist", id)
	}

	return proto.CloneOf(ledger.LedgerInfo), nil
}

// GetAllLedgers returns all ledgers (including deleted ones)
func (fsm *FSM) GetAllLedgers() map[string]*ledgerpb.LedgerInfo {
	ret := make(map[string]*ledgerpb.LedgerInfo, len(fsm.state.Ledgers))
	for _, state := range fsm.state.Ledgers {
		ret[state.LedgerInfo.Name] = proto.CloneOf(state.LedgerInfo)
	}

	return ret
}

// CreateSnapshot creates a snapshot of the FSM state
func (fsm *FSM) CreateSnapshot(ctx context.Context) ([]byte, error) {
	if err := fsm.store.CreateSnapshot(ctx); err != nil {
		return nil, fmt.Errorf("creating snapshot: %w", err)
	}

	return proto.Marshal(fsm.state)
}

func (fsm *FSM) InstallSnapshot(ctx context.Context, snapshot raftpb.Snapshot) error {
	fsm.snapshotIndex = snapshot.Metadata.Index
	return proto.Unmarshal(snapshot.Data, fsm.state)
}

func (fsm *FSM) projectLog(ctx context.Context, batch store.Batch, log *ledgerpb.Log) error {
	projectTransaction := func(ctx context.Context, batch store.Batch, tx *ledgerpb.Transaction) error {
		for _, posting := range tx.Postings {
			if err := batch.AppendBalanceDiff(ctx, log.LedgerId, posting.Source, posting.Asset, posting.Amount.Neg(), log.Id); err != nil {
				return fmt.Errorf("appending balance diff for posting %s: %w", posting.String(), err)
			}
			if err := batch.AppendBalanceDiff(ctx, log.LedgerId, posting.Destination, posting.Asset, posting.Amount, log.Id); err != nil {
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

func (fsm *FSM) SynchronizeWithLeader(ctx context.Context, logStreamer LogStreamer) (uint64, error) {
	ledgers, err := fsm.store.ListLedgers(ctx)
	if err != nil {
		return 0, err
	}

	batch := fsm.store.NewBatch(fsm.snapshotIndex)
	defer func() {
		_ = batch.Cancel(ctx)
	}()

deleteOldLedgers:
	for _, ledger := range ledgers {
		for _, ledgerState := range fsm.state.Ledgers {
			if ledger.Id == ledgerState.LedgerInfo.Id {
				continue deleteOldLedgers
			}
		}

		if err := batch.DeleteLedger(ctx, ledger.Id); err != nil {
			return 0, fmt.Errorf("deleting ledger from store: %w", err)
		}
	}

createNewLedgers:
	for _, inMemoryLedger := range fsm.state.Ledgers {
		for _, ledger := range ledgers {
			if inMemoryLedger.LedgerInfo.Id == ledger.Id {
				continue createNewLedgers
			}
		}

		if err := batch.RegisterLedger(ctx, fsm.state.Ledgers[inMemoryLedger.LedgerInfo.Id].LedgerInfo); err != nil {
			return 0, fmt.Errorf("registering ledger in store: %w", err)
		}
	}

	for ledgerID, ledgerState := range fsm.state.Ledgers {
		lastLogID, err := fsm.store.GetLastLogID(ctx, ledgerID)
		if err != nil {
			return 0, err
		}
		if lastLogID < ledgerState.NextLogId-1 {
			fsm.logger.WithFields(map[string]any{
				"ledgerID":     ledgerID,
				"lastLogID":    lastLogID,
				"newNextLogId": ledgerState.NextLogId,
			}).Infof("Syncing logs from leader")
			logStream, err := logStreamer.GetAllLogs(ctx, ledgerID, lastLogID, ledgerState.NextLogId-1)
			if err != nil {
				return 0, fmt.Errorf("streaming logs from peer: %w", err)
			}

			count := 0
			for {
				log, err := logStream.Next(ctx)
				if err != nil {
					if err == io.EOF {
						break
					}
					return 0, fmt.Errorf("reading log during catch-up: %w", err)
				}
				count++

				if err := fsm.projectLog(ctx, batch, log); err != nil {
					return 0, fmt.Errorf("projecting log %d: %w", log.Id, err)
				}

				if err := batch.AppendLogs(ctx, log); err != nil {
					return 0, fmt.Errorf("writing catch-up logs to runtime store: %w", err)
				}
			}

			fsm.logger.WithFields(map[string]any{
				"logsWritten": count,
			}).Infof("Synced logs from leader")
		}
	}

	if err := batch.Commit(ctx); err != nil {
		return 0, fmt.Errorf("committing batch: %w", err)
	}

	fsm.storeLastAppliedIndex = fsm.snapshotIndex

	return fsm.snapshotIndex, nil
}

func (fsm *FSM) IsStoreUpToDate(ctx context.Context) (bool, error) {
	return fsm.storeLastAppliedIndex >= fsm.snapshotIndex, nil
}

type ApplyResult struct {
	CommandID uint64
	Result    any
	Error     error
}
