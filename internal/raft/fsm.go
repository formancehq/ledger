package raft

import (
	"context"
	"fmt"
	"io"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
)

type FSM struct {
	// todo: don't expose the state, to avoid the lock
	// use the store directly
	state     *raftcmdpb.State // FSM state
	logger    logging.Logger
	store     store.Store
	transport Transport

	storeLastAppliedIndex uint64
	snapshotIndex         uint64

	// Metrics
	logsAppendedCounter metric.Int64Counter
}

func newFSM(logger logging.Logger, store store.Store, transport Transport, meter metric.Meter) (*FSM, error) {
	lastAppliedIndex, err := store.GetLastAppliedIndex()
	if err != nil {
		return nil, err
	}

	logsAppendedCounter, err := meter.Int64Counter(
		"raft.fsm.logs_appended",
		metric.WithDescription("Total number of logs appended to the store. Use rate() to get logs per second."),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating logs_appended counter: %w", err)
	}

	return &FSM{
		state: &raftcmdpb.State{
			Ledgers:      make(map[uint32]*raftcmdpb.LedgerState),
			NextLedgerId: 1,
			NextSequence: 1,
		},
		logger:                logger,
		store:                 store,
		transport:             transport,
		storeLastAppliedIndex: lastAppliedIndex,
		logsAppendedCounter:   logsAppendedCounter,
	}, nil
}

// getNextSequence returns the next sequence number and increments the counter
func (fsm *FSM) getNextSequence() uint64 {
	seq := fsm.state.NextSequence
	fsm.state.NextSequence++
	return seq
}

// GetState returns a copy of the FSM state
func (fsm *FSM) GetState() *raftcmdpb.State {
	return proto.CloneOf(fsm.state)
}

// Note: With etcd/raft, we don't have an Apply method on FSM.
// The entries are applied directly in the readyLoop of Node.
// Ledgers and logs are now managed by ledger Raft groups.

// handleCreateLedger handles the create ledger action
func (fsm *FSM) handleCreateLedger(action *raftcmdpb.Action, cmdDate *commonpb.Timestamp) (*commonpb.Log, error) {
	var createCmd raftcmdpb.CreateLedgerCommand
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
	ledgerInfo := &commonpb.LedgerInfo{
		Name:      createCmd.Name,
		Metadata:  createCmd.Metadata,
		CreatedAt: cmdDate,
		Id:        ledgerID,
	}
	fsm.state.Ledgers[ledgerID] = &raftcmdpb.LedgerState{
		LedgerInfo:        ledgerInfo,
		NextLogId:         1,
		NextTransactionId: 1,
	}

	fsm.logger.WithFields(map[string]any{
		"ledger": ledgerInfo.Name,
	}).Infof("Ledger created")

	return &commonpb.Log{
		Sequence: fsm.getNextSequence(),
		Payload: &commonpb.Log_CreateLedger{
			CreateLedger: &commonpb.CreateLedgerLog{
				Info: &commonpb.LedgerInfo{
					Id:        ledgerID,
					Name:      createCmd.Name,
					CreatedAt: commonpb.NewTimestamp(time.Now()),
				},
			},
		},
	}, nil
}

// handleDeleteLedger handles the delete ledger action (hard delete)
func (fsm *FSM) handleDeleteLedger(action *raftcmdpb.Action) (*commonpb.Log, error) {
	var deleteCmd raftcmdpb.DeleteLedgerCommand
	if err := UnmarshalCommandData(action.Data, &deleteCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal delete ledger command")
		return nil, fmt.Errorf("unmarshaling delete ledger command: %w", err)
	}

	// Check if ledger exists
	_, ok := fsm.state.Ledgers[deleteCmd.Id]
	if !ok {
		return nil, commonpb.NewNotFoundError("ledger %d does not exist", deleteCmd.Id)
	}

	delete(fsm.state.Ledgers, deleteCmd.Id)

	return &commonpb.Log{
		Sequence: fsm.getNextSequence(),
		Payload: &commonpb.Log_DeleteLedger{
			DeleteLedger: &commonpb.DeleteLedgerLog{
				LedgerId: deleteCmd.Id,
			},
		},
	}, nil
}

// createLog handles the insert log action by building the log entry
func (fsm *FSM) createLog(action *raftcmdpb.Action, cmdDate *commonpb.Timestamp) (*commonpb.Log, error) {

	switch action.ActionType {
	case raftcmdpb.ActionType_CreateLedger:
		var cmd raftcmdpb.CreateLedgerCommand
		if err := UnmarshalCommandData(action.Data, &cmd); err != nil {
			fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal insert log command")
			return nil, err
		}

		return fsm.handleCreateLedger(action, cmdDate)

	case raftcmdpb.ActionType_DeleteLedger:
		var cmd raftcmdpb.DeleteLedgerCommand
		if err := UnmarshalCommandData(action.Data, &cmd); err != nil {
			fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal insert log command")
			return nil, err
		}
		return fsm.handleDeleteLedger(action)

	case raftcmdpb.ActionType_CreateLog:
		var createCmd raftcmdpb.CreateLogCommand
		if err := UnmarshalCommandData(action.Data, &createCmd); err != nil {
			fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal insert log command")
			return nil, err
		}

		var logPayload *commonpb.LogPayload
		switch cmd := createCmd.Input.Command.(type) {
		case *raftcmdpb.CommandInput_AppendTransaction:
			timestamp := cmd.AppendTransaction.Timestamp
			if timestamp == nil || timestamp.Data == 0 {
				timestamp = cmdDate
			}
			logPayload = &commonpb.LogPayload{
				Payload: &commonpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{
						Transaction: &commonpb.Transaction{
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
		case *raftcmdpb.CommandInput_SaveMetadata:
			logPayload = &commonpb.LogPayload{
				Payload: &commonpb.LogPayload_SavedMetadata{
					SavedMetadata: &commonpb.SavedMetadata{
						Target:   cmd.SaveMetadata.Target,
						Metadata: cmd.SaveMetadata.Metadata,
					},
				},
			}
		case *raftcmdpb.CommandInput_DeleteMetadata:
			logPayload = &commonpb.LogPayload{
				Payload: &commonpb.LogPayload_DeletedMetadata{
					DeletedMetadata: &commonpb.DeletedMetadata{
						Target: cmd.DeleteMetadata.Target,
						Key:    cmd.DeleteMetadata.Key,
					},
				},
			}
		case *raftcmdpb.CommandInput_RevertTransaction: // todo: should not need to read the original data
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

			logPayload = &commonpb.LogPayload{
				Payload: &commonpb.LogPayload_RevertedTransaction{
					RevertedTransaction: &commonpb.RevertedTransaction{
						RevertedTransactionId: cmd.RevertTransaction.TransactionId,
						RevertTransaction:     revertTx,
					},
				},
			}
		default:
			return nil, fmt.Errorf("unhandled command type: %T", cmd)
		}

		return &commonpb.Log{
			Sequence: fsm.getNextSequence(),
			Payload: &commonpb.Log_Apply{
				Apply: &commonpb.ApplyLog{
					LedgerId: createCmd.LedgerId,
					Log: &commonpb.LedgerLog{
						Id:   fsm.state.Ledgers[createCmd.LedgerId].GetNextLogID(),
						Date: cmdDate,
						Data: logPayload,
					},
				},
			},
			Idempotency: nil,
		}, nil

	default:
		return nil, fmt.Errorf("unknown action type: %s", action.ActionType)
	}
}

func (fsm *FSM) ApplyEntries(ctx context.Context, entries ...raftpb.Entry) ([]ApplyResult, error) {

	if fsm.snapshotIndex > fsm.storeLastAppliedIndex {
		return nil, fmt.Errorf("last snapshot index is %d, expecting lower than %d, node out of sync", fsm.snapshotIndex, fsm.storeLastAppliedIndex)
	}

	batch := fsm.store.NewBatch(entries[len(entries)-1].Index)
	defer func() {
		_ = batch.Cancel(ctx)
	}()
	cmd := &raftcmdpb.Command{}

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
		result, err := fsm.applyEntry(ctx, batch, cmd)
		if err != nil {
			return nil, err
		}
		ret = append(ret, *result)
	}

	if err := batch.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing batch: %w", err)
	}

	return ret, nil
}

// applyEntry processes all actions in a command atomically
func (fsm *FSM) applyEntry(ctx context.Context, batch store.Batch, cmd *raftcmdpb.Command) (*ApplyResult, error) {
	var logs []*commonpb.Log

	for _, action := range cmd.Actions {
		log, err := fsm.applyAction(ctx, batch, action, cmd.Date)
		if err != nil {
			return nil, err
		}
		if log != nil {
			logs = append(logs, log)
		}
	}

	return &ApplyResult{
		Logs:      logs,
		CommandID: cmd.Id,
	}, nil
}

// applyAction processes a single action
func (fsm *FSM) applyAction(ctx context.Context, batch store.Batch, action *raftcmdpb.Action, cmdDate *commonpb.Timestamp) (*commonpb.Log, error) {

	log, err := fsm.createLog(action, cmdDate)
	if err != nil {
		return nil, err
	}

	if err := fsm.projectLog(ctx, batch, log); err != nil {
		return nil, fmt.Errorf("projecting log %d: %w", log.Sequence, err)
	}

	if err := batch.AppendLogs(ctx, log); err != nil {
		return nil, fmt.Errorf("writing system log to runtime store: %w", err)
	}

	// Increment the logs appended counter
	fsm.logsAppendedCounter.Add(ctx, 1)

	return log, nil
}

// GetLedgerByName returns the ledger node for a given name (including deleted ledgers)
func (fsm *FSM) GetLedgerByName(name string) (*commonpb.LedgerInfo, error) {
	for _, state := range fsm.state.Ledgers {
		if state.LedgerInfo.Name == name {
			return proto.CloneOf(state.LedgerInfo), nil
		}
	}

	return nil, commonpb.NewNotFoundError("ledger %s does not exist", name)
}

func (fsm *FSM) GetLedgerInfo(id uint32) (*commonpb.LedgerInfo, error) {
	ledger, ok := fsm.state.Ledgers[id]
	if !ok {
		return nil, commonpb.NewNotFoundError("ledger %d does not exist", id)
	}

	return proto.CloneOf(ledger.LedgerInfo), nil
}

// GetAllLedgers returns all ledgers (including deleted ones)
func (fsm *FSM) GetAllLedgers() map[string]*commonpb.LedgerInfo {
	ret := make(map[string]*commonpb.LedgerInfo, len(fsm.state.Ledgers))
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

func (fsm *FSM) projectLog(ctx context.Context, batch store.Batch, log *commonpb.Log) error {

	switch {
	case log.GetApply() != nil:
		ledgerLog := log.GetApply().Log

		projectTransaction := func(ctx context.Context, batch store.Batch, tx *commonpb.Transaction) error {
			for _, posting := range tx.Postings {
				if err := batch.AppendBalanceDiff(ctx, log.GetApply().LedgerId, posting.Source, posting.Asset, posting.Amount.Neg(), ledgerLog.Id); err != nil {
					return fmt.Errorf("appending balance diff for posting %s: %w", posting.String(), err)
				}
				if err := batch.AppendBalanceDiff(ctx, log.GetApply().LedgerId, posting.Destination, posting.Asset, posting.Amount, ledgerLog.Id); err != nil {
					return fmt.Errorf("appending balance diff for posting %s: %w", posting.String(), err)
				}
			}

			return nil
		}

		switch payload := ledgerLog.Data.Payload.(type) {
		case *commonpb.LogPayload_CreatedTransaction:
			if err := projectTransaction(ctx, batch, payload.CreatedTransaction.Transaction); err != nil {
				return fmt.Errorf("projecting transaction %d: %w", payload.CreatedTransaction.Transaction.Id, err)
			}
			// Store the global sequence for transaction ID lookup
			if err := batch.StoreTransactionID(ctx, log.GetApply().LedgerId, payload.CreatedTransaction.Transaction.Id, log.Sequence); err != nil {
				return err
			}
			if payload.CreatedTransaction.AccountMetadata != nil {
				for account, metadata := range payload.CreatedTransaction.AccountMetadata {
					err := batch.SaveAccountMetadata(ctx, log.GetApply().LedgerId, account, metadata)
					if err != nil {
						return err
					}
				}
			}

		case *commonpb.LogPayload_RevertedTransaction:
			if err := projectTransaction(ctx, batch, payload.RevertedTransaction.RevertTransaction); err != nil {
				return fmt.Errorf("projecting transaction %d: %w", payload.RevertedTransaction.RevertTransaction.Id, err)
			}
			// Store the global sequence for reverted transaction lookup
			if err := batch.StoreRevertedTransactionID(ctx, log.GetApply().LedgerId, payload.RevertedTransaction.RevertedTransactionId, log.Sequence); err != nil {
				return err
			}
		case *commonpb.LogPayload_SavedMetadata:
			if account := payload.SavedMetadata.Target.GetAccount(); account != nil {
				if err := batch.SaveAccountMetadata(ctx, log.GetApply().LedgerId, account.Addr, payload.SavedMetadata.Metadata); err != nil {
					return err
				}
			}
		case *commonpb.LogPayload_DeletedMetadata:
			if account := payload.DeletedMetadata.Target.GetAccount(); account != nil {
				if err := batch.DeleteAccountMetadata(ctx, log.GetApply().LedgerId, account.Addr, []string{
					payload.DeletedMetadata.Key,
				}); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unhandled log payload type: %T", payload)
		}

		return nil
	case log.GetCreateLedger() != nil:
		return batch.RegisterLedger(ctx, log.GetCreateLedger().GetInfo())
	case log.GetDeleteLedger() != nil:
		return batch.DeleteLedger(ctx, log.GetDeleteLedger().GetLedgerId())
	default:
		return fmt.Errorf("unhandled log type: %T", log)
	}
}

func (fsm *FSM) SynchronizeWithLeader(ctx context.Context, logStreamer LogStreamer) (uint64, error) {
	lastSequence, err := fsm.store.GetLastSequence(ctx)
	if err != nil {
		return 0, err
	}

	// If we're already up to date, nothing to do
	if lastSequence >= fsm.state.NextSequence-1 {
		fsm.storeLastAppliedIndex = fsm.snapshotIndex
		return fsm.snapshotIndex, nil
	}

	fsm.logger.WithFields(map[string]any{
		"lastSequence":    lastSequence,
		"newNextSequence": fsm.state.NextSequence,
	}).Infof("Syncing system logs from leader")

	logStream, err := logStreamer.GetAllLogs(ctx, lastSequence, fsm.state.NextSequence-1)
	if err != nil {
		return 0, fmt.Errorf("streaming system logs from peer: %w", err)
	}

	batch := fsm.store.NewBatch(fsm.snapshotIndex)
	defer func() {
		_ = batch.Cancel(ctx)
	}()

	count := 0
	for {
		log, err := logStream.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, fmt.Errorf("reading system log during catch-up: %w", err)
		}
		count++

		// Project the log
		if err := fsm.projectLog(ctx, batch, log); err != nil {
			return 0, fmt.Errorf("projecting log %d: %w", log.Sequence, err)
		}

		if err := batch.AppendLogs(ctx, log); err != nil {
			return 0, fmt.Errorf("writing catch-up system logs to runtime store: %w", err)
		}
	}

	fsm.logger.WithFields(map[string]any{
		"logsWritten": count,
	}).Infof("Synced system logs from leader")

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
	Logs      []*commonpb.Log
	Error     error
}
