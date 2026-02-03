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
	store     *store.Store
	transport Transport

	storeLastAppliedIndex uint64
	snapshotIndex         uint64

	// Metrics
	logsAppendedCounter metric.Int64Counter
}

func newFSM(logger logging.Logger, store *store.Store, transport Transport, meter metric.Meter) (*FSM, error) {
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
func (fsm *FSM) handleCreateLedger(createCmd *raftcmdpb.CreateLedgerCommand, cmdDate *commonpb.Timestamp) (*commonpb.Log, error) {
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
		Metadata:  commonpb.MetadataSetFromMap(createCmd.Metadata),
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
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{
					Info: &commonpb.LedgerInfo{
						Id:        ledgerID,
						Name:      createCmd.Name,
						CreatedAt: commonpb.NewTimestamp(time.Now()),
					},
				},
			},
		},
	}, nil
}

// handleDeleteLedger handles the delete ledger action (soft delete)
func (fsm *FSM) handleDeleteLedger(deleteCmd *raftcmdpb.DeleteLedgerCommand, cmdDate *commonpb.Timestamp) (*commonpb.Log, error) {
	// Check if ledger exists
	ledgerState, ok := fsm.state.Ledgers[deleteCmd.Id]
	if !ok {
		return nil, commonpb.NewNotFoundError("ledger %d does not exist", deleteCmd.Id)
	}

	// Check if already deleted
	if ledgerState.LedgerInfo.DeletedAt != nil {
		return nil, fmt.Errorf("ledger %d is already deleted", deleteCmd.Id)
	}

	// Mark as deleted (soft delete)
	ledgerState.LedgerInfo.DeletedAt = cmdDate

	return &commonpb.Log{
		Sequence: fsm.getNextSequence(),
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeleteLedger{
				DeleteLedger: &commonpb.DeleteLedgerLog{
					Info: ledgerState.LedgerInfo,
				},
			},
		},
	}, nil
}

// createLog handles the insert log action by building the log entry
func (fsm *FSM) createLog(action *raftcmdpb.Action, cmdDate *commonpb.Timestamp) (*commonpb.Log, error) {
	cmd := action.Command
	if cmd == nil {
		return nil, fmt.Errorf("action has no command")
	}

	switch {
	case cmd.GetCreateLedger() != nil:
		return fsm.handleCreateLedger(cmd.GetCreateLedger(), cmdDate)

	case cmd.GetDeleteLedger() != nil:
		return fsm.handleDeleteLedger(cmd.GetDeleteLedger(), cmdDate)

	case cmd.GetCreateLedgerLog() != nil:
		createCmd := cmd.GetCreateLedgerLog()

		var logPayload *commonpb.LedgerLogPayload
		switch cmd := createCmd.Command.(type) {
		case *raftcmdpb.CreateLedgerLogCommand_AppendTransaction:
			timestamp := cmd.AppendTransaction.Timestamp
			if timestamp == nil || timestamp.Data == 0 {
				timestamp = cmdDate
			}
			logPayload = &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{
						Transaction: &commonpb.Transaction{
							Postings:   cmd.AppendTransaction.Postings,
							Metadata:   commonpb.MetadataSetFromMap(cmd.AppendTransaction.Metadata),
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
		case *raftcmdpb.CreateLedgerLogCommand_SaveMetadata:
			logPayload = &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_SavedMetadata{
					SavedMetadata: &commonpb.SavedMetadata{
						Target:   cmd.SaveMetadata.Target,
						Metadata: cmd.SaveMetadata.Metadata,
					},
				},
			}
		case *raftcmdpb.CreateLedgerLogCommand_DeleteMetadata:
			logPayload = &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
					DeletedMetadata: &commonpb.DeletedMetadata{
						Target: cmd.DeleteMetadata.Target,
						Key:    cmd.DeleteMetadata.Key,
					},
				},
			}
		case *raftcmdpb.CreateLedgerLogCommand_RevertTransaction:
			// Build the revert transaction from the command data
			revertCmd := cmd.RevertTransaction.RevertTransaction
			timestamp := revertCmd.Timestamp
			if timestamp == nil || timestamp.Data == 0 {
				timestamp = cmdDate
			}

			// Create a new transaction with the assigned ID and timestamps
			revertTx := &commonpb.Transaction{
				Postings:   revertCmd.Postings,
				Metadata:   commonpb.MetadataSetFromMap(revertCmd.Metadata),
				Timestamp:  timestamp,
				Reference:  revertCmd.Reference,
				Id:         fsm.state.Ledgers[createCmd.LedgerId].GetNextTransactionID(),
				InsertedAt: cmdDate,
				UpdatedAt:  cmdDate,
			}

			logPayload = &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
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
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerId: createCmd.LedgerId,
						Log: &commonpb.LedgerLog{
							Id:   fsm.state.Ledgers[createCmd.LedgerId].GetNextLogID(),
							Date: cmdDate,
							Data: logPayload,
						},
					},
				},
			},
			Idempotency: nil,
		}, nil

	default:
		return nil, fmt.Errorf("unknown action command type")
	}
}

func (fsm *FSM) ApplyEntries(ctx context.Context, entries ...raftpb.Entry) ([]ApplyResult, error) {

	if fsm.snapshotIndex > fsm.storeLastAppliedIndex {
		return nil, fmt.Errorf("last snapshot index is %d, expecting lower than %d, node out of sync", fsm.snapshotIndex, fsm.storeLastAppliedIndex)
	}

	batch := fsm.store.NewBatch()
	defer func() {
		_ = batch.Cancel()
	}()
	lastIndex := entries[len(entries)-1].Index
	cmd := &raftcmdpb.CommandBatch{}

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

	if err := batch.SetAppliedIndex(lastIndex); err != nil {
		return nil, fmt.Errorf("setting applied index: %w", err)
	}
	if err := batch.Commit(); err != nil {
		return nil, fmt.Errorf("committing batch: %w", err)
	}

	return ret, nil
}

// applyEntry processes all actions in a command atomically
func (fsm *FSM) applyEntry(ctx context.Context, batch *store.Batch, cmd *raftcmdpb.CommandBatch) (*ApplyResult, error) {
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
func (fsm *FSM) applyAction(ctx context.Context, batch *store.Batch, action *raftcmdpb.Action, cmdDate *commonpb.Timestamp) (*commonpb.Log, error) {

	log, err := fsm.createLog(action, cmdDate)
	if err != nil {
		return nil, err
	}

	// Use the current raft index for balance diff keys (storeLastAppliedIndex is updated before this call)
	if err := fsm.projectLog(ctx, batch, log, fsm.storeLastAppliedIndex); err != nil {
		return nil, fmt.Errorf("projecting log %d: %w", log.Sequence, err)
	}

	if err := batch.AppendLogs(log); err != nil {
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

// getLedgerNameByID returns the ledger name for a given ID.
// This is an internal helper used during log projection.
func (fsm *FSM) getLedgerNameByID(id uint32) (string, error) {
	ledger, ok := fsm.state.Ledgers[id]
	if !ok {
		return "", fmt.Errorf("ledger %d does not exist", id)
	}
	return ledger.LedgerInfo.Name, nil
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
func (fsm *FSM) CreateSnapshot(_ context.Context) ([]byte, error) {
	if err := fsm.store.CreateSnapshot(); err != nil {
		return nil, fmt.Errorf("creating snapshot: %w", err)
	}

	return proto.Marshal(fsm.state)
}

func (fsm *FSM) InstallSnapshot(ctx context.Context, snapshot raftpb.Snapshot) error {
	fsm.snapshotIndex = snapshot.Metadata.Index
	return proto.Unmarshal(snapshot.Data, fsm.state)
}

// projectLog projects a log to the store using the given index for balance diff keys.
// The index parameter is used as the unique suffix for balance diff keys to avoid collisions.
// During normal raft apply, this is the raft entry index.
// During sync from leader, this is the log's sequence number.
func (fsm *FSM) projectLog(_ context.Context, batch *store.Batch, log *commonpb.Log, index uint64) error {
	switch {
	case log.Payload.GetApply() != nil:
		ledgerLog := log.Payload.GetApply().Log
		ledgerID := log.Payload.GetApply().LedgerId
		ledgerName, err := fsm.getLedgerNameByID(ledgerID)
		if err != nil {
			return fmt.Errorf("getting ledger name for ID %d: %w", ledgerID, err)
		}

		projectTransaction := func(batch *store.Batch, tx *commonpb.Transaction) error {
			for _, posting := range tx.Postings {
				sourceKey := store.TimestampedBalanceKey{
					TimestampedAccountKey: store.TimestampedAccountKey{
						AccountKey: store.AccountKey{LedgerName: ledgerName, Account: posting.Source},
						RaftIndex:  index,
					},
					Asset: posting.Asset,
				}
				if err := batch.AppendBalanceDiff(sourceKey, posting.Amount.Neg()); err != nil {
					return fmt.Errorf("appending balance diff for posting %s: %w", posting.String(), err)
				}
				destKey := store.TimestampedBalanceKey{
					TimestampedAccountKey: store.TimestampedAccountKey{
						AccountKey: store.AccountKey{LedgerName: ledgerName, Account: posting.Destination},
						RaftIndex:  index,
					},
					Asset: posting.Asset,
				}
				if err := batch.AppendBalanceDiff(destKey, posting.Amount); err != nil {
					return fmt.Errorf("appending balance diff for posting %s: %w", posting.String(), err)
				}
			}

			return nil
		}

		switch payload := ledgerLog.Data.Payload.(type) {
		case *commonpb.LedgerLogPayload_CreatedTransaction:
			if err := projectTransaction(batch, payload.CreatedTransaction.Transaction); err != nil {
				return fmt.Errorf("projecting transaction %d: %w", payload.CreatedTransaction.Transaction.Id, err)
			}
			// Store transaction init update
			if err := batch.StoreTransactionUpdate(ledgerName, payload.CreatedTransaction.Transaction.Id, &commonpb.TransactionUpdate{
				ByLog: log.Sequence,
				Updates: []*commonpb.TransactionUpdateType{
					{
						TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
							TransactionInit: &commonpb.TransactionInit{},
						},
					},
				},
			}); err != nil {
				return err
			}
			if payload.CreatedTransaction.AccountMetadata != nil {
				for account, metadataSet := range payload.CreatedTransaction.AccountMetadata {
					if metadataSet != nil {
						for _, md := range metadataSet.Metadata {
							if md != nil && md.Value != nil {
								key := store.TimestampedMetadataKey{
									TimestampedAccountKey: store.TimestampedAccountKey{
										AccountKey: store.AccountKey{LedgerName: ledgerName, Account: account},
										RaftIndex:  log.Sequence,
									},
									Key: md.Key,
								}
								if err := batch.AppendMetadataDiff(key, md.Value); err != nil {
									return err
								}
							}
						}
					}
				}
			}

		case *commonpb.LedgerLogPayload_RevertedTransaction:
			if err := projectTransaction(batch, payload.RevertedTransaction.RevertTransaction); err != nil {
				return fmt.Errorf("projecting transaction %d: %w", payload.RevertedTransaction.RevertTransaction.Id, err)
			}
			// Store transaction revert update for the original transaction
			if err := batch.StoreTransactionUpdate(ledgerName, payload.RevertedTransaction.RevertedTransactionId, &commonpb.TransactionUpdate{
				ByLog: log.Sequence,
				Updates: []*commonpb.TransactionUpdateType{
					{
						TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationRevert{
							TransactionModificationRevert: &commonpb.TransactionUpdateRevert{
								ByTransaction: payload.RevertedTransaction.RevertTransaction.Id,
							},
						},
					},
				},
			}); err != nil {
				return err
			}
			// Store transaction init for the revert transaction itself
			if err := batch.StoreTransactionUpdate(ledgerName, payload.RevertedTransaction.RevertTransaction.Id, &commonpb.TransactionUpdate{
				ByLog: log.Sequence,
				Updates: []*commonpb.TransactionUpdateType{
					{
						TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
							TransactionInit: &commonpb.TransactionInit{},
						},
					},
				},
			}); err != nil {
				return err
			}
		case *commonpb.LedgerLogPayload_SavedMetadata:
			if account := payload.SavedMetadata.Target.GetAccount(); account != nil {
				if payload.SavedMetadata.Metadata != nil {
					for _, md := range payload.SavedMetadata.Metadata.Metadata {
						if md != nil && md.Value != nil {
							key := store.TimestampedMetadataKey{
								TimestampedAccountKey: store.TimestampedAccountKey{
									AccountKey: store.AccountKey{LedgerName: ledgerName, Account: account.Addr},
									RaftIndex:  log.Sequence,
								},
								Key: md.Key,
							}
							if err := batch.AppendMetadataDiff(key, md.Value); err != nil {
								return err
							}
						}
					}
				}
			}
		case *commonpb.LedgerLogPayload_DeletedMetadata:
			if account := payload.DeletedMetadata.Target.GetAccount(); account != nil {
				key := store.TimestampedMetadataKey{
					TimestampedAccountKey: store.TimestampedAccountKey{
						AccountKey: store.AccountKey{LedgerName: ledgerName, Account: account.Addr},
						RaftIndex:  log.Sequence,
					},
					Key: payload.DeletedMetadata.Key,
				}
				if err := batch.AppendMetadataDiff(key, nil); err != nil { // nil means deletion
					return err
				}
			}
		default:
			return fmt.Errorf("unhandled log payload type: %T", payload)
		}

		return nil
	case log.Payload.GetCreateLedger() != nil:
		return batch.SaveLedger(log.Payload.GetCreateLedger().GetInfo())
	case log.Payload.GetDeleteLedger() != nil:
		return batch.SaveLedger(log.Payload.GetDeleteLedger().GetInfo())
	default:
		return fmt.Errorf("unhandled log type: %T", log)
	}
}

func (fsm *FSM) SynchronizeWithLeader(ctx context.Context, logStreamer LogStreamer) (uint64, error) {
	lastSequence, err := fsm.store.GetLastSequence()
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

	logStream, err := logStreamer.GetAllLogs(lastSequence, fsm.state.NextSequence-1)
	if err != nil {
		return 0, fmt.Errorf("streaming system logs from peer: %w", err)
	}

	batch := fsm.store.NewBatch()
	defer func() {
		_ = batch.Cancel()
	}()

	count := 0
	for {
		log, err := logStream.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, fmt.Errorf("reading system log during catch-up: %w", err)
		}
		count++

		// Project the log using the log's sequence as the index for balance diff keys
		// (during sync we don't have the original raft index, so we use sequence which is globally unique)
		if err := fsm.projectLog(ctx, batch, log, log.Sequence); err != nil {
			return 0, fmt.Errorf("projecting log %d: %w", log.Sequence, err)
		}

		if err := batch.AppendLogs(log); err != nil {
			return 0, fmt.Errorf("writing catch-up system logs to runtime store: %w", err)
		}
	}

	fsm.logger.WithFields(map[string]any{
		"logsWritten": count,
	}).Infof("Synced system logs from leader")

	if err := batch.SetAppliedIndex(fsm.snapshotIndex); err != nil {
		return 0, fmt.Errorf("setting applied index: %w", err)
	}
	if err := batch.Commit(); err != nil {
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
