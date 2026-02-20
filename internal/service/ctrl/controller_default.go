package ctrl

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller_default.go -destination controller_default_generated_test.go -package ctrl . Admission
type Admission interface {
	Admit(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error)
}

// DefaultController is the default implementation of the Controller interface.
// It is responsible for forwarding requests to the Raft admission layer.
// The FSM is responsible for interpreting requests, validating, and applying changes.
// Idempotency is handled in the FSM to ensure consistency in the Raft log.
type DefaultController struct {
	logger    logging.Logger
	admission Admission
	store     *data.Store
	attrs     *attributes.Attributes
}

// NewDefaultController creates a new default controller
func NewDefaultController(
	admission Admission,
	store *data.Store,
	logger logging.Logger,
	attrs *attributes.Attributes,
) *DefaultController {
	return &DefaultController{
		logger:    logger,
		admission: admission,
		store:     store,
		attrs:     attrs,
	}
}

// ListLedgers returns a cursor over all active (non-deleted) ledgers
func (ctrl *DefaultController) ListLedgers(_ context.Context) (data.Cursor[*commonpb.LedgerInfo], error) {
	handle := ctrl.store.NewReadHandle()
	cursor, err := data.ReadLedgers(handle)
	if err != nil {
		_ = handle.Close()
		return nil, err
	}
	// Filter out soft-deleted ledgers, close handle when cursor closes
	filtered := data.NewFilteredCursor(cursor, func(ledger *commonpb.LedgerInfo) bool {
		return ledger.DeletedAt == nil
	})
	return data.NewClosingCursor(filtered, handle), nil
}

func (ctrl *DefaultController) GetTransaction(_ context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error) {
	ledgerInfo, err := ctrl.store.GetLedgerByName(ledgerName)
	if err != nil {
		if errors.Is(err, data.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}
		return nil, err
	}

	handle := ctrl.store.NewReadHandle()
	defer func() { _ = handle.Close() }()

	return buildTransaction(handle, ledgerInfo.Id, transactionID)
}

// buildTransaction builds a transaction from updates and logs using the given reader.
func buildTransaction(reader data.PebbleReader, ledgerID uint32, transactionID uint64) (*commonpb.Transaction, error) {
	updates, err := data.ReadTransactionUpdates(reader, ledgerID, transactionID)
	if err != nil {
		return nil, fmt.Errorf("getting transaction updates for %d: %w", transactionID, err)
	}

	return assembleTransaction(reader, transactionID, updates)
}

// assembleTransaction builds a transaction from a slice of updates and a log reader.
// The updates must be in chronological order (lowest byLog first).
func assembleTransaction(reader data.PebbleReader, transactionID uint64, updates []*commonpb.TransactionUpdate) (*commonpb.Transaction, error) {
	var (
		sequence         uint64
		reverted         bool
		metadataToAdd    = make(map[string]string)
		metadataToDelete = make(map[string]struct{})
	)
	for _, update := range updates {
		for _, updateType := range update.Updates {
			if updateType.GetTransactionInit() != nil {
				sequence = update.ByLog
			}
			if updateType.GetTransactionModificationRevert() != nil {
				reverted = true
			}
			if addMeta := updateType.GetTransactionModificationAddMetadata(); addMeta != nil {
				metadataToAdd[addMeta.Metadata.Key] = addMeta.Metadata.Value.Value
				delete(metadataToDelete, addMeta.Metadata.Key)
			}
			if delMeta := updateType.GetTransactionModificationDeleteMetadata(); delMeta != nil {
				metadataToDelete[delMeta.Key] = struct{}{}
				delete(metadataToAdd, delMeta.Key)
			}
		}
	}

	if sequence == 0 {
		return nil, commonpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	log, err := data.ReadLogBySequence(reader, sequence)
	if err != nil {
		return nil, fmt.Errorf("getting system log %d: %w", sequence, err)
	}
	if log == nil {
		return nil, commonpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	applyLog, ok := log.Payload.Type.(*commonpb.LogPayload_Apply)
	if !ok || applyLog.Apply == nil || applyLog.Apply.Log == nil {
		return nil, fmt.Errorf("log %d does not contain an apply log", sequence)
	}
	ledgerLog := applyLog.Apply.Log

	var tx *commonpb.Transaction
	switch payload := ledgerLog.Data.Payload.(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if payload.CreatedTransaction == nil || payload.CreatedTransaction.Transaction == nil {
			return nil, fmt.Errorf("invalid log payload: missing transaction")
		}
		tx = payload.CreatedTransaction.Transaction
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if payload.RevertedTransaction == nil || payload.RevertedTransaction.RevertTransaction == nil {
			return nil, fmt.Errorf("invalid log payload: missing revert transaction")
		}
		tx = payload.RevertedTransaction.RevertTransaction
	default:
		return nil, fmt.Errorf("ledger log %d does not contain a transaction", ledgerLog.Id)
	}

	tx.Reverted = reverted
	if len(metadataToAdd) > 0 || len(metadataToDelete) > 0 {
		existingMeta := tx.Metadata.ToMap()
		if existingMeta == nil {
			existingMeta = make(map[string]string)
		}
		for key, value := range metadataToAdd {
			existingMeta[key] = value
		}
		for key := range metadataToDelete {
			delete(existingMeta, key)
		}
		tx.Metadata = commonpb.MetadataSetFromMap(existingMeta)
	}

	return tx, nil
}

// ListTransactions returns a cursor over transactions for a ledger (newest first).
// Uses a single reverse iterator over transaction update keys for efficiency.
func (ctrl *DefaultController) ListTransactions(_ context.Context, ledgerName string, pageSize uint32, afterTxID uint64) (data.Cursor[*commonpb.Transaction], error) {
	ledgerInfo, err := ctrl.store.GetLedgerByName(ledgerName)
	if err != nil {
		if errors.Is(err, data.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}
		return nil, err
	}

	handle := ctrl.store.NewReadHandle()

	// Build iterator bounds for [keyPrefixTransactionUpdate][ledgerID]...[afterTxID or max]
	kb := data.NewKeyBuilder()
	kb.PutByte(data.KeyPrefixTransactionUpdate).
		PutLedgerPrefix(ledgerInfo.Id)
	lowerBound := kb.Snapshot()

	if afterTxID > 0 {
		kb.PutUInt64(afterTxID)
	} else {
		kb.PutBytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	}
	upperBound := kb.Build()

	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		_ = handle.Close()
		return nil, fmt.Errorf("creating iterator for transaction list: %w", err)
	}

	return &transactionCursor{
		handle:     handle,
		iter:       iter,
		pageSize:   pageSize,
		lastTxID:   ^uint64(0),
		txIDOffset: data.TxUpdateTxIDOffset,
	}, nil
}

// ListAccounts returns a cursor over accounts for a ledger (alphabetical order).
// Uses a single forward iterator over the attribute range to discover accounts
// and collect metadata in one pass.
func (ctrl *DefaultController) ListAccounts(_ context.Context, ledgerName string, pageSize uint32, afterAddress string, prefix string) (data.Cursor[*commonpb.Account], error) {
	ledgerInfo, err := ctrl.store.GetLedgerByName(ledgerName)
	if err != nil {
		if errors.Is(err, data.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}
		return nil, err
	}

	handle := ctrl.store.NewReadHandle()

	// Build lower bound: [0xF1][ledgerID] optionally followed by [afterAddress]\x02 or [prefix]
	kb := data.NewKeyBuilder()
	kb.PutByte(data.KeyPrefixAttributes).PutLedgerPrefix(ledgerInfo.Id)
	if afterAddress != "" {
		kb.PutString(afterAddress).PutByte(0x02) // skip past both \x00 (Volume) and \x01 (Metadata)
	} else if prefix != "" {
		kb.PutString(prefix)
	}
	lowerBound := kb.Build()

	// Build upper bound: [0xF1][ledgerID][IncrementBytes(prefix)] or [0xF1][ledgerID+1]
	kb.PutByte(data.KeyPrefixAttributes)
	if prefix != "" {
		if incPrefix := attributes.IncrementBytes([]byte(prefix)); incPrefix != nil {
			kb.PutLedgerPrefix(ledgerInfo.Id).PutBytes(incPrefix)
		} else {
			kb.PutLedgerPrefix(ledgerInfo.Id + 1)
		}
	} else {
		kb.PutLedgerPrefix(ledgerInfo.Id + 1)
	}
	upperBound := kb.Build()

	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		_ = handle.Close()
		return nil, fmt.Errorf("creating iterator for account list: %w", err)
	}

	return &accountCursor{
		handle:   handle,
		iter:     iter,
		volAcc:   ctrl.attrs.Volume.NewAccumulator(),
		metaAcc:  ctrl.attrs.Metadata.NewAccumulator(),
		pageSize: pageSize,
	}, nil
}

func (ctrl *DefaultController) GetAccount(_ context.Context, ledgerName string, address string) (*commonpb.Account, error) {
	ledgerInfo, err := ctrl.store.GetLedgerByName(ledgerName)
	if err != nil {
		if errors.Is(err, data.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}
		return nil, err
	}

	handle := ctrl.store.NewReadHandle()
	defer func() { _ = handle.Close() }()

	metadataMap, err := GetAccountMetadata(handle, ctrl.attrs, ledgerInfo.Id, []string{address})
	if err != nil {
		return nil, fmt.Errorf("getting account metadata: %w", err)
	}

	volumes, err := GetAccountVolumes(handle, ctrl.attrs, ledgerInfo.Id, address)
	if err != nil {
		return nil, fmt.Errorf("getting account volumes: %w", err)
	}

	account := &commonpb.Account{
		Address:  address,
		Metadata: &commonpb.MetadataSet{},
		Volumes:  volumes,
	}

	if md, exists := metadataMap[address]; exists {
		account.Metadata = commonpb.MetadataSetFromMap(md)
	}

	return account, nil
}

func (ctrl *DefaultController) GetLedgerByName(_ context.Context, name string) (*commonpb.LedgerInfo, error) {
	ledgerInfo, err := ctrl.store.GetLedgerByName(name)
	if err != nil {
		if errors.Is(err, data.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", name)
		}
		return nil, err
	}
	return ledgerInfo, nil
}

// ListLogs returns a cursor over system logs.
func (ctrl *DefaultController) ListLogs(_ context.Context, afterSequence uint64, pageSize uint32) (data.Cursor[*commonpb.Log], error) {
	handle := ctrl.store.NewReadHandle()
	cursor, err := data.ReadLogsSince(handle, afterSequence)
	if err != nil {
		_ = handle.Close()
		return nil, fmt.Errorf("listing logs: %w", err)
	}

	var result = data.NewClosingCursor(cursor, handle)
	if pageSize > 0 {
		result = data.NewLimitedCursor(result, pageSize)
	}

	return result, nil
}

// ListAuditEntries returns a cursor over audit entries, applying optional filters.
func (ctrl *DefaultController) ListAuditEntries(_ context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32) (data.Cursor[*auditpb.AuditEntry], error) {
	handle := ctrl.store.NewReadHandle()
	cursor, err := data.ReadAuditEntries(handle, afterSequence)
	if err != nil {
		_ = handle.Close()
		return nil, fmt.Errorf("listing audit entries: %w", err)
	}

	var result = data.NewClosingCursor(cursor, handle)

	if failuresOnly {
		result = data.NewFilteredCursor(result, func(entry *auditpb.AuditEntry) bool {
			return entry.GetFailure() != nil
		})
	}

	if pageSize > 0 {
		result = data.NewLimitedCursor(result, pageSize)
	}

	return result, nil
}

// ListPeriods returns a cursor over all non-purged periods from the store.
func (ctrl *DefaultController) ListPeriods(_ context.Context) (data.Cursor[*commonpb.Period], error) {
	periods, err := ctrl.store.GetPeriods()
	if err != nil {
		return nil, err
	}
	return data.NewSliceCursor(periods), nil
}

// Apply applies a list of requests and returns the resulting logs.
// The controller forwards requests to the Raft admission layer.
// The FSM is responsible for interpreting orders, validating, and applying changes.
// Idempotency is handled in the FSM to ensure consistency.
func (ctrl *DefaultController) Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	if len(requests) == 0 {
		return nil, fmt.Errorf("at least one request is required")
	}

	logs, err := ctrl.admission.Admit(ctx, requests...)
	if err != nil {
		return nil, fmt.Errorf("applying raft requests: %w", err)
	}

	return logs, nil
}

var _ Controller = (*DefaultController)(nil)
