package ctrl

import (
	"context"
	"errors"
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
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

// GetAllLedgersInfo returns a cursor over all active (non-deleted) ledgers
func (ctrl *DefaultController) GetAllLedgersInfo(_ context.Context) (data.Cursor[*commonpb.LedgerInfo], error) {
	cursor, err := ctrl.store.ListLedgers()
	if err != nil {
		return nil, err
	}
	// Filter out soft-deleted ledgers
	return data.NewFilteredCursor(cursor, func(ledger *commonpb.LedgerInfo) bool {
		return ledger.DeletedAt == nil
	}), nil
}

func (ctrl *DefaultController) GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error) {
	ledgerInfo, err := ctrl.store.GetLedgerByName(ledgerName)
	if err != nil {
		if errors.Is(err, data.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}
		return nil, err
	}
	return ctrl.buildTransaction(ledgerInfo.Id, transactionID)
}

// buildTransaction builds a transaction from updates and logs.
func (ctrl *DefaultController) buildTransaction(ledgerID uint32, transactionID uint64) (*commonpb.Transaction, error) {
	// Get all updates for this transaction
	updates, err := ctrl.store.GetTransactionUpdates(ledgerID, transactionID)
	if err != nil {
		return nil, fmt.Errorf("getting transaction updates for %d: %w", transactionID, err)
	}

	// Collect metadata modifications and find sequence/revert status
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
				// Add metadata - later updates override earlier ones
				metadataToAdd[addMeta.Metadata.Key] = addMeta.Metadata.Value.Value
				// If this key was previously deleted, remove from delete set
				delete(metadataToDelete, addMeta.Metadata.Key)
			}
			if delMeta := updateType.GetTransactionModificationDeleteMetadata(); delMeta != nil {
				// Delete metadata - mark for deletion (empty value)
				metadataToDelete[delMeta.Key] = struct{}{}
				// Remove from add map if present
				delete(metadataToAdd, delMeta.Key)
			}
		}
	}

	if sequence == 0 {
		return nil, commonpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	// Get the system log containing the transaction
	log, err := ctrl.store.GetLogBySequence(sequence)
	if err != nil {
		return nil, fmt.Errorf("getting system log %d: %w", sequence, err)
	}
	if log == nil {
		return nil, commonpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	// Extract the ledger log from the log
	applyLog, ok := log.Payload.Type.(*commonpb.LogPayload_Apply)
	if !ok || applyLog.Apply == nil || applyLog.Apply.Log == nil {
		return nil, fmt.Errorf("log %d does not contain an apply log", sequence)
	}
	ledgerLog := applyLog.Apply.Log

	// Extract the transaction from the log payload
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

	// Apply metadata modifications
	tx.Reverted = reverted
	if len(metadataToAdd) > 0 || len(metadataToDelete) > 0 {
		// Get existing metadata as map
		existingMeta := tx.Metadata.ToMap()
		if existingMeta == nil {
			existingMeta = make(map[string]string)
		}

		// Apply additions
		for key, value := range metadataToAdd {
			existingMeta[key] = value
		}

		// Apply deletions (set to empty string, as per the system convention)
		for key := range metadataToDelete {
			existingMeta[key] = ""
		}

		// Convert back to MetadataSet
		tx.Metadata = commonpb.MetadataSetFromMap(existingMeta)
	}

	return tx, nil
}

// ListTransactions returns a cursor over transactions for a ledger (newest first).
func (ctrl *DefaultController) ListTransactions(_ context.Context, ledgerName string, pageSize uint32, afterTxID uint64) (data.Cursor[*commonpb.Transaction], error) {
	ledgerInfo, err := ctrl.store.GetLedgerByName(ledgerName)
	if err != nil {
		if errors.Is(err, data.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}
		return nil, err
	}

	// Get transaction ID cursor from store
	idCursor, err := ctrl.store.ListTransactionIDs(ledgerInfo.Id, pageSize, afterTxID)
	if err != nil {
		return nil, fmt.Errorf("listing transaction IDs: %w", err)
	}

	// Return a cursor that builds full transactions from IDs
	return &transactionCursor{
		ctrl:     ctrl,
		ledgerID: ledgerInfo.Id,
		idCursor: idCursor,
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

	// Get account metadata using attributes
	metadataMap, err := GetAccountMetadata(ctrl.store, ctrl.attrs, ledgerInfo.Id, []string{address})
	if err != nil {
		return nil, fmt.Errorf("getting account metadata: %w", err)
	}

	// Get account volumes
	volumes, err := GetAccountVolumes(ctrl.store, ctrl.attrs, ledgerInfo.Id, address)
	if err != nil {
		return nil, fmt.Errorf("getting account volumes: %w", err)
	}

	// Build the account response
	account := &commonpb.Account{
		Address:  address,
		Metadata: &commonpb.MetadataSet{},
		Volumes:  volumes,
	}

	// Add metadata if it exists
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

// Apply applies a list of requests and returns the resulting logs.
// The controller forwards requests to the Raft admission layer.
// The FSM is responsible for interpreting orders, validating, and applying changes.
// Idempotency is handled in the FSM to ensure consistency.
func (ctrl *DefaultController) Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	if len(requests) == 0 {
		return nil, fmt.Errorf("at least one request is required")
	}

	// Apply all requests through Raft
	// The FSM handles idempotency checking and returns cached logs if needed
	logs, err := ctrl.admission.Admit(ctx, requests...)
	if err != nil {
		return nil, fmt.Errorf("applying raft requests: %w", err)
	}

	return logs, nil
}

var _ Controller = (*DefaultController)(nil)

// transactionCursor wraps a transaction ID cursor to return full transactions.
type transactionCursor struct {
	ctrl     *DefaultController
	ledgerID uint32
	idCursor data.Cursor[uint64]
}

func (c *transactionCursor) Next() (*commonpb.Transaction, error) {
	txID, err := c.idCursor.Next()
	if err != nil {
		return nil, err
	}

	tx, err := c.ctrl.buildTransaction(c.ledgerID, txID)
	if err != nil {
		return nil, fmt.Errorf("building transaction %d: %w", txID, err)
	}

	return tx, nil
}

func (c *transactionCursor) Close() error {
	return c.idCursor.Close()
}
