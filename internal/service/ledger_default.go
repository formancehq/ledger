package service

import (
	"context"
	"fmt"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"go.uber.org/zap"
)

// DefaultLedger is the default implementation of the Ledger interface
type DefaultLedger struct {
	transactionCreator TransactionCreator // Creates transactions via Raft FSM
	logger             *zap.Logger
}

// NewDefaultLedger creates a new default ledger service
func NewDefaultLedger(transactionCreator TransactionCreator, logger *zap.Logger) *DefaultLedger {
	return &DefaultLedger{
		transactionCreator: transactionCreator,
		logger:             logger,
	}
}

// CreateTransaction creates a new transaction
func (l *DefaultLedger) CreateTransaction(ctx context.Context, ledgerName string, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	input := parameters.Input

	// Validate postings
	if len(input.Postings) == 0 {
		return nil, nil, ErrInvalidVars
	}

	// Create transaction via TransactionCreator (which will use Raft FSM)
	// Balance checking, idempotency key checking, and dry run handling are all done in the FSM
	createdLog, err := l.transactionCreator.CreateTransaction(ledgerName, input, parameters.IdempotencyKey, parameters.DryRun)
	if err != nil {
		return nil, nil, fmt.Errorf("creating transaction: %w", err)
	}

	// Extract CreatedTransaction from log
	createdTx, ok := createdLog.Data.(*ledger.CreatedTransaction)
	if !ok {
		return nil, nil, fmt.Errorf("invalid log data type")
	}

	return createdLog, createdTx, nil
}

// RevertTransaction is not implemented yet
func (l *DefaultLedger) RevertTransaction(ctx context.Context, ledgerName string, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	return nil, nil, ErrNotFound
}

// SaveTransactionMetadata is not implemented yet
func (l *DefaultLedger) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

// SaveAccountMetadata is not implemented yet
func (l *DefaultLedger) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

// DeleteTransactionMetadata is not implemented yet
func (l *DefaultLedger) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

// DeleteAccountMetadata is not implemented yet
func (l *DefaultLedger) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

// Import is not implemented yet
func (l *DefaultLedger) Import(ctx context.Context, ledgerName string, stream chan ledger.Log) error {
	return ErrNotFound
}

// Export is not implemented yet
func (l *DefaultLedger) Export(ctx context.Context, ledgerName string, w ExportWriter) error {
	return ErrNotFound
}
