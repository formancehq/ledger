package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

// DefaultLedger is the default implementation of the Ledger interface
type DefaultLedger struct {
	raft         *raft.Raft
	volumesStore VolumesStore
	logStore     LogStore // Needed for GetLastLog and GetLogWithIdempotencyKey
	logger       *zap.Logger
}

// NewDefaultLedger creates a new default ledger service
func NewDefaultLedger(raft *raft.Raft, volumesStore VolumesStore, logStore LogStore, logger *zap.Logger) *DefaultLedger {
	return &DefaultLedger{
		raft:         raft,
		volumesStore: volumesStore,
		logStore:     logStore,
		logger:       logger,
	}
}

// CreateTransaction creates a new transaction
func (l *DefaultLedger) CreateTransaction(ctx context.Context, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	input := parameters.Input

	// Validate postings
	if len(input.Postings) == 0 {
		return nil, nil, ErrInvalidVars
	}

	// Check idempotency: if idempotency key is provided, check if a log already exists
	if parameters.IdempotencyKey != "" {
		existingLog, err := l.logStore.GetLogWithIdempotencyKey(ctx, parameters.IdempotencyKey)
		if err != nil {
			return nil, nil, err
		}
		if existingLog != nil {
			// Log already exists with this idempotency key
			// Verify that the idempotency hash matches
			expectedHash := ledger.ComputeIdempotencyHash(input)
			if existingLog.IdempotencyHash != expectedHash {
				return nil, nil, ErrIdempotencyKeyConflict
			}
			// Same transaction, return the existing log
			createdTx, ok := existingLog.Data.(*ledger.CreatedTransaction)
			if !ok {
				return nil, nil, ErrIdempotencyKeyConflict
			}
			return existingLog, createdTx, nil
		}
	}

	// Group postings by source account and asset to check balances
	// Build balance query: map[account] = [assets]
	balanceQuery := make(map[string][]string)
	requiredFunds := make(map[string]map[string]*big.Int) // account -> asset -> amount

	for _, posting := range input.Postings {
		if posting.Source == ledger.WORLD {
			continue // WORLD account has infinite funds
		}

		// Add account and asset to query
		if balanceQuery[posting.Source] == nil {
			balanceQuery[posting.Source] = make([]string, 0)
		}
		// Check if asset is already in the list
		assetExists := false
		for _, asset := range balanceQuery[posting.Source] {
			if asset == posting.Asset {
				assetExists = true
				break
			}
		}
		if !assetExists {
			balanceQuery[posting.Source] = append(balanceQuery[posting.Source], posting.Asset)
		}

		// Track required funds
		if requiredFunds[posting.Source] == nil {
			requiredFunds[posting.Source] = make(map[string]*big.Int)
		}
		if requiredFunds[posting.Source][posting.Asset] == nil {
			requiredFunds[posting.Source][posting.Asset] = big.NewInt(0)
		}
		requiredFunds[posting.Source][posting.Asset].Add(requiredFunds[posting.Source][posting.Asset], posting.Amount)
	}

	// Check sufficient funds for all source accounts
	balances, err := l.volumesStore.GetBalance(ctx, balanceQuery)
	if err != nil {
		return nil, nil, err
	}

	// Check if accounts have sufficient funds
	for account, assets := range requiredFunds {
		accountBalances, ok := balances[account]
		if !ok {
			accountBalances = make(map[string]*big.Int)
		}

		for asset, requiredAmount := range assets {
			balance, ok := accountBalances[asset]
			if !ok {
				balance = big.NewInt(0)
			}

			if balance.Cmp(requiredAmount) < 0 {
				return nil, nil, ErrInsufficientFunds
			}
		}
	}

	// Get last log to chain the new log
	lastLog, err := l.logStore.GetLastLog(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Create transaction
	tx := ledger.NewTransaction().
		WithPostings(input.Postings...).
		WithTimestamp(input.Timestamp).
		WithMetadata(input.Metadata)

	if input.Reference != "" {
		tx = tx.WithReference(input.Reference)
	}

	// Create CreatedTransaction payload
	createdTx := &ledger.CreatedTransaction{
		Transaction:     tx,
		AccountMetadata: ledger.AccountMetadata(input.AccountMetadata),
	}

	// Create log
	log := ledger.NewLog(createdTx).
		WithDate(input.Timestamp)

	if parameters.IdempotencyKey != "" {
		log = log.WithIdempotencyKey(parameters.IdempotencyKey)
		idempotencyHash := ledger.ComputeIdempotencyHash(input)
		log.IdempotencyHash = idempotencyHash
	}

	// Chain log with previous log
	log = log.ChainLog(lastLog)

	// If not dry run, apply the log via Raft
	if !parameters.DryRun {
		// Serialize the log as an array (FSM expects an array of logs)
		logsArray := []ledger.Log{log}
		logData, err := json.Marshal(logsArray)
		if err != nil {
			return nil, nil, fmt.Errorf("marshaling logs: %w", err)
		}

		// Apply the logs via Raft (FSM will persist them to the store)
		future := l.raft.Apply(logData, 10*time.Second)
		if err := future.Error(); err != nil {
			return nil, nil, fmt.Errorf("applying logs via raft: %w", err)
		}

		// Check if FSM returned an error
		if future.Response() != nil {
			if err, ok := future.Response().(error); ok {
				return nil, nil, fmt.Errorf("fsm error: %w", err)
			}
		}

		l.logger.Debug("Logs applied via Raft successfully", zap.Int("count", len(logsArray)))
	}

	return &log, createdTx, nil
}

// RevertTransaction is not implemented yet
func (l *DefaultLedger) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	return nil, nil, ErrNotFound
}

// SaveTransactionMetadata is not implemented yet
func (l *DefaultLedger) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

// SaveAccountMetadata is not implemented yet
func (l *DefaultLedger) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

// DeleteTransactionMetadata is not implemented yet
func (l *DefaultLedger) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

// DeleteAccountMetadata is not implemented yet
func (l *DefaultLedger) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

// Import is not implemented yet
func (l *DefaultLedger) Import(ctx context.Context, stream chan ledger.Log) error {
	return ErrNotFound
}

// Export is not implemented yet
func (l *DefaultLedger) Export(ctx context.Context, w ExportWriter) error {
	return ErrNotFound
}
