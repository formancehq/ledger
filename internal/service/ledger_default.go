package service

import (
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"

	"context"
	"fmt"
	"math/big"
	"sync"

	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/numscript"
)

// DefaultLedger is the default implementation of the Ledger interface
type DefaultLedger struct {
	logWriter          LogWriter // Writes logs via Raft
	lockedVolumesStore LockedBalancesStore
	logStore           LogStore // Needed for GetLastLog, GetLogWithIdempotencyKey, and GetAllLogs
	logger             logging.Logger
	nextLogIDs         map[string]uint64 // Counter for log IDs per ledger
	nextLogIDMutex     sync.RWMutex      // Protects nextLogIDs access
}

// NewDefaultLedger creates a new default ledger service
func NewDefaultLedger(logWriter LogWriter, lockedVolumesStore LockedBalancesStore, logStore LogStore, logger logging.Logger) *DefaultLedger {
	return &DefaultLedger{
		logWriter:          logWriter,
		lockedVolumesStore: lockedVolumesStore,
		logStore:           logStore,
		logger:             logger,
		nextLogIDs:         make(map[string]uint64),
	}
}

// getNextLogID returns the next log ID for a ledger and increments the counter (thread-safe)
// It initializes the counter from the last log if not already initialized
func (l *DefaultLedger) getNextLogID(ctx context.Context, ledgerName string) (uint64, error) {
	// First, check if counter is already initialized (read lock)
	l.nextLogIDMutex.RLock()
	_, exists := l.nextLogIDs[ledgerName]
	l.nextLogIDMutex.RUnlock()

	if !exists {
		// Need to initialize counter, acquire write lock
		l.nextLogIDMutex.Lock()
		// Double-check after acquiring write lock
		_, exists = l.nextLogIDs[ledgerName]
		if !exists {
			// Initialize counter from last log
			lastLog, err := l.logStore.GetLastLog(ctx, ledgerName)
			if err != nil {
				l.nextLogIDMutex.Unlock()
				return 0, fmt.Errorf("getting last log to initialize counter: %w", err)
			}

			var counter uint64
			if lastLog != nil && lastLog.ID != nil {
				// Initialize counter to last log ID + 1
				counter = *lastLog.ID + 1
				l.logger.WithFields(map[string]any{"ledger": ledgerName, "lastLogID": *lastLog.ID, "nextLogID": counter}).Infof("Initialized log ID counter from last log")
			} else {
				// No logs yet, start at 1
				counter = 1
				l.logger.WithFields(map[string]any{"ledger": ledgerName}).Infof("Initialized log ID counter to 1 (no previous logs)")
			}
			l.nextLogIDs[ledgerName] = counter
		}
		l.nextLogIDMutex.Unlock()
	}

	// Get current ID and increment (need write lock)
	l.nextLogIDMutex.Lock()
	defer l.nextLogIDMutex.Unlock()

	// Get counter and increment
	counter := l.nextLogIDs[ledgerName]
	currentID := counter
	l.nextLogIDs[ledgerName] = counter + 1

	return currentID, nil
}

// CreateTransaction creates a new transaction
func (l *DefaultLedger) CreateTransaction(ctx context.Context, ledgerName string, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	input := parameters.Input

	// Validate that we have either postings or script, but not both
	hasPostings := len(input.Postings) > 0
	hasScript := input.Script != nil && input.Script.Plain != ""

	if hasPostings && hasScript {
		return nil, nil, fmt.Errorf("cannot pass postings and numscript in the same request")
	}

	if !hasPostings && !hasScript {
		return nil, nil, fmt.Errorf("you need to pass either a posting array or a numscript script")
	}

	var (
		// If script is provided, compile and execute it to generate postings
		scriptMetadata        metadata.Metadata
		scriptAccountMetadata map[string]metadata.Metadata
	)
	if hasScript {
		postings, metadata, accountMetadata, err := l.executeNumscript(ctx, ledgerName, input.Script)
		if err != nil {
			return nil, nil, fmt.Errorf("executing numscript: %w", err)
		}
		input.Postings = postings
		scriptMetadata = metadata
		scriptAccountMetadata = accountMetadata
	}

	// Check idempotency: if idempotency key is provided, check if a log already exists
	if parameters.IdempotencyKey != "" {
		// todo: get from hot storage
		existingLog, err := l.logStore.GetLogWithIdempotencyKey(ctx, ledgerName, parameters.IdempotencyKey)
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
			// Assign log ID to transaction
			if existingLog.ID != nil {
				createdTx.Transaction = createdTx.Transaction.WithID(*existingLog.ID)
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

	// Lock and check sufficient funds for all source accounts
	balances, release, err := l.lockedVolumesStore.LockBalances(ctx, ledgerName, balanceQuery)
	if err != nil {
		// GetBalance failed in LockBalances, return the error
		// Locks are already released in LockBalances on error
		return nil, nil, err
	}

	// Ensure locks are released when we're done
	defer release()

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

	// Get next log ID from counter
	nextLogID, err := l.getNextLogID(ctx, ledgerName)
	if err != nil {
		return nil, nil, fmt.Errorf("getting next log ID: %w", err)
	}

	// Determine timestamp: use provided timestamp or current time if not provided
	timestamp := input.Timestamp
	now := time.Now()
	if timestamp == nil {
		timestamp = &now
	}

	// Create transaction (metadata will be set later after merging with script metadata)
	tx := ledger.NewTransaction().
		WithPostings(input.Postings...).
		WithTimestamp(*timestamp).
		WithInsertedAt(now).
		WithUpdatedAt(now)

	if input.Reference != "" {
		tx = tx.WithReference(input.Reference)
	}

	// Merge script metadata with input metadata
	finalMetadata := input.Metadata
	if scriptMetadata != nil {
		if finalMetadata == nil {
			finalMetadata = make(metadata.Metadata)
		}
		// Check for metadata conflicts
		for k, v := range scriptMetadata {
			if existing, exists := finalMetadata[k]; exists && existing != v {
				return nil, nil, fmt.Errorf("metadata key '%s' conflicts: script sets '%s' but input provides '%s'", k, v, existing)
			}
			finalMetadata[k] = v
		}
	}

	// Merge script account metadata with input account metadata
	finalAccountMetadata := input.AccountMetadata
	if scriptAccountMetadata != nil {
		if finalAccountMetadata == nil {
			finalAccountMetadata = make(map[string]metadata.Metadata)
		}
		for account, accountMeta := range scriptAccountMetadata {
			if existing, exists := finalAccountMetadata[account]; exists {
				// Merge metadata for this account
				for k, v := range accountMeta {
					if existingValue, exists := existing[k]; exists && existingValue != v {
						return nil, nil, fmt.Errorf("account metadata key '%s' for account '%s' conflicts: script sets '%s' but input provides '%s'", k, account, v, existingValue)
					}
					existing[k] = v
				}
				finalAccountMetadata[account] = existing
			} else {
				finalAccountMetadata[account] = accountMeta
			}
		}
	}

	// Update transaction with final metadata
	tx = tx.WithMetadata(finalMetadata)

	// Create CreatedTransaction payload
	createdTx := &ledger.CreatedTransaction{
		Transaction:     tx,
		AccountMetadata: ledger.AccountMetadata(finalAccountMetadata),
	}

	// Create log with ID from counter
	log := ledger.NewLog(createdTx).
		WithDate(*timestamp).
		WithLedger(ledgerName).
		WithID(nextLogID)

	// Assign log ID to transaction
	createdTx.Transaction = createdTx.Transaction.WithID(nextLogID)

	if parameters.IdempotencyKey != "" {
		log = log.WithIdempotencyKey(parameters.IdempotencyKey)
		idempotencyHash := ledger.ComputeIdempotencyHash(input)
		log.IdempotencyHash = idempotencyHash
	}

	// If not dry run, write the log via LogWriter (which will use Raft)
	if !parameters.DryRun {
		if err := l.logWriter.InsertLogs(ctx, log); err != nil {
			return nil, nil, fmt.Errorf("inserting logs: %w", err)
		}

		l.logger.WithFields(map[string]any{"count": 1}).Debugf("Logs written successfully")
	}

	return &log, createdTx, nil
}

// executeNumscript compiles and executes a numscript script to generate postings, metadata, and account metadata
func (l *DefaultLedger) executeNumscript(ctx context.Context, ledgerName string, script *TransactionScript) (ledger.Postings, metadata.Metadata, map[string]metadata.Metadata, error) {
	if script == nil || script.Plain == "" {
		return nil, nil, nil, fmt.Errorf("script is required")
	}

	// Parse the numscript
	parseResult := numscript.Parse(script.Plain)

	// Check for parsing errors
	parsingErrors := parseResult.GetParsingErrors()
	if len(parsingErrors) > 0 {
		return nil, nil, nil, fmt.Errorf("failed to parse numscript: %s", numscript.ParseErrorsToString(parsingErrors, parseResult.GetSource()))
	}

	// Create a store adapter that uses our balance store
	storeAdapter := &numscriptStoreAdapter{
		ledgerName:         ledgerName,
		lockedVolumesStore: l.lockedVolumesStore,
		logStore:           l.logStore,
		ctx:                ctx,
	}

	// Execute the script
	result, err := parseResult.Run(ctx, script.Vars, storeAdapter)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to execute numscript: %w", err)
	}

	// Convert result postings to ledger.Postings
	postings := make(ledger.Postings, 0, len(result.Postings))
	for _, p := range result.Postings {
		// numscript.Posting.Amount is already a *big.Int
		postings = append(postings, ledger.NewPosting(p.Source, p.Destination, p.Asset, p.Amount))
	}

	// Convert numscript metadata to our metadata format
	var txMetadata metadata.Metadata
	if result.Metadata != nil {
		txMetadata = make(metadata.Metadata)
		for k, v := range result.Metadata {
			txMetadata[k] = v.String()
		}
	}

	// Convert numscript account metadata to our format
	var accountMetadata map[string]metadata.Metadata
	if result.AccountsMetadata != nil {
		accountMetadata = make(map[string]metadata.Metadata)
		for account, accountMeta := range result.AccountsMetadata {
			accountMetadata[account] = accountMeta
		}
	}

	return postings, txMetadata, accountMetadata, nil
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

type numscriptStoreAdapter struct {
	ledgerName         string
	lockedVolumesStore LockedBalancesStore
	logStore           LogStore
	ctx                context.Context
}

func (s *numscriptStoreAdapter) GetBalances(ctx context.Context, q numscript.BalanceQuery) (numscript.Balances, error) {
	// Convert numscript.BalanceQuery to our format
	balanceQuery := make(map[string][]string)
	for account, assets := range q {
		balanceQuery[account] = assets
	}

	// Get balances using our locked volumes store
	balances, _, err := s.lockedVolumesStore.LockBalances(ctx, s.ledgerName, balanceQuery)
	if err != nil {
		return nil, err
	}

	// Convert to numscript.Balances format
	result := make(numscript.Balances)
	for account, accountBalances := range balances {
		result[account] = make(map[string]*big.Int)
		for asset, balance := range accountBalances {
			result[account][asset] = balance
		}
	}

	return result, nil
}

// todo: implements GetAccountsMetadata
func (s *numscriptStoreAdapter) GetAccountsMetadata(ctx context.Context, q numscript.MetadataQuery) (numscript.AccountsMetadata, error) {
	// For now, return empty metadata as we don't have account metadata stored separately
	// This can be enhanced later if needed
	result := make(numscript.AccountsMetadata)
	for address := range q {
		result[address] = make(map[string]string)
	}
	return result, nil
}
