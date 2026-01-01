package service

import (
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"

	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/numscript"
)

// DefaultLedger is the default implementation of the Ledger interface
type DefaultLedger struct {
	logWriter          LogWriter // Writes logs via Raft
	lockedVolumesStore LockedBalancesStore
	logReader          LogReader
	logger             logging.Logger
	runtimeStore       RuntimeStore
}

// NewDefaultLedger creates a new default ledger service
func NewDefaultLedger(
	logWriter LogWriter,
	lockedVolumesStore LockedBalancesStore,
	logReader LogReader,
	runtimeStore RuntimeStore,
	logger logging.Logger,
) *DefaultLedger {
	return &DefaultLedger{
		logWriter:          logWriter,
		lockedVolumesStore: lockedVolumesStore,
		logReader:          logReader,
		runtimeStore:       runtimeStore,
		logger:             logger,
	}
}

// checkIdempotency checks if a log with the given idempotency key already exists.
// If it exists and the hash matches, returns the existing log.
// If it exists but the hash doesn't match, returns ErrIdempotencyKeyConflict.
// If it doesn't exist, returns nil, nil.
func (l *DefaultLedger) checkIdempotency(ctx context.Context, idempotencyKey string, input interface{}) (*ledgerpb.Log, error) {
	if idempotencyKey == "" {
		return nil, nil
	}

	idempotencyHash, existingLogID, err := l.runtimeStore.GetLogForIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if existingLogID == 0 {
		return nil, nil
	}

	// Log already exists with this idempotency key
	// Verify that the idempotency hash matches
	expectedHash := ledgerpb.ComputeIdempotencyHash(input)
	if idempotencyHash != expectedHash {
		return nil, ErrIdempotencyKeyConflict
	}

	// Same operation, return the existing log
	ret, err := l.logReader.GetAllLogs(ctx, existingLogID-1, existingLogID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = ret.Close()
	}()

	next, err := ret.Next(ctx)
	if err != nil {
		return nil, err
	}

	return next, nil
}

// CreateTransaction creates a new transaction
func (l *DefaultLedger) CreateTransaction(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, *ledgerpb.CreatedTransaction, error) {

	l.logger.
		WithFields(map[string]any{"ledger": ledgerName}).
		Info("Creating transaction")

	input := parameters.Input

	// Validate that we have either postings or script, but not both
	hasScript := input.Script != nil && input.Script.Plain != ""

	if len(parameters.Input.Postings) > 0 && hasScript {
		return nil, nil, fmt.Errorf("cannot pass postings and numscript in the same request")
	}

	if len(parameters.Input.Postings) == 0 && !hasScript {
		return nil, nil, fmt.Errorf("you need to pass either a posting array or a numscript script")
	}

	// Check idempotency: if idempotency key is provided, check if a log already exists
	existingLog, err := l.checkIdempotency(ctx, parameters.IdempotencyKey, input)
	if err != nil {
		return nil, nil, err
	}
	if existingLog != nil {
		// Same transaction, return the existing log
		// Extract CreatedTransaction from protobuf LogPayload
		if existingLog.Data == nil || existingLog.Data.Payload == nil {
			return nil, nil, ErrIdempotencyKeyConflict
		}

		createdTxPayload, ok := existingLog.Data.Payload.(*ledgerpb.LogPayload_CreatedTransaction)
		if !ok {
			return nil, nil, ErrIdempotencyKeyConflict
		}

		createdTx := createdTxPayload.CreatedTransaction
		// Update transaction ID if needed
		if existingLog.Id != 0 && createdTx.Transaction != nil {
			createdTx.Transaction.Id = existingLog.Id
		}

		l.logger.Infof("Returning existing transaction with ID %d", existingLog.Id)
		return existingLog, createdTx, nil
	}

	var (
		// If script is provided, compile and execute it to generate postings
		scriptMetadata        metadata.Metadata
		scriptAccountMetadata map[string]metadata.Metadata
		postings              []*ledgerpb.Posting
	)
	if hasScript {
		script := input.Script
		var err error
		postings, scriptMetadata, scriptAccountMetadata, err = l.executeNumscript(ctx, ledgerName, script)
		if err != nil {
			return nil, nil, fmt.Errorf("executing numscript: %w", err)
		}
	} else {
		postings = input.Postings
		// Group postings by source account and asset to check balances
		// Build balance query: map[account] = [assets]
		balanceQuery := make(map[string]map[string]bool)      // account -> asset -> true (for deduplication)
		requiredFunds := make(map[string]map[string]*big.Int) // account -> asset -> amount

		for _, posting := range postings {
			if posting == nil {
				continue
			}
			if posting.Source == "world" {
				continue // WORLD account has infinite funds
			}

			// Track assets for balance query
			if balanceQuery[posting.Source] == nil {
				balanceQuery[posting.Source] = make(map[string]bool)
			}
			balanceQuery[posting.Source][posting.Asset] = true

			// Track required funds
			if requiredFunds[posting.Source] == nil {
				requiredFunds[posting.Source] = make(map[string]*big.Int)
			}
			if requiredFunds[posting.Source][posting.Asset] == nil {
				requiredFunds[posting.Source][posting.Asset] = big.NewInt(0)
			}
			requiredFunds[posting.Source][posting.Asset].Add(requiredFunds[posting.Source][posting.Asset], posting.Amount.Value())
		}

		// Convert balanceQuery to the format expected by LockBalances
		balanceQueryList := make(map[string][]string)
		for account, assets := range balanceQuery {
			assetList := make([]string, 0, len(assets))
			for asset := range assets {
				assetList = append(assetList, asset)
			}
			balanceQueryList[account] = assetList
		}

		// Lock and check sufficient funds for all source accounts
		balances, release, err := l.lockedVolumesStore.LockBalances(ctx, balanceQueryList)
		if err != nil {
			return nil, nil, err
		}
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
	}

	// Determine timestamp: use provided timestamp or current time if not provided
	var timestamp *time.Time
	if input.Timestamp != nil {
		t := input.Timestamp.AsTime()
		timestamp = &t
	}
	now := time.Now()
	if timestamp == nil {
		timestamp = &now
	}

	// Create transaction (metadata will be set later after merging with script metadata)
	tx := ledgerpb.NewTransaction().
		WithPostings(postings...).
		WithTimestamp(*timestamp).
		WithInsertedAt(now).
		WithUpdatedAt(now)

	if input.Reference != "" {
		tx = tx.WithReference(input.Reference)
	}

	// Merge script metadata with input metadata
	var finalMetadata metadata.Metadata
	if input.Metadata != nil {
		finalMetadata = input.Metadata
	}
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
	finalAccountMetadata := make(map[string]metadata.Metadata)
	for addr, md := range input.AccountMetadata {
		if md != nil && md.Entries != nil {
			finalAccountMetadata[addr] = md.Entries
		}
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

	// Update transaction with final metadata
	tx = tx.WithMetadata(finalMetadata)

	// Convert account metadata to protobuf
	accountMetadataProto := make(map[string]*ledgerpb.Metadata)
	for addr, md := range finalAccountMetadata {
		if len(md) > 0 {
			accountMetadataProto[addr] = &ledgerpb.Metadata{Entries: md}
		}
	}

	// Create CreatedTransaction payload in protobuf
	createdTx := &ledgerpb.CreatedTransaction{
		Transaction:     tx,
		AccountMetadata: accountMetadataProto,
	}

	// Create log payload
	logPayload := &ledgerpb.LogPayload{
		Payload: &ledgerpb.LogPayload_CreatedTransaction{
			CreatedTransaction: createdTx,
		},
	}

	// Create protobuf Log
	log := &ledgerpb.Log{
		Data: logPayload,
	}

	// Set date
	log.Date = ledgerpb.NewTimestamp(*timestamp)

	if parameters.IdempotencyKey != "" {
		log.IdempotencyKey = parameters.IdempotencyKey
		log.IdempotencyHash = ledgerpb.ComputeIdempotencyHash(input)
	}

	// If not dry run, write the log via LogWriter (which will use Raft)
	if !parameters.DryRun {
		l.logger.Info("Writing new log...")
		if err := l.logWriter.InsertLogs(ctx, log); err != nil {
			return nil, nil, fmt.Errorf("inserting logs: %w", err)
		}

		l.logger.Infof("Log written successfully")
	}

	return log, createdTx, nil
}

// executeNumscript compiles and executes a numscript script to generate postings, metadata, and account metadata
func (l *DefaultLedger) executeNumscript(ctx context.Context, ledgerName string, script *ledgerpb.Script) ([]*ledgerpb.Posting, metadata.Metadata, map[string]metadata.Metadata, error) {
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
	// todo: need release when the transaction is committed
	storeAdapter := &numscriptStoreAdapter{
		ledgerName:         ledgerName,
		lockedVolumesStore: l.lockedVolumesStore,
		runtimeStore:       l.runtimeStore,
	}

	// Execute the script
	result, err := parseResult.Run(ctx, script.Vars, storeAdapter)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to execute numscript: %w", err)
	}

	// Convert result postings to []*ledgerpb.Posting
	postings := make([]*ledgerpb.Posting, 0, len(result.Postings))
	for _, p := range result.Postings {
		// numscript.Posting.Amount is already a *big.Int
		postings = append(postings, ledgerpb.NewPosting(p.Source, p.Destination, p.Asset, p.Amount))
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
func (l *DefaultLedger) RevertTransaction(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, *ledgerpb.RevertedTransaction, error) {
	return nil, nil, ErrNotFound
}

// SaveTransactionMetadata is not implemented yet
func (l *DefaultLedger) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return nil, ErrNotFound
}

// SaveAccountMetadata saves metadata for an account
func (l *DefaultLedger) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	input := parameters.Input

	// Validate input
	if input.Address == "" {
		return nil, fmt.Errorf("account address is required")
	}
	if input.Metadata == nil {
		return nil, fmt.Errorf("metadata is required")
	}

	// Check idempotency: if idempotency key is provided, check if a log already exists
	existingLog, err := l.checkIdempotency(ctx, parameters.IdempotencyKey, parameters.Input)
	if err != nil {
		return nil, err
	}
	if existingLog != nil {
		// Same metadata operation, return the existing log
		return existingLog, nil
	}

	// Create SavedMetadata payload in protobuf
	savedMetadata := &ledgerpb.SavedMetadata{
		TargetType: "ACCOUNT",
		TargetId: &ledgerpb.SavedMetadata_AccountId{
			AccountId: input.Address,
		},
		Metadata: input.Metadata,
	}

	// Create log payload
	logPayload := &ledgerpb.LogPayload{
		Payload: &ledgerpb.LogPayload_SavedMetadata{
			SavedMetadata: savedMetadata,
		},
	}

	// Create protobuf Log
	now := time.Now()
	log := &ledgerpb.Log{
		Data: logPayload,
		Date: ledgerpb.NewTimestamp(now),
	}

	if parameters.IdempotencyKey != "" {
		log.IdempotencyKey = parameters.IdempotencyKey
		log.IdempotencyHash = ledgerpb.ComputeIdempotencyHash(input)
	}

	// If not dry run, write the log via LogWriter (which will use Raft)
	if !parameters.DryRun {
		if err := l.logWriter.InsertLogs(ctx, log); err != nil {
			return nil, fmt.Errorf("inserting log: %w", err)
		}

		l.logger.WithFields(map[string]any{"account": input.Address, "count": 1}).Debugf("Account metadata log written successfully")
	}

	return log, nil
}

// DeleteTransactionMetadata is not implemented yet
func (l *DefaultLedger) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return nil, ErrNotFound
}

// DeleteAccountMetadata is not implemented yet
func (l *DefaultLedger) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return nil, ErrNotFound
}

// Import is not implemented yet
func (l *DefaultLedger) Import(ctx context.Context, ledgerName string, stream chan *ledgerpb.Log) error {
	return ErrNotFound
}

// Export is not implemented yet
func (l *DefaultLedger) Export(ctx context.Context, ledgerName string, w ExportWriter) error {
	return ErrNotFound
}

func (l *DefaultLedger) GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error) {
	return l.logReader.GetAllLogs(ctx, from, to)
}

type numscriptStoreAdapter struct {
	ledgerName         string
	lockedVolumesStore LockedBalancesStore
	runtimeStore       RuntimeStore
}

func (s *numscriptStoreAdapter) GetBalances(ctx context.Context, q numscript.BalanceQuery) (numscript.Balances, error) {
	// Convert numscript.BalanceQuery to our format
	balanceQuery := make(map[string][]string)
	for account, assets := range q {
		balanceQuery[account] = assets
	}

	// Get balances using our locked volumes store
	balances, release, err := s.lockedVolumesStore.LockBalances(ctx, balanceQuery)
	if err != nil {
		return nil, err
	}
	// todo: need release (put here for debug but it is not good!!!!)
	release()

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

// GetAccountsMetadata retrieves account metadata for accounts in the query
func (s *numscriptStoreAdapter) GetAccountsMetadata(ctx context.Context, q numscript.MetadataQuery) (numscript.AccountsMetadata, error) {
	// Convert numscript.MetadataQuery (map[string]struct{}) to []string
	accounts := make([]string, 0, len(q))
	for address := range q {
		accounts = append(accounts, address)
	}

	// Get metadata from the log store
	metadataMap, err := s.runtimeStore.GetAccountMetadata(ctx, accounts)
	if err != nil {
		return nil, err
	}

	// Convert to numscript.AccountsMetadata format (map[string]map[string]string)
	result := make(numscript.AccountsMetadata)
	for address, accountMetadata := range metadataMap {
		result[address] = accountMetadata
	}

	// Ensure all requested accounts are in the result (even if empty)
	for address := range q {
		if _, exists := result[address]; !exists {
			result[address] = make(map[string]string)
		}
	}

	return result, nil
}
