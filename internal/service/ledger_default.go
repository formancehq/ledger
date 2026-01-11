package service

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/numscript"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ledger_default.go -destination ledger_default_generated_test.go -package service . LogFactory
type LogFactory interface {
	CreateLog(ctx context.Context, idempotency *ledgerpb.Idempotency, payload *ledgerpb.CommandInput) (*ledgerpb.Log, error)
}

// DefaultLedger is the default implementation of the Ledger interface
type DefaultLedger struct {
	keySetLocker KeySetLocker
	logReader    LogReader
	logFactory   LogFactory
	logger       logging.Logger
	runtimeStore RuntimeStore
	// todo: use a LRU cache with limits
	scriptCache               sync.Map // Cache for parsed numscript scripts: map[string]numscript.ParseResult
	createTransactionLp       *logProcessor[*ledgerpb.CreateTransactionRequestPayload]
	saveTransactionMetadataLp *logProcessor[*ledgerpb.SaveTransactionMetadataRequestPayload]
	saveAccountMetadataLp     *logProcessor[*ledgerpb.SaveAccountMetadataRequestPayload]
}

// NewDefaultLedger creates a new default ledger service
func NewDefaultLedger(
	logReader LogReader,
	logFactory LogFactory,
	runtimeStore RuntimeStore,
	logger logging.Logger,
) *DefaultLedger {
	l := &DefaultLedger{
		keySetLocker: NewDefaultKeySetLocker(),
		logReader:    logReader,
		runtimeStore: runtimeStore,
		logger:       logger,
	}
	l.createTransactionLp = newLogProcessor(
		"CreateTransaction",
		runtimeStore,
		logReader,
		logFactory,
		l.keySetLocker,
		l.createTransaction,
	)
	l.saveTransactionMetadataLp = newLogProcessor(
		"SaveTransactionMetadata",
		runtimeStore,
		logReader,
		logFactory,
		l.keySetLocker,
		l.saveTransactionMetadata,
	)
	l.saveAccountMetadataLp = newLogProcessor(
		"SaveAccountMetadata",
		runtimeStore,
		logReader,
		logFactory,
		l.keySetLocker,
		l.saveAccountMetadata,
	)

	return l
}

// CreateTransaction creates a new transaction
func (l *DefaultLedger) CreateTransaction(ctx context.Context, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, error) {
	log, _, err := l.createTransactionLp.forgeLog(ctx, parameters)
	return log, err
}

func (l *DefaultLedger) createTransaction(ctx context.Context, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.CommandInput, error) {
	l.logger.Debugf("Creating transaction")

	input := parameters.Input

	// Validate that we have either postings or script, but not both
	hasScript := input.Script != nil && input.Script.Plain != ""

	if len(parameters.Input.Postings) > 0 && hasScript {
		return nil, fmt.Errorf("cannot pass postings and numscript in the same request")
	}

	if len(parameters.Input.Postings) == 0 && !hasScript {
		return nil, fmt.Errorf("you need to pass either a posting array or a numscript script")
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
		postings, scriptMetadata, scriptAccountMetadata, err = l.executeNumscript(ctx, script)
		if err != nil {
			return nil, fmt.Errorf("executing numscript: %w", err)
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

		// Convert balanceQuery to the format expected by the balance query
		balanceQueryList := make(map[string][]string)
		for account, assets := range balanceQuery {
			assetList := make([]string, 0, len(assets))
			for asset := range assets {
				assetList = append(assetList, asset)
			}
			balanceQueryList[account] = assetList
		}

		// Lock and check sufficient funds for all source accounts
		lockKeys := makeBalanceLockKeys(balanceQueryList)
		release, err := l.keySetLocker.LockKeys(ctx, lockKeys...)
		if err != nil {
			return nil, err
		}
		defer release()

		balances, err := l.runtimeStore.GetBalances(ctx, balanceQueryList)
		if err != nil {
			return nil, err
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
					return nil, ErrInsufficientFunds
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
				return nil, fmt.Errorf("metadata key '%s' conflicts: script sets '%s' but input provides '%s'", k, v, existing)
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
					return nil, fmt.Errorf("account metadata key '%s' for account '%s' conflicts: script sets '%s' but input provides '%s'", k, account, v, existingValue)
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

	return &ledgerpb.CommandInput{
		Command: &ledgerpb.CommandInput_AppendTransaction{
			AppendTransaction: &ledgerpb.AppendTransactionCommand{
				AccountMetadata: accountMetadataProto,
				Metadata:        finalMetadata,
				Timestamp:       parameters.Input.Timestamp,
				Reference:       parameters.Input.Reference,
				Postings:        postings,
			},
		},
	}, nil
}

// executeNumscript compiles and executes a numscript script to generate postings, metadata, and account metadata
func (l *DefaultLedger) executeNumscript(ctx context.Context, script *ledgerpb.Script) ([]*ledgerpb.Posting, metadata.Metadata, map[string]metadata.Metadata, error) {
	if script == nil || script.Plain == "" {
		return nil, nil, nil, fmt.Errorf("script is required")
	}

	// Check cache first
	scriptKey := script.Plain
	cached, ok := l.scriptCache.Load(scriptKey)
	var parseResult numscript.ParseResult
	if ok {
		parseResult = cached.(numscript.ParseResult)
	} else {
		// Parse the numscript
		parseResult = numscript.Parse(script.Plain)

		// Check for parsing errors
		parsingErrors := parseResult.GetParsingErrors()
		if len(parsingErrors) > 0 {
			return nil, nil, nil, fmt.Errorf("failed to parse numscript: %s", numscript.ParseErrorsToString(parsingErrors, parseResult.GetSource()))
		}

		// Cache the parsed result only if parsing succeeded
		l.scriptCache.Store(scriptKey, parseResult)
	}

	// Create a store adapter that uses our balance store
	// todo: need release when the transaction is committed
	storeAdapter := &numscriptStoreAdapter{
		keySetLocker: l.keySetLocker,
		runtimeStore: l.runtimeStore,
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
func (l *DefaultLedger) RevertTransaction(ctx context.Context, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, error) {
	return nil, ErrNotFound
}

// SaveTransactionMetadata saves metadata for a transaction
func (l *DefaultLedger) SaveTransactionMetadata(ctx context.Context, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	log, _, err := l.saveTransactionMetadataLp.forgeLog(ctx, parameters)
	return log, err
}

func (l *DefaultLedger) saveTransactionMetadata(ctx context.Context, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.CommandInput, error) {
	input := parameters.Input

	// Validate input
	if input.TransactionId == 0 {
		return nil, fmt.Errorf("transaction id is required")
	}
	if input.Metadata == nil {
		return nil, fmt.Errorf("metadata is required")
	}

	return &ledgerpb.CommandInput{
		Command: &ledgerpb.CommandInput_SaveMetadata{
			SaveMetadata: &ledgerpb.SaveMetadataCommand{
				Target: &ledgerpb.Target{
					Target: &ledgerpb.Target_Transaction{
						Transaction: &ledgerpb.TargetTransaction{
							Id: input.TransactionId,
						},
					},
				},
				Metadata: input.Metadata,
			},
		},
	}, nil
}

// SaveAccountMetadata saves metadata for an account
func (l *DefaultLedger) SaveAccountMetadata(ctx context.Context, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	log, _, err := l.saveAccountMetadataLp.forgeLog(ctx, parameters)
	return log, err
}

func (l *DefaultLedger) saveAccountMetadata(ctx context.Context, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.CommandInput, error) {
	input := parameters.Input

	// Validate input
	if input.Address == "" {
		return nil, fmt.Errorf("account address is required")
	}
	if input.Metadata == nil {
		return nil, fmt.Errorf("metadata is required")
	}

	return &ledgerpb.CommandInput{
		Command: &ledgerpb.CommandInput_SaveMetadata{
			SaveMetadata: &ledgerpb.SaveMetadataCommand{
				Target: &ledgerpb.Target{
					Target: &ledgerpb.Target_Account{
						Account: &ledgerpb.TargetAccount{
							Addr: input.Address,
						},
					},
				},
				Metadata: input.Metadata,
			},
		},
	}, nil
}

// DeleteTransactionMetadata is not implemented yet
func (l *DefaultLedger) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return nil, ErrNotFound
}

// DeleteAccountMetadata is not implemented yet
func (l *DefaultLedger) DeleteAccountMetadata(ctx context.Context, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return nil, ErrNotFound
}

// Import is not implemented yet
func (l *DefaultLedger) Import(ctx context.Context, stream chan *ledgerpb.Log) error {
	return ErrNotFound
}

// Export is not implemented yet
func (l *DefaultLedger) Export(ctx context.Context, w ExportWriter) error {
	return ErrNotFound
}

func (l *DefaultLedger) GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error) {
	return l.logReader.GetAllLogs(ctx, from, to)
}

type numscriptStoreAdapter struct {
	keySetLocker KeySetLocker
	runtimeStore RuntimeStore
}

func (s *numscriptStoreAdapter) GetBalances(ctx context.Context, q numscript.BalanceQuery) (numscript.Balances, error) {
	// Convert numscript.BalanceQuery to our format
	balanceQuery := make(map[string][]string)
	for account, assets := range q {
		balanceQuery[account] = assets
	}

	lockKeys := makeBalanceLockKeys(balanceQuery)
	release, err := s.keySetLocker.LockKeys(ctx, lockKeys...)
	if err != nil {
		return nil, err
	}
	defer release()

	balances, err := s.runtimeStore.GetBalances(ctx, balanceQuery)
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

func makeBalanceLockKeys(balanceQuery map[string][]string) []string {
	lockKeys := make([]string, 0)
	for account, assets := range balanceQuery {
		for _, asset := range assets {
			lockKeys = append(lockKeys, fmt.Sprintf("%s:%s", account, asset))
		}
	}
	return lockKeys
}
