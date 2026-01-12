package service

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
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
	logger       logging.Logger
	runtimeStore RuntimeStore
	// todo: use a LRU cache with limits
	scriptCache                 sync.Map // Cache for parsed numscript scripts: map[string]numscript.ParseResult
	createTransactionLp         *logProcessor[*ledgerpb.CreateTransactionRequestPayload]
	saveTransactionMetadataLp   *logProcessor[*ledgerpb.SaveTransactionMetadataRequestPayload]
	saveAccountMetadataLp       *logProcessor[*ledgerpb.SaveAccountMetadataRequestPayload]
	deleteTransactionMetadataLp *logProcessor[*ledgerpb.DeleteTransactionMetadataRequestPayload]
	deleteAccountMetadataLp     *logProcessor[*ledgerpb.DeleteAccountMetadataRequestPayload]
	revertTransactionLp         *logProcessor[*ledgerpb.RevertTransactionRequestPayload]
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
	l.deleteTransactionMetadataLp = newLogProcessor(
		"DeleteTransactionMetadata",
		runtimeStore,
		logReader,
		logFactory,
		l.keySetLocker,
		l.deleteTransactionMetadata,
	)
	l.deleteAccountMetadataLp = newLogProcessor(
		"DeleteAccountMetadata",
		runtimeStore,
		logReader,
		logFactory,
		l.keySetLocker,
		l.deleteAccountMetadata,
	)
	l.revertTransactionLp = newLogProcessor(
		"RevertTransaction",
		runtimeStore,
		logReader,
		logFactory,
		l.keySetLocker,
		l.revertTransaction,
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
		// Check that all source accounts have sufficient funds
		if err := l.checkBalances(ctx, postings); err != nil {
			return nil, err
		}
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
	storeAdapter := &balancesLockedStoreAdapter{
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

// RevertTransaction reverts a transaction by creating a reverse transaction
func (l *DefaultLedger) RevertTransaction(ctx context.Context, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, error) {
	log, _, err := l.revertTransactionLp.forgeLog(ctx, parameters)
	return log, err
}

func (l *DefaultLedger) revertTransaction(ctx context.Context, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.CommandInput, error) {
	input := parameters.Input

	// Validate input
	if input.TransactionId == 0 {
		return nil, fmt.Errorf("transaction id is required")
	}

	// Lock the transaction ID to prevent concurrent revert operations
	lockKey := fmt.Sprintf("tx/revert/%d", input.TransactionId)
	// todo: the release is done before the log is inserted
	release, err := l.keySetLocker.LockKeys(ctx, lockKey)
	if err != nil {
		return nil, fmt.Errorf("locking transaction %d: %w", input.TransactionId, err)
	}
	defer release()

	// Check if transaction is already reverted (fast path using store index)
	isReverted, err := l.runtimeStore.IsTransactionReverted(ctx, input.TransactionId)
	if err != nil {
		return nil, fmt.Errorf("checking if transaction %d is reverted: %w", input.TransactionId, err)
	}
	if isReverted {
		return nil, fmt.Errorf("transaction %d is already reverted", input.TransactionId)
	}

	// Get the log ID for the transaction ID
	logID, err := l.runtimeStore.GetLogIDForTransactionID(ctx, input.TransactionId)
	if err != nil {
		return nil, fmt.Errorf("getting log ID for transaction %d: %w", input.TransactionId, err)
	}
	if logID == 0 {
		return nil, fmt.Errorf("transaction %d not found", input.TransactionId)
	}

	// Get the log containing the original transaction
	log, err := l.logReader.GetLogByID(ctx, logID)
	if err != nil {
		return nil, fmt.Errorf("getting log %d: %w", logID, err)
	}
	if log == nil {
		return nil, fmt.Errorf("log %d not found", logID)
	}

	// Extract the original transaction from the log
	var originalTx *ledgerpb.Transaction
	switch payload := log.Data.Payload.(type) {
	case *ledgerpb.LogPayload_CreatedTransaction:
		if payload.CreatedTransaction == nil || payload.CreatedTransaction.Transaction == nil {
			return nil, fmt.Errorf("invalid log payload: missing transaction")
		}
		originalTx = payload.CreatedTransaction.Transaction
	case *ledgerpb.LogPayload_RevertedTransaction:
		// Transaction already reverted (double-check)
		return nil, fmt.Errorf("transaction %d is already a revert transaction", input.TransactionId)
	default:
		return nil, fmt.Errorf("log %d does not contain a transaction", logID)
	}

	// Create reverse transaction with swapped source/destination
	reversedPostings := make([]*ledgerpb.Posting, len(originalTx.Postings))
	for i, posting := range originalTx.Postings {
		if posting == nil {
			return nil, fmt.Errorf("nil posting at index %d", i)
		}
		reversedPostings[i] = &ledgerpb.Posting{
			Source:      posting.Destination,
			Destination: posting.Source,
			Amount:      posting.Amount,
			Asset:       posting.Asset,
		}
	}

	// Reverse the order of postings
	for i := 0; i < len(reversedPostings)/2; i++ {
		reversedPostings[i], reversedPostings[len(reversedPostings)-i-1] = reversedPostings[len(reversedPostings)-i-1], reversedPostings[i]
	}

	// Validate balances for revert transaction (unless force is true)
	if !input.Force {
		if err := l.checkBalances(ctx, reversedPostings); err != nil {
			return nil, err
		}
	}

	// Merge metadata: original transaction metadata + revert metadata
	revertMetadata := make(map[string]string)
	if originalTx.Metadata != nil {
		for k, v := range originalTx.Metadata {
			revertMetadata[k] = v
		}
	}
	if input.Metadata != nil {
		for k, v := range input.Metadata {
			revertMetadata[k] = v
		}
	}

	// Determine timestamp for revert transaction
	var revertTimestamp *ledgerpb.Timestamp
	if input.AtEffectiveDate && originalTx.Timestamp != nil {
		revertTimestamp = originalTx.Timestamp
	}

	// Create the revert transaction (transaction ID will be assigned by FSM)
	revertTx := &ledgerpb.Transaction{
		Postings:  reversedPostings,
		Metadata:  revertMetadata,
		Timestamp: revertTimestamp,
		Reference: originalTx.Reference,
	}

	return &ledgerpb.CommandInput{
		Command: &ledgerpb.CommandInput_RevertTransaction{
			RevertTransaction: &ledgerpb.RevertTransactionCommand{
				TransactionId:     input.TransactionId,
				RevertTransaction: revertTx,
			},
		},
	}, nil
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

// DeleteTransactionMetadata deletes a metadata key from a transaction
func (l *DefaultLedger) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	log, _, err := l.deleteTransactionMetadataLp.forgeLog(ctx, parameters)
	return log, err
}

func (l *DefaultLedger) deleteTransactionMetadata(ctx context.Context, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.CommandInput, error) {
	input := parameters.Input

	if input.TransactionId == 0 {
		return nil, fmt.Errorf("transaction id is required")
	}
	if input.Key == "" {
		return nil, fmt.Errorf("metadata key is required")
	}

	return &ledgerpb.CommandInput{
		Command: &ledgerpb.CommandInput_DeleteMetadata{
			DeleteMetadata: &ledgerpb.DeleteMetadataCommand{
				Target: &ledgerpb.Target{
					Target: &ledgerpb.Target_Transaction{
						Transaction: &ledgerpb.TargetTransaction{
							Id: input.TransactionId,
						},
					},
				},
				Key: input.Key,
			},
		},
	}, nil
}

// DeleteAccountMetadata deletes a metadata key from an account
func (l *DefaultLedger) DeleteAccountMetadata(ctx context.Context, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	log, _, err := l.deleteAccountMetadataLp.forgeLog(ctx, parameters)
	return log, err
}

func (l *DefaultLedger) deleteAccountMetadata(ctx context.Context, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.CommandInput, error) {
	input := parameters.Input

	if input.Address == "" {
		return nil, fmt.Errorf("account address is required")
	}
	if input.Key == "" {
		return nil, fmt.Errorf("metadata key is required")
	}

	return &ledgerpb.CommandInput{
		Command: &ledgerpb.CommandInput_DeleteMetadata{
			DeleteMetadata: &ledgerpb.DeleteMetadataCommand{
				Target: &ledgerpb.Target{
					Target: &ledgerpb.Target_Account{
						Account: &ledgerpb.TargetAccount{
							Addr: input.Address,
						},
					},
				},
				Key: input.Key,
			},
		},
	}, nil
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

// checkBalances verifies that all source accounts have sufficient funds for the given postings.
// It locks the relevant balance keys, fetches current balances, and returns ErrInsufficientFunds
// if any source account lacks the required funds.
func (l *DefaultLedger) checkBalances(ctx context.Context, postings []*ledgerpb.Posting) error {
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

	// No balance check needed if no non-world sources
	if len(balanceQuery) == 0 {
		return nil
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
		return err
	}
	defer release()

	balances, err := l.runtimeStore.GetBalances(ctx, balanceQueryList)
	if err != nil {
		return err
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
				return ErrInsufficientFunds
			}
		}
	}

	return nil
}
