package service

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math/big"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/formancehq/numscript"
	"google.golang.org/protobuf/proto"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller_default.go -destination controller_default_generated_test.go -package service . LogFactory
type Engine interface {
	Apply(ctx context.Context, cmd *ledgerpb.Command) (any, error)
}

// DefaultController is the default implementation of the Ledger interface
type DefaultController struct {
	logger logging.Logger
	engine Engine
	// todo: use a LRU cache with limits
	scriptCache                 sync.Map // Cache for parsed numscript scripts: map[string]numscript.ParseResult
	createTransactionLp         *logProcessor[*ledgerpb.CreateTransactionRequestPayload]
	saveTransactionMetadataLp   *logProcessor[*ledgerpb.SaveTransactionMetadataRequestPayload]
	saveAccountMetadataLp       *logProcessor[*ledgerpb.SaveAccountMetadataRequestPayload]
	deleteTransactionMetadataLp *logProcessor[*ledgerpb.DeleteTransactionMetadataRequestPayload]
	deleteAccountMetadataLp     *logProcessor[*ledgerpb.DeleteAccountMetadataRequestPayload]
	revertTransactionLp         *logProcessor[*ledgerpb.RevertTransactionRequestPayload]
	store                       store.Store
}

// NewDefaultLedger creates a new default ledger service
func NewDefaultController(
	engine Engine,
	store store.Store,
	logger logging.Logger,
) *DefaultController {
	l := &DefaultController{
		logger: logger,
		engine: engine,
		store:  store,
	}
	keySetLocker := NewDefaultKeySetLocker()
	l.createTransactionLp = newLogProcessor(
		"CreateTransaction",
		store,
		engine,
		keySetLocker,
		logger,
		l.createTransaction,
	)
	l.saveTransactionMetadataLp = newLogProcessor(
		"SaveTransactionMetadata",
		store,
		engine,
		keySetLocker,
		logger,
		l.saveTransactionMetadata,
	)
	l.saveAccountMetadataLp = newLogProcessor(
		"SaveAccountMetadata",
		store,
		engine,
		keySetLocker,
		logger,
		l.saveAccountMetadata,
	)
	l.deleteTransactionMetadataLp = newLogProcessor(
		"DeleteTransactionMetadata",
		store,
		engine,
		keySetLocker,
		logger,
		l.deleteTransactionMetadata,
	)
	l.deleteAccountMetadataLp = newLogProcessor(
		"DeleteAccountMetadata",
		store,
		engine,
		keySetLocker,
		logger,
		l.deleteAccountMetadata,
	)
	l.revertTransactionLp = newLogProcessor(
		"RevertTransaction",
		store,
		engine,
		keySetLocker,
		logger,
		l.revertTransaction,
	)

	return l
}


// GetAllLedgersInfo returns all ledgers
func (ctrl *DefaultController) GetAllLedgersInfo(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error) {
	ledgers, err := ctrl.store.ListLedgers(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing ledgers: %w", err)
	}

	ret := make(map[string]*ledgerpb.LedgerInfo)
	for _, ledger := range ledgers {
		ret[ledger.Name] = ledger
	}
	return ret, nil
}

func (ctrl *DefaultController) GetTransaction(ctx context.Context, ledgerID uint32, transactionID uint64) (*ledgerpb.Transaction, error) {
	// Get the log ID for the transaction ID
	logID, err := ctrl.store.GetLogIDForTransactionID(ctx, ledgerID, transactionID)
	if err != nil {
		return nil, fmt.Errorf("getting log ID for transaction %d: %w", transactionID, err)
	}
	if logID == 0 {
		return nil, ledgerpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	// Get the log containing the transaction
	log, err := ctrl.store.GetLogByID(ctx, ledgerID, logID)
	if err != nil {
		return nil, fmt.Errorf("getting log %d: %w", logID, err)
	}
	if log == nil {
		return nil, ledgerpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	// Extract the transaction from the log payload
	switch payload := log.Data.Payload.(type) {
	case *ledgerpb.LogPayload_CreatedTransaction:
		if payload.CreatedTransaction == nil || payload.CreatedTransaction.Transaction == nil {
			return nil, fmt.Errorf("invalid log payload: missing transaction")
		}
		return payload.CreatedTransaction.Transaction, nil
	case *ledgerpb.LogPayload_RevertedTransaction:
		if payload.RevertedTransaction == nil || payload.RevertedTransaction.RevertTransaction == nil {
			return nil, fmt.Errorf("invalid log payload: missing revert transaction")
		}
		return payload.RevertedTransaction.RevertTransaction, nil
	default:
		return nil, fmt.Errorf("log %d does not contain a transaction", logID)
	}
}

func (ctrl *DefaultController) GetAllLogs(ctx context.Context, ledgerID uint32, from uint64, to uint64) (store.Cursor[*ledgerpb.Log], error) {
	return ctrl.store.GetAllLogs(ctx, ledgerID, from, to)
}

func (ctrl *DefaultController) GetLedgerByName(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	return ctrl.store.GetLedgerByName(ctx, name)
}

// CreateLedger creates a new ledger
func (ctrl *DefaultController) CreateLedger(ctx context.Context, req *ledgerpb.CreateLedgerCommand) (*ledgerpb.LedgerInfo, error) {
	cmd := newCreateLedgerCommand(req)

	ret, err := ctrl.engine.Apply(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("applying create ledger command: %w", err)
	}

	// Extract the ledger info from the result (first element of the results array)
	results := ret.([]any)
	return results[0].(*ledgerpb.LedgerInfo), nil
}

// DeleteLedger deletes a ledger
func (ctrl *DefaultController) DeleteLedger(ctx context.Context, id uint32) error {
	cmd := newDeleteLedgerCommand(id)

	_, err := ctrl.engine.Apply(ctx, cmd)
	if err != nil {
		return fmt.Errorf("applying delete ledger command: %w", err)
	}

	return nil
}

// CreateTransaction creates a new transaction
func (ctrl *DefaultController) CreateTransaction(ctx context.Context, ledgerID uint32, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, error) {
	log, _, err := ctrl.createTransactionLp.forgeLog(ctx, ledgerID, parameters)
	return log, err
}

func (ctrl *DefaultController) createTransaction(ctx context.Context, unitOfWork *unitOfWork, ledgerID uint32, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.CommandInput, error) {
	ctrl.logger.Debugf("Creating transaction")

	input := parameters.Input

	// Validate that we have either postings or script, but not both
	hasScript := input.Script != nil && input.Script.Plain != ""

	if len(parameters.Input.Postings) > 0 && hasScript {
		return nil, fmt.Errorf("cannot pass postings and numscript in the same request")
	}

	if len(parameters.Input.Postings) == 0 && !hasScript {
		return nil, fmt.Errorf("you need to pass either a posting array or a numscript script")
	}

	if input.Reference != "" {
		_, err := unitOfWork.LockKeys(ctx, ledgerID, fmt.Sprintf("tx/references/%s", input.Reference))
		if err != nil {
			return nil, fmt.Errorf("locking reference %s: %w", input.Reference, err)
		}
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
		postings, scriptMetadata, scriptAccountMetadata, err = ctrl.executeNumscript(ctx, unitOfWork, ledgerID, script)
		if err != nil {
			return nil, fmt.Errorf("executing numscript: %w", err)
		}
	} else {
		postings = input.Postings
		// Check that all source accounts have sufficient funds
		if err := ctrl.checkBalances(ctx, unitOfWork, ledgerID, postings); err != nil {
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
func (ctrl *DefaultController) executeNumscript(ctx context.Context, uow *unitOfWork, ledgerID uint32, script *ledgerpb.Script) ([]*ledgerpb.Posting, metadata.Metadata, map[string]metadata.Metadata, error) {
	if script == nil || script.Plain == "" {
		return nil, nil, nil, fmt.Errorf("script is required")
	}

	// Check cache first
	scriptKey := script.Plain
	cached, ok := ctrl.scriptCache.Load(scriptKey)
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
		ctrl.scriptCache.Store(scriptKey, parseResult)
	}

	// Create numscriptStore wrapper with ledgerID for numscript interface methods
	store := &numscriptStore{unitOfWork: uow, ledgerID: ledgerID}

	// Execute the script
	result, err := parseResult.Run(ctx, script.Vars, store)
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
func (ctrl *DefaultController) RevertTransaction(ctx context.Context, ledgerID uint32, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, error) {
	log, _, err := ctrl.revertTransactionLp.forgeLog(ctx, ledgerID, parameters)
	return log, err
}

func (ctrl *DefaultController) revertTransaction(ctx context.Context, unitOfWork *unitOfWork, ledgerID uint32, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.CommandInput, error) {
	input := parameters.Input

	// Validate input
	if input.TransactionId == 0 {
		return nil, fmt.Errorf("transaction id is required")
	}

	// Lock the transaction ID to prevent concurrent revert operations
	lockKey := fmt.Sprintf("tx/revert/%d", input.TransactionId)
	_, err := unitOfWork.LockKeys(ctx, ledgerID, lockKey)
	if err != nil {
		return nil, fmt.Errorf("locking transaction %d: %w", input.TransactionId, err)
	}

	// Check if transaction is already reverted (fast path using store index)
	isReverted, err := unitOfWork.IsTransactionReverted(ctx, ledgerID, input.TransactionId)
	if err != nil {
		return nil, fmt.Errorf("checking if transaction %d is reverted: %w", input.TransactionId, err)
	}
	if isReverted {
		return nil, fmt.Errorf("transaction %d is already reverted", input.TransactionId)
	}

	// Get the log ID for the transaction ID
	logID, err := unitOfWork.GetLogIDForTransactionID(ctx, ledgerID, input.TransactionId)
	if err != nil {
		return nil, fmt.Errorf("getting log ID for transaction %d: %w", input.TransactionId, err)
	}
	if logID == 0 {
		return nil, fmt.Errorf("transaction %d not found", input.TransactionId)
	}

	// Get the log containing the original transaction
	log, err := unitOfWork.GetLogByID(ctx, ledgerID, logID)
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
		if err := ctrl.checkBalances(ctx, unitOfWork, ledgerID, reversedPostings); err != nil {
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
func (ctrl *DefaultController) SaveTransactionMetadata(ctx context.Context, ledgerID uint32, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	log, _, err := ctrl.saveTransactionMetadataLp.forgeLog(ctx, ledgerID, parameters)
	return log, err
}

func (ctrl *DefaultController) saveTransactionMetadata(ctx context.Context, store *unitOfWork, ledgerID uint32, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.CommandInput, error) {
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
func (ctrl *DefaultController) SaveAccountMetadata(ctx context.Context, ledgerID uint32, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	log, _, err := ctrl.saveAccountMetadataLp.forgeLog(ctx, ledgerID, parameters)
	return log, err
}

func (ctrl *DefaultController) saveAccountMetadata(ctx context.Context, store *unitOfWork, ledgerID uint32, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.CommandInput, error) {
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
func (ctrl *DefaultController) DeleteTransactionMetadata(ctx context.Context, ledgerID uint32, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	log, _, err := ctrl.deleteTransactionMetadataLp.forgeLog(ctx, ledgerID, parameters)
	return log, err
}

func (ctrl *DefaultController) deleteTransactionMetadata(ctx context.Context, store *unitOfWork, ledgerID uint32, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.CommandInput, error) {
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
func (ctrl *DefaultController) DeleteAccountMetadata(ctx context.Context, ledgerID uint32, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	log, _, err := ctrl.deleteAccountMetadataLp.forgeLog(ctx, ledgerID, parameters)
	return log, err
}

func (ctrl *DefaultController) deleteAccountMetadata(ctx context.Context, store *unitOfWork, ledgerID uint32, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.CommandInput, error) {
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
func (ctrl *DefaultController) Import(ctx context.Context, ledgerID uint32, stream chan *ledgerpb.Log) error {
	return fmt.Errorf("import is not implemented yet")
}

// Export is not implemented yet
func (ctrl *DefaultController) Export(ctx context.Context, ledgerID uint32, w ExportWriter) error {
	return fmt.Errorf("export is not implemented yet")
}

// checkBalances verifies that all source accounts have sufficient funds for the given postings.
// It locks the relevant balance keys, fetches current balances, and returns ErrInsufficientFunds
// if any source account lacks the required funds.
func (ctrl *DefaultController) checkBalances(ctx context.Context, uow *unitOfWork, ledgerID uint32, postings []*ledgerpb.Posting) error {
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

	// Lock balance keys
	lockKeys := makeBalanceLockKeys(balanceQueryList)
	_, err := uow.LockKeys(ctx, ledgerID, lockKeys...)
	if err != nil {
		return err
	}

	balances, err := uow.Store.GetBalances(ctx, ledgerID, balanceQueryList)
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

var _ Controller = (*DefaultController)(nil)

// newCreateLedgerCommand creates a new CreateLedger command
func newCreateLedgerCommand(req *ledgerpb.CreateLedgerCommand) *ledgerpb.Command {
	data, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}

	action := &ledgerpb.Action{
		ActionType: ledgerpb.ActionType_CreateLedger,
		Data:       data,
	}

	return &ledgerpb.Command{
		Id:      generateCommandID(),
		Actions: []*ledgerpb.Action{action},
		Date:    ledgerpb.NewTimestamp(time.Now()),
	}
}

// newDeleteLedgerCommand creates a new DeleteLedger command
func newDeleteLedgerCommand(id uint32) *ledgerpb.Command {
	data, err := proto.Marshal(&ledgerpb.DeleteLedgerCommand{Id: id})
	if err != nil {
		panic(err)
	}

	action := &ledgerpb.Action{
		ActionType: ledgerpb.ActionType_DeleteLedger,
		Data:       data,
	}

	return &ledgerpb.Command{
		Id:      generateCommandID(),
		Actions: []*ledgerpb.Action{action},
		Date:    ledgerpb.NewTimestamp(time.Now()),
	}
}

// generateCommandID generates a random uint64 ID for commands
func generateCommandID() uint64 {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint64(b[:])
}
