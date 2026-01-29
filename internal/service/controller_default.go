package service

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/formancehq/numscript"
	"google.golang.org/protobuf/proto"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller_default.go -destination controller_default_generated_test.go -package service . LogFactory
type Engine interface {
	Apply(ctx context.Context, actions ...*raftcmdpb.Action) ([]*commonpb.Log, error)
}

// DefaultController is the default implementation of the Ledger interface
type DefaultController struct {
	logger logging.Logger
	engine Engine
	// todo: use a LRU cache with limits
	scriptCache   sync.Map // Cache for parsed numscript scripts: map[string]numscript.ParseResult
	ledgerIDCache sync.Map // Cache for ledger name to ID: map[string]uint32
	store         store.Store
	keySetLocker  KeySetLocker
}

// NewDefaultLedger creates a new default ledger service
func NewDefaultController(
	engine Engine,
	store store.Store,
	logger logging.Logger,
) *DefaultController {
	return &DefaultController{
		logger:       logger,
		engine:       engine,
		store:        store,
		keySetLocker: NewDefaultKeySetLocker(),
	}
}

// GetAllLedgersInfo returns a cursor over all ledgers
func (ctrl *DefaultController) GetAllLedgersInfo(ctx context.Context) (store.Cursor[*commonpb.LedgerInfo], error) {
	return ctrl.store.ListLedgers(ctx)
}

func (ctrl *DefaultController) GetTransaction(ctx context.Context, ledgerID uint32, transactionID uint64) (*commonpb.Transaction, error) {
	// Get the sequence for the transaction ID
	sequence, err := ctrl.store.GetSequenceForTransactionID(ctx, ledgerID, transactionID)
	if err != nil {
		return nil, fmt.Errorf("getting sequence for transaction %d: %w", transactionID, err)
	}
	if sequence == 0 {
		return nil, commonpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	// Get the system log containing the transaction
	log, err := ctrl.store.GetLogBySequence(ctx, sequence)
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
	switch payload := ledgerLog.Data.Payload.(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if payload.CreatedTransaction == nil || payload.CreatedTransaction.Transaction == nil {
			return nil, fmt.Errorf("invalid log payload: missing transaction")
		}
		tx := payload.CreatedTransaction.Transaction
		// Check if the transaction has been reverted
		reverted, err := ctrl.store.IsTransactionReverted(ctx, ledgerID, transactionID)
		if err != nil {
			return nil, fmt.Errorf("checking if transaction %d is reverted: %w", transactionID, err)
		}
		tx.Reverted = reverted
		return tx, nil
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if payload.RevertedTransaction == nil || payload.RevertedTransaction.RevertTransaction == nil {
			return nil, fmt.Errorf("invalid log payload: missing revert transaction")
		}
		return payload.RevertedTransaction.RevertTransaction, nil
	default:
		return nil, fmt.Errorf("ledger log %d does not contain a transaction", ledgerLog.Id)
	}
}

func (ctrl *DefaultController) GetAccount(ctx context.Context, ledgerID uint32, address string) (*commonpb.Account, error) {
	// Get account metadata
	metadataMap, err := ctrl.store.GetAccountMetadata(ctx, ledgerID, []string{address})
	if err != nil {
		return nil, fmt.Errorf("getting account metadata: %w", err)
	}

	// Get account volumes
	volumes, err := ctrl.store.GetAccountVolumes(ctx, ledgerID, address)
	if err != nil {
		return nil, fmt.Errorf("getting account volumes: %w", err)
	}

	// Build the account response
	account := &commonpb.Account{
		Address:  address,
		Metadata: make(map[string]string),
		Volumes:  volumes,
	}

	// Add metadata if it exists
	if md, exists := metadataMap[address]; exists {
		account.Metadata = md
	}

	return account, nil
}

func (ctrl *DefaultController) GetAllLogs(ctx context.Context, from uint64, to uint64) (store.Cursor[*commonpb.Log], error) {
	return ctrl.store.GetAllLogs(ctx, from, to)
}

func (ctrl *DefaultController) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	ledgerInfo, err := ctrl.store.GetLedgerByName(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", name)
		}
		return nil, err
	}
	return ledgerInfo, nil
}

// Apply applies a list of actions and returns the resulting logs
func (ctrl *DefaultController) Apply(ctx context.Context, actions ...*servicepb.Action) ([]*commonpb.Log, error) {
	if len(actions) == 0 {
		return nil, fmt.Errorf("at least one action is required")
	}

	// forgedAction holds the result of forging a servicepb.Action
	type forgedAction struct {
		raftAction *raftcmdpb.Action // raft action to apply (nil if cached)
		cachedLog  *commonpb.Log     // cached log from idempotency (nil if needs raft apply)
	}

	// Create a single unit of work for all actions
	uow := &unitOfWork{
		KeySetLocker: ctrl.keySetLocker,
		Store:        ctrl.store,
	}
	defer uow.ReleaseLocks()

	// First pass: resolve all actions and check idempotency
	forged := make([]*forgedAction, len(actions))
	var raftActions []*raftcmdpb.Action
	for i, action := range actions {
		input, err := ctrl.resolveActionInput(action)
		if err != nil {
			return nil, fmt.Errorf("resolving action input %d: %w", i, err)
		}

		// Check idempotency
		cachedLog, err := checkIdempotency(ctx, uow, action.IdempotencyKey, input)
		if err != nil {
			return nil, fmt.Errorf("checking idempotency for action %d: %w", i, err)
		}
		if cachedLog != nil {
			forged[i] = &forgedAction{
				cachedLog: cachedLog,
			}
			continue
		}

		builder, err := ctrl.resolveActionBuilder(action)
		if err != nil {
			return nil, fmt.Errorf("resolving action builder %d: %w", i, err)
		}

		// Build the action
		cmd, err := builder(ctx, uow, input)
		if err != nil {
			return nil, fmt.Errorf("building action %d: %w", i, err)
		}
		actionData := &raftcmdpb.ActionData{
			Command: cmd,
		}
		if action.IdempotencyKey != "" {
			actionData.Idempotency = &commonpb.Idempotency{
				Key:  action.IdempotencyKey,
				Hash: commonpb.ComputeIdempotencyHash(input),
			}
		}
		raftAction := raft.NewActionFromData(actionData)
		forged[i] = &forgedAction{
			raftAction: raftAction,
		}
		raftActions = append(raftActions, raftAction)
	}

	// Second pass: batch apply all raft actions
	var raftLogs []*commonpb.Log
	if len(raftActions) > 0 {
		var err error
		raftLogs, err = ctrl.engine.Apply(ctx, raftActions...)
		if err != nil {
			return nil, fmt.Errorf("applying raft actions: %w", err)
		}
	}

	// Third pass: merge results in the correct order
	logs := make([]*commonpb.Log, len(actions))
	raftLogIndex := 0
	for i, fa := range forged {
		if fa.cachedLog != nil {
			logs[i] = fa.cachedLog
		} else {
			logs[i] = raftLogs[raftLogIndex]
			raftLogIndex++
		}
	}
	return logs, nil
}

// resolveActionInput resolves action input data (ledgerID and input proto.Message).
func (ctrl *DefaultController) resolveActionInput(action *servicepb.Action) (proto.Message, error) {
	switch actionType := action.Type.(type) {
	case *servicepb.Action_Apply:
		switch data := actionType.Apply.Data.(type) {
		case *servicepb.LedgerApplyAction_CreateTransaction:
			return data.CreateTransaction, nil

		case *servicepb.LedgerApplyAction_AddMetadata:
			if data.AddMetadata == nil || data.AddMetadata.Target == nil {
				return nil, fmt.Errorf("missing add metadata data or target")
			}
			return data.AddMetadata, nil

		case *servicepb.LedgerApplyAction_RevertTransaction:
			return data.RevertTransaction, nil

		case *servicepb.LedgerApplyAction_DeleteMetadata:
			if data.DeleteMetadata == nil || data.DeleteMetadata.Target == nil {
				return nil, fmt.Errorf("missing delete metadata data or target")
			}
			return data.DeleteMetadata, nil

		default:
			return nil, fmt.Errorf("unsupported action type")
		}

	case *servicepb.Action_CreateLedger:
		return actionType.CreateLedger, nil

	case *servicepb.Action_DeleteLedger:
		return actionType.DeleteLedger, nil

	default:
		return nil, fmt.Errorf("unsupported action type")
	}
}

// resolveActionBuilder resolves the builder function for an action.
func (ctrl *DefaultController) resolveActionBuilder(action *servicepb.Action) (ForgeActionBuilder, error) {
	if action == nil {
		return nil, fmt.Errorf("action is required")
	}

	switch actionType := action.Type.(type) {
	case *servicepb.Action_Apply:
		id, err := ctrl.resolveLedgerID(context.Background(), actionType.Apply.Ledger)
		if err != nil {
			return nil, err
		}

		switch data := actionType.Apply.Data.(type) {
		case *servicepb.LedgerApplyAction_CreateTransaction:
			return func(ctx context.Context, uow *unitOfWork, input proto.Message) (*raftcmdpb.AnyCommand, error) {
				return ctrl.createTransaction(ctx, uow, id, input)
			}, nil

		case *servicepb.LedgerApplyAction_AddMetadata:
			if data.AddMetadata == nil || data.AddMetadata.Target == nil {
				return nil, fmt.Errorf("missing add metadata data or target")
			}
			switch data.AddMetadata.Target.Target.(type) {
			case *commonpb.Target_Account:
				return func(ctx context.Context, uow *unitOfWork, input proto.Message) (*raftcmdpb.AnyCommand, error) {
					return ctrl.saveAccountMetadata(ctx, uow, id, input)
				}, nil
			case *commonpb.Target_Transaction:
				return func(ctx context.Context, uow *unitOfWork, input proto.Message) (*raftcmdpb.AnyCommand, error) {
					return ctrl.saveTransactionMetadata(ctx, uow, id, input)
				}, nil
			default:
				return nil, fmt.Errorf("unsupported target type for add metadata")
			}

		case *servicepb.LedgerApplyAction_RevertTransaction:
			return func(ctx context.Context, uow *unitOfWork, input proto.Message) (*raftcmdpb.AnyCommand, error) {
				return ctrl.revertTransaction(ctx, uow, id, input)
			}, nil

		case *servicepb.LedgerApplyAction_DeleteMetadata:
			if data.DeleteMetadata == nil || data.DeleteMetadata.Target == nil {
				return nil, fmt.Errorf("missing delete metadata data or target")
			}
			switch data.DeleteMetadata.Target.Target.(type) {
			case *commonpb.Target_Account:
				return func(ctx context.Context, uow *unitOfWork, input proto.Message) (*raftcmdpb.AnyCommand, error) {
					return ctrl.deleteAccountMetadata(ctx, uow, id, input)
				}, nil
			case *commonpb.Target_Transaction:
				return func(ctx context.Context, uow *unitOfWork, input proto.Message) (*raftcmdpb.AnyCommand, error) {
					return ctrl.deleteTransactionMetadata(ctx, uow, id, input)
				}, nil
			default:
				return nil, fmt.Errorf("unsupported target type for delete metadata")
			}

		default:
			return nil, fmt.Errorf("unsupported action type")
		}
	case *servicepb.Action_CreateLedger:
		return ctrl.createLedger, nil

	case *servicepb.Action_DeleteLedger:
		return ctrl.deleteLedger, nil

	default:
		return nil, fmt.Errorf("unsupported action type")
	}
}

// resolveLedgerID resolves a LedgerNameOrId to a ledger ID
func (ctrl *DefaultController) resolveLedgerID(ctx context.Context, ledger *servicepb.LedgerNameOrId) (uint32, error) {
	if ledger == nil {
		return 0, fmt.Errorf("ledger identifier is required")
	}

	switch t := ledger.Type.(type) {
	case *servicepb.LedgerNameOrId_Id:
		return t.Id, nil
	case *servicepb.LedgerNameOrId_Name:
		// Check cache first
		if cached, ok := ctrl.ledgerIDCache.Load(t.Name); ok {
			return cached.(uint32), nil
		}

		// Cache miss - fetch from store
		ledgerInfo, err := ctrl.store.GetLedgerByName(ctx, t.Name)
		if err != nil {
			return 0, fmt.Errorf("resolving ledger name %q: %w", t.Name, err)
		}

		// Store in cache
		ctrl.ledgerIDCache.Store(t.Name, ledgerInfo.Id)
		return ledgerInfo.Id, nil
	default:
		return 0, fmt.Errorf("invalid ledger identifier type")
	}
}

func (ctrl *DefaultController) createTransaction(ctx context.Context, unitOfWork *unitOfWork, ledgerID uint32, in proto.Message) (*raftcmdpb.AnyCommand, error) {
	ctrl.logger.Debugf("Creating transaction")

	input := in.(*servicepb.CreateTransactionPayload)

	// Validate that we have either postings or script, but not both
	hasScript := input.Script != nil && input.Script.Plain != ""

	if len(input.Postings) > 0 && hasScript {
		return nil, fmt.Errorf("cannot pass postings and numscript in the same request")
	}

	if len(input.Postings) == 0 && !hasScript {
		return nil, fmt.Errorf("you need to pass either a posting array or a numscript script")
	}

	if input.Reference != "" {
		_, err := unitOfWork.LockKeys(ctx, fmt.Sprintf("%d/tx/references/%s", ledgerID, input.Reference))
		if err != nil {
			return nil, fmt.Errorf("locking reference %s: %w", input.Reference, err)
		}
	}

	var (
		// If script is provided, compile and execute it to generate postings
		scriptMetadata        metadata.Metadata
		scriptAccountMetadata map[string]metadata.Metadata
		postings              []*commonpb.Posting
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
	accountMetadataProto := make(map[string]*commonpb.Metadata)
	for addr, md := range finalAccountMetadata {
		if len(md) > 0 {
			accountMetadataProto[addr] = &commonpb.Metadata{Entries: md}
		}
	}

	return &raftcmdpb.AnyCommand{Command: &raftcmdpb.AnyCommand_CreateLedgerLog{
		CreateLedgerLog: &raftcmdpb.CreateLedgerLogCommand{
			LedgerId: ledgerID,
			Command: &raftcmdpb.CreateLedgerLogCommand_AppendTransaction{
				AppendTransaction: &raftcmdpb.AppendTransactionCommand{
					AccountMetadata: accountMetadataProto,
					Metadata:        finalMetadata,
					Timestamp:       input.Timestamp,
					Reference:       input.Reference,
					Postings:        postings,
				},
			},
		},
	}}, nil
}

// executeNumscript compiles and executes a numscript script to generate postings, metadata, and account metadata
func (ctrl *DefaultController) executeNumscript(ctx context.Context, uow *unitOfWork, ledgerID uint32, script *commonpb.Script) ([]*commonpb.Posting, metadata.Metadata, map[string]metadata.Metadata, error) {
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

	// Convert result postings to []*commonpb.Posting
	postings := make([]*commonpb.Posting, 0, len(result.Postings))
	for _, p := range result.Postings {
		// numscript.Posting.Amount is already a *big.Int
		postings = append(postings, commonpb.NewPosting(p.Source, p.Destination, p.Asset, p.Amount))
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

func (ctrl *DefaultController) revertTransaction(ctx context.Context, unitOfWork *unitOfWork, ledgerID uint32, in proto.Message) (*raftcmdpb.AnyCommand, error) {
	input := in.(*servicepb.RevertTransactionPayload)

	// Validate input
	if input.TransactionId == 0 {
		return nil, fmt.Errorf("transaction id is required")
	}

	// Lock the transaction ID to prevent concurrent revert operations
	lockKey := fmt.Sprintf("%d/tx/revert/%d", ledgerID, input.TransactionId)
	_, err := unitOfWork.LockKeys(ctx, lockKey)
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

	// Get the sequence for the transaction ID
	sequence, err := unitOfWork.GetSequenceForTransactionID(ctx, ledgerID, input.TransactionId)
	if err != nil {
		return nil, fmt.Errorf("getting sequence for transaction %d: %w", input.TransactionId, err)
	}
	if sequence == 0 {
		return nil, fmt.Errorf("transaction %d not found", input.TransactionId)
	}

	// Get the log containing the original transaction
	log, err := unitOfWork.GetLogBySequence(ctx, sequence)
	if err != nil {
		return nil, fmt.Errorf("getting log %d: %w", sequence, err)
	}
	if log == nil {
		return nil, fmt.Errorf("log %d not found", sequence)
	}

	// Extract the ledger log from the log
	applyLog, ok := log.Payload.Type.(*commonpb.LogPayload_Apply)
	if !ok || applyLog.Apply == nil || applyLog.Apply.Log == nil {
		return nil, fmt.Errorf("log %d does not contain an apply log", sequence)
	}
	ledgerLog := applyLog.Apply.Log

	// Extract the original transaction from the ledger log
	var originalTx *commonpb.Transaction
	switch payload := ledgerLog.Data.Payload.(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if payload.CreatedTransaction == nil || payload.CreatedTransaction.Transaction == nil {
			return nil, fmt.Errorf("invalid log payload: missing transaction")
		}
		originalTx = payload.CreatedTransaction.Transaction
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		// Transaction already reverted (double-check)
		return nil, fmt.Errorf("transaction %d is already a revert transaction", input.TransactionId)
	default:
		return nil, fmt.Errorf("ledger log %d does not contain a transaction", ledgerLog.Id)
	}

	// Create reverse transaction with swapped source/destination
	reversedPostings := make([]*commonpb.Posting, len(originalTx.Postings))
	for i, posting := range originalTx.Postings {
		if posting == nil {
			return nil, fmt.Errorf("nil posting at index %d", i)
		}
		reversedPostings[i] = &commonpb.Posting{
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
	var revertTimestamp *commonpb.Timestamp
	if input.AtEffectiveDate && originalTx.Timestamp != nil {
		revertTimestamp = originalTx.Timestamp
	}

	// Create the revert transaction (transaction ID will be assigned by FSM)
	revertTx := &commonpb.Transaction{
		Postings:  reversedPostings,
		Metadata:  revertMetadata,
		Timestamp: revertTimestamp,
		Reference: originalTx.Reference,
	}

	return &raftcmdpb.AnyCommand{Command: &raftcmdpb.AnyCommand_CreateLedgerLog{
		CreateLedgerLog: &raftcmdpb.CreateLedgerLogCommand{
			LedgerId: ledgerID,
			Command: &raftcmdpb.CreateLedgerLogCommand_RevertTransaction{
				RevertTransaction: &raftcmdpb.RevertTransactionCommand{
					TransactionId:     input.TransactionId,
					RevertTransaction: revertTx,
				},
			},
		},
	}}, nil
}

func (ctrl *DefaultController) saveTransactionMetadata(_ context.Context, _ *unitOfWork, ledgerID uint32, in proto.Message) (*raftcmdpb.AnyCommand, error) {
	input := in.(*commonpb.SaveMetadataCommand)

	// Validate input
	target, ok := input.Target.Target.(*commonpb.Target_Transaction)
	if !ok {
		return nil, fmt.Errorf("invalid target type for save transaction metadata")
	}
	if target.Transaction.Id == 0 {
		return nil, fmt.Errorf("transaction id is required")
	}
	if input.Metadata == nil {
		return nil, fmt.Errorf("metadata is required")
	}

	return &raftcmdpb.AnyCommand{Command: &raftcmdpb.AnyCommand_CreateLedgerLog{
		CreateLedgerLog: &raftcmdpb.CreateLedgerLogCommand{
			LedgerId: ledgerID,
			Command: &raftcmdpb.CreateLedgerLogCommand_SaveMetadata{
				SaveMetadata: input,
			},
		},
	}}, nil
}

func (ctrl *DefaultController) saveAccountMetadata(_ context.Context, _ *unitOfWork, ledgerID uint32, in proto.Message) (*raftcmdpb.AnyCommand, error) {
	input := in.(*commonpb.SaveMetadataCommand)

	// Validate input
	target, ok := input.Target.Target.(*commonpb.Target_Account)
	if !ok {
		return nil, fmt.Errorf("invalid target type for save account metadata")
	}
	if target.Account.Addr == "" {
		return nil, fmt.Errorf("account address is required")
	}
	if input.Metadata == nil {
		return nil, fmt.Errorf("metadata is required")
	}

	return &raftcmdpb.AnyCommand{Command: &raftcmdpb.AnyCommand_CreateLedgerLog{
		CreateLedgerLog: &raftcmdpb.CreateLedgerLogCommand{
			LedgerId: ledgerID,
			Command: &raftcmdpb.CreateLedgerLogCommand_SaveMetadata{
				SaveMetadata: input,
			},
		},
	}}, nil
}

func (ctrl *DefaultController) deleteTransactionMetadata(_ context.Context, _ *unitOfWork, ledgerID uint32, in proto.Message) (*raftcmdpb.AnyCommand, error) {
	input := in.(*commonpb.DeleteMetadataCommand)

	target, ok := input.Target.Target.(*commonpb.Target_Transaction)
	if !ok {
		return nil, fmt.Errorf("invalid target type for delete transaction metadata")
	}
	if target.Transaction.Id == 0 {
		return nil, fmt.Errorf("transaction id is required")
	}
	if input.Key == "" {
		return nil, fmt.Errorf("metadata key is required")
	}

	return &raftcmdpb.AnyCommand{Command: &raftcmdpb.AnyCommand_CreateLedgerLog{
		CreateLedgerLog: &raftcmdpb.CreateLedgerLogCommand{
			LedgerId: ledgerID,
			Command: &raftcmdpb.CreateLedgerLogCommand_DeleteMetadata{
				DeleteMetadata: input,
			},
		},
	}}, nil
}

func (ctrl *DefaultController) deleteAccountMetadata(_ context.Context, _ *unitOfWork, ledgerID uint32, in proto.Message) (*raftcmdpb.AnyCommand, error) {
	input := in.(*commonpb.DeleteMetadataCommand)

	target, ok := input.Target.Target.(*commonpb.Target_Account)
	if !ok {
		return nil, fmt.Errorf("invalid target type for delete account metadata")
	}
	if target.Account.Addr == "" {
		return nil, fmt.Errorf("account address is required")
	}
	if input.Key == "" {
		return nil, fmt.Errorf("metadata key is required")
	}

	return &raftcmdpb.AnyCommand{Command: &raftcmdpb.AnyCommand_CreateLedgerLog{
		CreateLedgerLog: &raftcmdpb.CreateLedgerLogCommand{
			LedgerId: ledgerID,
			Command: &raftcmdpb.CreateLedgerLogCommand_DeleteMetadata{
				DeleteMetadata: input,
			},
		},
	}}, nil
}

func (ctrl *DefaultController) createLedger(_ context.Context, _ *unitOfWork, in proto.Message) (*raftcmdpb.AnyCommand, error) {
	input := in.(*servicepb.CreateLedgerRequest)

	if input.Name == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	return &raftcmdpb.AnyCommand{Command: &raftcmdpb.AnyCommand_CreateLedger{
		CreateLedger: &raftcmdpb.CreateLedgerCommand{
			Name:     input.Name,
			Metadata: input.Metadata,
		},
	}}, nil
}

func (ctrl *DefaultController) deleteLedger(ctx context.Context, uow *unitOfWork, in proto.Message) (*raftcmdpb.AnyCommand, error) {
	input := in.(*servicepb.DeleteLedgerRequest)

	if input.Id == 0 {
		return nil, fmt.Errorf("ledger ID is required")
	}

	_, err := uow.LockKeys(ctx, fmt.Sprintf("%d/ledgers", input.Id))
	if err != nil {
		return nil, fmt.Errorf("locking ledger %d: %w", input.Id, err)
	}

	_, err = ctrl.store.GetLedgerByID(ctx, input.Id)
	if err != nil {
		return nil, fmt.Errorf("ledger %d does not exist: %w", input.Id, err)
	}

	return &raftcmdpb.AnyCommand{Command: &raftcmdpb.AnyCommand_DeleteLedger{
		DeleteLedger: &raftcmdpb.DeleteLedgerCommand{
			Id: input.Id,
		},
	}}, nil
}

// Import is not implemented yet
func (ctrl *DefaultController) Import(ctx context.Context, ledgerID uint32, stream chan *commonpb.LedgerLog) error {
	return fmt.Errorf("import is not implemented yet")
}

// Export is not implemented yet
func (ctrl *DefaultController) Export(ctx context.Context, ledgerID uint32, w ExportWriter) error {
	return fmt.Errorf("export is not implemented yet")
}

// checkBalances verifies that all source accounts have sufficient funds for the given postings.
// It locks the relevant balance keys, fetches current balances, and returns ErrInsufficientFunds
// if any source account lacks the required funds.
func (ctrl *DefaultController) checkBalances(ctx context.Context, uow *unitOfWork, ledgerID uint32, postings []*commonpb.Posting) error {
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
	lockKeys := makeBalanceLockKeys(ledgerID, balanceQueryList)
	_, err := uow.LockKeys(ctx, lockKeys...)
	if err != nil {
		return err
	}

	balances, err := uow.GetBalances(ctx, ledgerID, balanceQueryList)
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
