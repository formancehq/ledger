package processing

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/zeebo/blake3"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

//go:generate mockgen -source=processor.go -destination=processor_mock_test.go -package=processing -mock_names=Store=MockStore

// Store is the interface used by RequestProcessor to access data.
// It abstracts the underlying storage mechanism (e.g., Buffered).
type Store interface {
	// Ledger operations
	GetLedger(name string) (*commonpb.LedgerInfo, bool)
	PutLedger(name string, info *commonpb.LedgerInfo)

	// Boundaries operations
	GetBoundaries(ledger string) (*raftcmdpb.LedgerBoundaries, bool)
	PutBoundaries(ledger string, boundaries *raftcmdpb.LedgerBoundaries)

	// Volume operations (Input/Output)
	GetInput(key data.VolumeKey) (*raftcmdpb.VolumeHolder, error)
	PutInput(key data.VolumeKey, value *raftcmdpb.VolumeHolder)
	GetOutput(key data.VolumeKey) (*raftcmdpb.VolumeHolder, error)
	PutOutput(key data.VolumeKey, value *raftcmdpb.VolumeHolder)

	// Account metadata operations
	GetAccountMetadata(key data.MetadataKey) (*commonpb.MetadataValue, error)
	PutAccountMetadata(key data.MetadataKey, value *commonpb.MetadataValue)
	DeleteAccountMetadata(key data.MetadataKey)

	// Ledger metadata operations
	PutLedgerMetadata(key data.LedgerMetadataKey, value *commonpb.MetadataValue)

	// Transaction reversion status operations
	GetReverted(key data.TransactionKey) (bool, error)
	PutReverted(key data.TransactionKey, reverted bool)

	// Idempotency key operations
	GetIdempotencyKey(key data.IdempotencyKey) (*commonpb.IdempotencyKeyValue, error)
	PutIdempotencyKey(key data.IdempotencyKey, value *commonpb.IdempotencyKeyValue)

	// Transaction updates
	AddTransactionUpdate(key data.TransactionKey, update *commonpb.TransactionUpdate)

	// Counters and timestamps
	GetNextLedgerID() uint32
	IncrementNextLedgerID() uint32
	GetNextSequenceID() uint64
	IncrementNextSequenceID() uint64
	GetDate() *commonpb.Timestamp
}

type RequestProcessor struct {
	numscriptCache *NumscriptCache
}

// NewRequestProcessor creates a new RequestProcessor with the given meter.
// If meter is nil, a noop meter is used.
func NewRequestProcessor(m metric.Meter) (*RequestProcessor, error) {
	if m == nil {
		m = noop.Meter{}
	}

	cache := newNumscriptCache()
	if err := cache.initCacheMetrics(m); err != nil {
		return nil, fmt.Errorf("creating numscript cache metrics: %w", err)
	}
	return &RequestProcessor{
		numscriptCache: cache,
	}, nil
}

// ErrIdempotencyKeyConflict is returned when an idempotency key is reused with different content.
var ErrIdempotencyKeyConflict = errors.New("idempotency key conflict: same key used with different request content")

// ProcessProposal processes a proposal (batch of orders) and returns the resulting response.
func (p *RequestProcessor) ProcessProposal(proposal *raftcmdpb.Proposal, s Store) (*raftcmdpb.ProposalResponse, error) {
	logs := make([]*raftcmdpb.CreatedLogOrReference, len(proposal.Orders))

	for i, order := range proposal.Orders {
		// Check idempotency before processing
		if order.Idempotency != nil && order.Idempotency.Key != "" {
			ikKey := data.IdempotencyKey{Key: order.Idempotency.Key}
			storedValue, err := s.GetIdempotencyKey(ikKey)
			if err != nil && !errors.Is(err, data.ErrNotFound) {
				return nil, fmt.Errorf("checking idempotency key: %w", err)
			}

			// Check if idempotency key exists
			if storedValue != nil {
				// Idempotency key exists - compute hash from order and compare
				hash := computeOrderHash(order)
				if !bytes.Equal(hash, storedValue.Hash) {
					return nil, ErrIdempotencyKeyConflict
				}

				// Hash matches - return reference to existing log without processing
				logs[i] = &raftcmdpb.CreatedLogOrReference{
					Type: &raftcmdpb.CreatedLogOrReference_ReferenceSequence{
						ReferenceSequence: storedValue.LogSequence,
					},
				}
				continue
			}
		}

		// No idempotency key or key not found - process normally
		payload, err := p.ProcessOrder(order, s)
		if err != nil {
			return nil, err
		}

		nextSequenceID := s.IncrementNextSequenceID()
		logs[i] = &raftcmdpb.CreatedLogOrReference{
			Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{
				CreatedLog: &commonpb.Log{
					Sequence:    nextSequenceID,
					Payload:     payload,
					Idempotency: order.Idempotency,
				},
			},
		}

		// Store idempotency key if present
		if order.Idempotency != nil && order.Idempotency.Key != "" {
			hash := computeOrderHash(order)
			s.PutIdempotencyKey(
				data.IdempotencyKey{
					Key: order.Idempotency.Key,
				},
				&commonpb.IdempotencyKeyValue{
					LogSequence: nextSequenceID,
					Hash:        hash,
				},
			)
		}
	}

	return &raftcmdpb.ProposalResponse{
		Logs: logs,
	}, nil
}

// computeOrderHash computes a blake3 hash of the order content (excluding idempotency) for idempotency checking.
func computeOrderHash(order *raftcmdpb.Order) []byte {
	// Create a copy without the idempotency field to compute hash
	orderCopy := proto.CloneOf(order)
	orderCopy.Idempotency = nil

	// todo: need to stabilize the format
	data, err := proto.Marshal(orderCopy)
	if err != nil {
		// This should never happen with a valid order
		panic(fmt.Sprintf("failed to marshal order: %v", err))
	}
	hash := blake3.Sum256(data)
	return hash[:]
}

// ProcessOrder processes an Order and returns the resulting LogPayload.
func (p *RequestProcessor) ProcessOrder(order *raftcmdpb.Order, s Store) (*commonpb.LogPayload, error) {
	switch orderType := order.Type.(type) {
	case *raftcmdpb.Order_Apply:
		return p.processApply(orderType.Apply, s)
	case *raftcmdpb.Order_CreateLedger:
		return p.processCreateLedger(orderType.CreateLedger, s)
	case *raftcmdpb.Order_DeleteLedger:
		return p.processDeleteLedger(orderType.DeleteLedger, s)
	default:
		return nil, fmt.Errorf("invalid order type")
	}
}

func (p *RequestProcessor) processCreateLedger(order *raftcmdpb.CreateLedgerOrder, s Store) (*commonpb.LogPayload, error) {
	_, ok := s.GetLedger(order.Name)
	if ok {
		return nil, fmt.Errorf("ledger already exists: %s", order.Name)
	}

	ledgerID := s.IncrementNextLedgerID()
	info := &commonpb.LedgerInfo{
		Name:      order.Name,
		CreatedAt: s.GetDate(),
		Id:        ledgerID,
	}
	s.PutLedger(order.Name, info)
	s.PutBoundaries(order.Name, &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1,
		NextLogId:         1,
	})

	// Store initial metadata using LedgerMetadata attributes
	if order.Metadata != nil {
		for _, m := range order.Metadata.Metadata {
			s.PutLedgerMetadata(data.LedgerMetadataKey{
				LedgerName: order.Name,
				Key:        m.Key,
			}, m.Value)
		}
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreateLedgerLog{
				Info: info,
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteLedger(order *raftcmdpb.DeleteLedgerOrder, s Store) (*commonpb.LogPayload, error) {
	l, ok := s.GetLedger(order.Name)
	if !ok {
		return nil, fmt.Errorf("ledger does not exist: %s", order.Name)
	}
	l.DeletedAt = s.GetDate()

	s.PutLedger(order.Name, l)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeleteLedger{
			DeleteLedger: &commonpb.DeleteLedgerLog{
				Info: l,
			},
		},
	}, nil
}

func (p *RequestProcessor) processApply(apply *raftcmdpb.LedgerApplyOrder, s Store) (*commonpb.LogPayload, error) {
	boundaries, ok := s.GetBoundaries(apply.Ledger)
	if !ok {
		return nil, fmt.Errorf("ledger does not exist: %s", apply.Ledger)
	}

	var (
		logPayload *commonpb.LedgerLogPayload
		err        error
	)
	switch applyData := apply.Data.(type) {
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		logPayload, err = p.processAddMetadata(apply.Ledger, boundaries, applyData.AddMetadata, s)
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		logPayload, err = p.processDeleteMetadata(apply.Ledger, boundaries, applyData.DeleteMetadata, s)
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		logPayload, err = p.processCreateTransaction(apply.Ledger, boundaries, applyData.CreateTransaction, s)
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		logPayload, err = p.processRevertTransaction(apply.Ledger, boundaries, applyData.RevertTransaction, s)
	default:
		return nil, fmt.Errorf("invalid apply type")
	}
	if err != nil {
		return nil, err
	}

	nextLogID := boundaries.NextLogId
	boundaries.NextLogId = nextLogID + 1

	s.PutBoundaries(apply.Ledger, boundaries)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: apply.Ledger,
				Log: &commonpb.LedgerLog{
					Data: logPayload,
					Date: s.GetDate(),
					Id:   nextLogID,
				},
			},
		},
	}, nil
}

func (p *RequestProcessor) processAddMetadata(ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.SaveMetadataOrder, s Store) (*commonpb.LedgerLogPayload, error) {
	if order.Target == nil {
		return nil, errors.New("target is required")
	}

	switch target := order.Target.Target.(type) {
	case *commonpb.Target_Account:
		for _, entry := range order.Metadata.Metadata {
			s.PutAccountMetadata(data.MetadataKey{
				AccountKey: data.AccountKey{
					LedgerName: ledgerName,
					Account:    target.Account.Addr,
				},
				Key: entry.Key,
			}, entry.Value)
		}
	case *commonpb.Target_Transaction:
		if target.Transaction.Id >= boundaries.NextTransactionId {
			return nil, fmt.Errorf("transaction id out of range: %d", target.Transaction.Id)
		}
		// Group all metadata updates into a single TransactionUpdate
		// to avoid key collisions in PebbleDB (all updates in same request share the same ByLog)
		updates := make([]*commonpb.TransactionUpdateType, len(order.Metadata.Metadata))
		for i, metadatum := range order.Metadata.Metadata {
			updates[i] = &commonpb.TransactionUpdateType{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationAddMetadata{
					TransactionModificationAddMetadata: &commonpb.TransactionUpdateAddMetadata{
						Metadata: metadatum,
					},
				},
			}
		}
		s.AddTransactionUpdate(data.TransactionKey{LedgerName: ledgerName, ID: target.Transaction.Id}, &commonpb.TransactionUpdate{
			ByLog:   s.GetNextSequenceID(),
			Updates: updates,
		})
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SavedMetadata{
			SavedMetadata: &commonpb.SavedMetadata{
				Target:   order.Target,
				Metadata: order.Metadata,
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteMetadata(ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.DeleteMetadataOrder, s Store) (*commonpb.LedgerLogPayload, error) {
	if order.Target == nil {
		return nil, errors.New("target is required")
	}
	if order.Key == "" {
		return nil, errors.New("key is required")
	}

	switch target := order.Target.Target.(type) {
	case *commonpb.Target_Account:
		// TODO: is it necessary to check if the metadata was already present?
		s.DeleteAccountMetadata(data.MetadataKey{
			AccountKey: data.AccountKey{
				LedgerName: ledgerName,
				Account:    target.Account.Addr,
			},
			Key: order.Key,
		})
	case *commonpb.Target_Transaction:
		if target.Transaction.Id >= boundaries.NextTransactionId {
			return nil, fmt.Errorf("transaction id out of range: %d", target.Transaction.Id)
		}
		// Use global sequence ID for ByLog (consistent with processCreateTransaction)
		// This ensures each transaction update has a unique key in PebbleDB
		s.AddTransactionUpdate(data.TransactionKey{LedgerName: ledgerName, ID: target.Transaction.Id}, &commonpb.TransactionUpdate{
			ByLog: s.GetNextSequenceID(),
			Updates: []*commonpb.TransactionUpdateType{{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationDeleteMetadata{
					TransactionModificationDeleteMetadata: &commonpb.TransactionUpdateDeleteMetadata{
						Key: order.Key,
					},
				},
			}},
		})
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
			DeletedMetadata: &commonpb.DeletedMetadata{
				Target: order.Target,
				Key:    order.Key,
			},
		},
	}, nil
}

func (p *RequestProcessor) processCreateTransaction(ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.CreateTransactionOrder, s Store) (*commonpb.LedgerLogPayload, error) {
	// Select the appropriate posting producer
	var producer postingProducer
	if order.Script != nil && order.Script.Plain != "" {
		producer = &numscriptPostingProducer{cache: p.numscriptCache, featureFlags: numscriptFeatureFlags}
	} else {
		producer = &stdPostingProducer{}
	}

	// Produce postings (handles balance checks and buffer updates)
	result, err := producer.produce(s, ledgerName, order)
	if err != nil {
		return nil, err
	}

	nextTransactionID := boundaries.NextTransactionId
	boundaries.NextTransactionId = nextTransactionID + 1

	// Store the transaction init update for later indexing
	s.AddTransactionUpdate(data.TransactionKey{LedgerName: ledgerName, ID: nextTransactionID}, &commonpb.TransactionUpdate{
		ByLog: s.GetNextSequenceID(), // Will be set correctly when committing
		Updates: []*commonpb.TransactionUpdateType{{
			TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
				TransactionInit: &commonpb.TransactionInit{},
			},
		}},
	})

	// Merge metadata: order metadata takes precedence over script metadata
	finalMetadata := order.Metadata
	if len(result.TransactionMetadata) > 0 {
		// Convert order metadata to map for merging
		orderMeta := commonpb.MetadataSetToMap(finalMetadata)
		if orderMeta == nil {
			orderMeta = make(map[string]string)
		}
		// Add script metadata (order metadata takes precedence if key exists)
		for key, value := range result.TransactionMetadata {
			if _, exists := orderMeta[key]; !exists {
				orderMeta[key] = value
			}
		}
		// Convert back to MetadataSet
		finalMetadata = commonpb.MetadataSetFromMap(orderMeta)
	}

	// Convert account metadata to protobuf format
	var accountMetadata map[string]*commonpb.MetadataSet
	if len(result.AccountsMetadata) > 0 {
		metaMap := make(map[string]metadata.Metadata, len(result.AccountsMetadata))
		for account, meta := range result.AccountsMetadata {
			metaMap[account] = metadata.Metadata(meta)
		}
		accountMetadata = commonpb.AccountMetadataFromMap(metaMap)
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{
					Postings:   result.Postings,
					Metadata:   finalMetadata,
					Timestamp:  order.Timestamp,
					Reference:  order.Reference,
					Id:         nextTransactionID,
					InsertedAt: s.GetDate(),
					UpdatedAt:  s.GetDate(),
				},
				AccountMetadata: accountMetadata,
			},
		},
	}, nil
}

func (p *RequestProcessor) processRevertTransaction(ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.RevertTransactionOrder, s Store) (*commonpb.LedgerLogPayload, error) {
	txKey := data.TransactionKey{
		LedgerName: ledgerName,
		ID:         order.TransactionId,
	}

	// Check if transaction exists (ID must be less than next transaction ID)
	if order.TransactionId >= boundaries.NextTransactionId {
		return nil, fmt.Errorf("transaction %d does not exist", order.TransactionId)
	}

	// Check if the transaction is already reverted
	reverted, err := s.GetReverted(txKey)
	if err != nil && !errors.Is(err, data.ErrNotFound) {
		return nil, fmt.Errorf("checking reverted status: %w", err)
	}
	if reverted {
		return nil, fmt.Errorf("transaction %d is already reverted", order.TransactionId)
	}

	// Create reversed postings and update volumes
	// For a revert: original destination becomes source, original source becomes destination
	revertPostings := make([]*commonpb.Posting, len(order.OriginalPostings))
	for i, originalPosting := range order.OriginalPostings {
		// Create reversed posting
		revertPostings[i] = &commonpb.Posting{
			Source:      originalPosting.Destination, // Original destination is now source
			Destination: originalPosting.Source,      // Original source is now destination
			Amount:      originalPosting.Amount,
			Asset:       originalPosting.Asset,
		}

		// Apply the reversed posting (skip balance check if force is set)
		if err := applyPosting(s, ledgerName, revertPostings[i], order.Force); err != nil {
			return nil, err
		}
	}

	// Mark the original transaction as reverted
	s.PutReverted(txKey, true)

	// Get new transaction ID for the revert transaction
	revertTxID := boundaries.NextTransactionId
	boundaries.NextTransactionId = revertTxID + 1

	// Add transaction update for the original transaction (mark as reverted)
	s.AddTransactionUpdate(data.TransactionKey{LedgerName: ledgerName, ID: order.TransactionId}, &commonpb.TransactionUpdate{
		ByLog: s.GetNextSequenceID(),
		Updates: []*commonpb.TransactionUpdateType{{
			TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationRevert{
				TransactionModificationRevert: &commonpb.TransactionUpdateRevert{
					ByTransaction: revertTxID,
				},
			},
		}},
	})

	// Add transaction init for the revert transaction
	s.AddTransactionUpdate(data.TransactionKey{LedgerName: ledgerName, ID: revertTxID}, &commonpb.TransactionUpdate{
		ByLog: s.GetNextSequenceID(),
		Updates: []*commonpb.TransactionUpdateType{{
			TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
				TransactionInit: &commonpb.TransactionInit{},
			},
		}},
	})

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: order.TransactionId,
				RevertTransaction: &commonpb.Transaction{
					Postings:   revertPostings,
					Metadata:   order.Metadata,
					Timestamp:  s.GetDate(),
					Id:         revertTxID,
					InsertedAt: s.GetDate(),
					UpdatedAt:  s.GetDate(),
				},
			},
		},
	}, nil
}

// applyPosting applies a single posting by updating volumes.
// It checks the source balance (unless skipBalanceCheck is true or source is "world"),
// increases Output for source and Input for destination.
func applyPosting(s Store, ledgerName string, posting *commonpb.Posting, skipBalanceCheck bool) error {
	sourceKey := data.VolumeKey{
		AccountKey: data.AccountKey{
			LedgerName: ledgerName,
			Account:    posting.Source,
		},
		Asset: posting.Asset,
	}

	// Get current volumes for source
	sourceInput, err := s.GetInput(sourceKey)
	if err != nil && !errors.Is(err, data.ErrNotFound) {
		return err
	}
	sourceOutput, err := s.GetOutput(sourceKey)
	if err != nil && !errors.Is(err, data.ErrNotFound) {
		return err
	}

	// Balance check (skip for "world" account and when skipBalanceCheck is true)
	if !skipBalanceCheck && posting.Source != "world" {
		if sourceInput == nil || sourceInput.Known == nil {
			return fmt.Errorf("balance not found for %s", posting.Source)
		}
		if sourceOutput == nil {
			sourceOutput = &raftcmdpb.VolumeHolder{}
		}
		var outputValue *big.Int
		if sourceOutput.Known != nil {
			outputValue = sourceOutput.Known.Value()
		} else {
			outputValue = big.NewInt(0)
		}
		balance := new(big.Int).Sub(sourceInput.Known.Value(), outputValue)
		if balance.Cmp(posting.Amount.Value()) < 0 {
			return fmt.Errorf("insufficient funds: %s", posting.Source)
		}
	}

	// Increase Output for source (money going out)
	if sourceOutput == nil {
		sourceOutput = &raftcmdpb.VolumeHolder{}
	}
	// If we know the absolute value, update Known (buffer.Merge will use SetBase).
	// If we don't know the absolute value, update DiffSinceBaseIndex (buffer.Merge will use AddDiff).
	if sourceOutput.Known != nil {
		sourceOutput.Known = commonpb.NewBigInt(
			new(big.Int).Add(sourceOutput.Known.Value(), posting.Amount.Value()),
		)
	} else {
		if sourceOutput.DiffSinceBaseIndex == nil {
			sourceOutput.DiffSinceBaseIndex = posting.Amount
		} else {
			sourceOutput.DiffSinceBaseIndex = commonpb.NewBigInt(
				new(big.Int).Add(sourceOutput.DiffSinceBaseIndex.Value(), posting.Amount.Value()),
			)
		}
	}
	s.PutOutput(sourceKey, sourceOutput)

	// Destination receives credit - increase Input
	destKey := data.VolumeKey{
		AccountKey: data.AccountKey{
			LedgerName: ledgerName,
			Account:    posting.Destination,
		},
		Asset: posting.Asset,
	}

	destInput, err := s.GetInput(destKey)
	if err != nil && !errors.Is(err, data.ErrNotFound) {
		return err
	}
	if destInput == nil {
		destInput = &raftcmdpb.VolumeHolder{}
	}
	// If we know the absolute value, update Known (buffer.Merge will use SetBase).
	// If we don't know the absolute value, update DiffSinceBaseIndex (buffer.Merge will use AddDiff).
	if destInput.Known != nil {
		destInput.Known = commonpb.NewBigInt(
			new(big.Int).Add(destInput.Known.Value(), posting.Amount.Value()),
		)
	} else {
		if destInput.DiffSinceBaseIndex == nil {
			destInput.DiffSinceBaseIndex = posting.Amount
		} else {
			destInput.DiffSinceBaseIndex = commonpb.NewBigInt(
				new(big.Int).Add(destInput.DiffSinceBaseIndex.Value(), posting.Amount.Value()),
			)
		}
	}
	s.PutInput(destKey, destInput)

	return nil
}

// produceResult holds the result of producing postings from an order.
// It includes the postings and any metadata set by the script.
type produceResult struct {
	Postings            []*commonpb.Posting
	TransactionMetadata map[string]string            // Metadata from set_tx_meta in Numscript
	AccountsMetadata    map[string]map[string]string // Metadata from set_account_meta in Numscript
}

type postingProducer interface {
	produce(s Store, ledgerName string, order *raftcmdpb.CreateTransactionOrder) (*produceResult, error)
}

type stdPostingProducer struct{}

func (p *stdPostingProducer) produce(s Store, ledgerName string, order *raftcmdpb.CreateTransactionOrder) (*produceResult, error) {
	for _, posting := range order.Postings {
		// Skip balance check when Force is true
		if err := applyPosting(s, ledgerName, posting, order.Force); err != nil {
			return nil, err
		}
	}

	return &produceResult{
		Postings:            order.Postings,
		TransactionMetadata: nil, // No script metadata for standard postings
	}, nil
}
