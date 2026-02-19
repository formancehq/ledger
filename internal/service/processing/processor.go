package processing

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/holiman/uint256"
	"github.com/zeebo/blake3"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

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

	// Volume operations (merged Input+Output)
	GetVolume(key data.VolumeKey) (*raftcmdpb.VolumePair, error)
	PutVolume(key data.VolumeKey, value *raftcmdpb.VolumePair)

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

	// Transaction reference operations
	GetTransactionReference(key data.TransactionReferenceKey) (*commonpb.TransactionReferenceValue, error)
	PutTransactionReference(key data.TransactionReferenceKey, value *commonpb.TransactionReferenceValue)

	// Transaction updates
	AddTransactionUpdate(key data.TransactionKey, update *commonpb.TransactionUpdate)

	// Signing key operations
	AddSigningKey(keyID string, publicKey []byte)
	RemoveSigningKey(keyID string)
	SetRequireSignatures(require bool)

	// Maintenance mode operations
	SetMaintenanceMode(enabled bool)

	// Events sink operations
	GetSinkConfig(name string) (*commonpb.SinkConfig, error)
	AddSinkConfig(config *commonpb.SinkConfig)
	RemoveSinkConfig(name string)

	// Log hash chaining
	GetLastLogHash() []byte
	SetLastLogHash(hash []byte)

	// Counters and timestamps
	GetNextLedgerID() uint32
	IncrementNextLedgerID() uint32
	GetNextSequenceID() uint64
	IncrementNextSequenceID() uint64
	GetDate() *commonpb.Timestamp

	// Period operations
	GetCurrentOpenPeriod() (*commonpb.Period, bool)
	GetClosingPeriod() (*commonpb.Period, bool)
	SetCurrentOpenPeriod(period *commonpb.Period)
	SetClosingPeriod(period *commonpb.Period)
	ClearClosingPeriod()
	GetNextPeriodID() uint64
	IncrementNextPeriodID() uint64

	// Archive period operations
	GetPeriodByID(periodID uint64) (*commonpb.Period, bool)
	UpdatePeriod(period *commonpb.Period)
	SetPurgeRange(periodID, startSequence, closeSequence uint64)
	SetPendingArchive(periodID, startSequence, closeSequence uint64)
}

type RequestProcessor struct {
	numscriptCache   *NumscriptCache
	logHasher        *blake3.Hasher
	orderHashBuf     []byte // reusable buffer for order hash marshaling
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
		logHasher:      blake3.New(),
		orderHashBuf:   make([]byte, 0, 512),
	}, nil
}


// ProcessProposal processes a proposal (batch of orders) and returns the resulting response.
func (p *RequestProcessor) ProcessProposal(proposal *raftcmdpb.Proposal, s Store) (*raftcmdpb.ProposalResponse, error) {
	logs := make([]*raftcmdpb.CreatedLogOrReference, len(proposal.Orders))

	for i, order := range proposal.Orders {
		// Compute idempotency hash once if needed (reused for check + store)
		hasIdempotency := order.Idempotency != nil && order.Idempotency.Key != ""
		var orderHash []byte
		if hasIdempotency {
			orderHash = p.computeOrderHash(order)

			ikKey := data.IdempotencyKey{Key: order.Idempotency.Key}
			storedValue, err := s.GetIdempotencyKey(ikKey)
			if err != nil && !errors.Is(err, data.ErrNotFound) {
				return nil, fmt.Errorf("checking idempotency key: %w", err)
			}

			// Check if idempotency key exists
			if storedValue != nil {
				if !bytes.Equal(orderHash, storedValue.Hash) {
					return nil, &ErrIdempotencyKeyConflict{Key: order.Idempotency.Key}
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
		log := &commonpb.Log{
			Sequence:    nextSequenceID,
			Payload:     payload,
			Idempotency: order.Idempotency,
			Signature:   order.Signature,
		}
		log.Hash = ComputeLogHash(p.logHasher, s.GetLastLogHash(), log)
		s.SetLastLogHash(log.Hash)

		logs[i] = &raftcmdpb.CreatedLogOrReference{
			Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{
				CreatedLog: log,
			},
		}

		// Store idempotency key if present (reuse orderHash computed above)
		if hasIdempotency {
			s.PutIdempotencyKey(
				data.IdempotencyKey{
					Key: order.Idempotency.Key,
				},
				&commonpb.IdempotencyKeyValue{
					LogSequence: nextSequenceID,
					Hash:        orderHash,
				},
			)
		}
	}

	return &raftcmdpb.ProposalResponse{
		Logs: logs,
	}, nil
}

// computeOrderHash computes a blake3 hash of the order content (excluding
// idempotency) for idempotency checking. It reuses p.orderHashBuf across calls
// to avoid allocations. Safe because ProcessProposal is single-threaded.
func (p *RequestProcessor) computeOrderHash(order *raftcmdpb.Order) []byte {
	// Temporarily nil the idempotency field to exclude it from the hash,
	// then restore it. This avoids a full CloneVT of the order.
	saved := order.Idempotency
	order.Idempotency = nil

	size := order.SizeVT()
	if cap(p.orderHashBuf) < size {
		p.orderHashBuf = make([]byte, size)
	} else {
		p.orderHashBuf = p.orderHashBuf[:size]
	}
	n, err := order.MarshalToVT(p.orderHashBuf)
	order.Idempotency = saved
	if err != nil {
		panic(fmt.Sprintf("failed to marshal order: %v", err))
	}

	hash := blake3.Sum256(p.orderHashBuf[:n])
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
	case *raftcmdpb.Order_RegisterSigningKey:
		return p.processRegisterSigningKey(orderType.RegisterSigningKey, s)
	case *raftcmdpb.Order_RevokeSigningKey:
		return p.processRevokeSigningKey(orderType.RevokeSigningKey, s)
	case *raftcmdpb.Order_SetSigningConfig:
		return p.processSetSigningConfig(orderType.SetSigningConfig, s)
	case *raftcmdpb.Order_SetMaintenanceMode:
		return p.processSetMaintenanceMode(orderType.SetMaintenanceMode, s)
	case *raftcmdpb.Order_AddEventsSink:
		return p.processAddEventsSink(orderType.AddEventsSink, s)
	case *raftcmdpb.Order_RemoveEventsSink:
		return p.processRemoveEventsSink(orderType.RemoveEventsSink, s)
	case *raftcmdpb.Order_ClosePeriod:
		return p.processClosePeriod(orderType.ClosePeriod, s)
	case *raftcmdpb.Order_SealPeriod:
		return p.processSealPeriod(orderType.SealPeriod, s)
	case *raftcmdpb.Order_ArchivePeriod:
		return p.processArchivePeriod(orderType.ArchivePeriod, s)
	case *raftcmdpb.Order_ConfirmArchivePeriod:
		return p.processConfirmArchivePeriod(orderType.ConfirmArchivePeriod, s)
	default:
		return nil, fmt.Errorf("invalid order type")
	}
}

func (p *RequestProcessor) processRegisterSigningKey(order *raftcmdpb.RegisterSigningKeyOrder, s Store) (*commonpb.LogPayload, error) {
	s.AddSigningKey(order.KeyId, order.PublicKey)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_RegisterSigningKey{
			RegisterSigningKey: &commonpb.RegisterSigningKeyLog{
				KeyId:     order.KeyId,
				PublicKey: order.PublicKey,
			},
		},
	}, nil
}

func (p *RequestProcessor) processRevokeSigningKey(order *raftcmdpb.RevokeSigningKeyOrder, s Store) (*commonpb.LogPayload, error) {
	s.RemoveSigningKey(order.KeyId)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_RevokeSigningKey{
			RevokeSigningKey: &commonpb.RevokeSigningKeyLog{
				KeyId: order.KeyId,
			},
		},
	}, nil
}

func (p *RequestProcessor) processSetSigningConfig(order *raftcmdpb.SetSigningConfigOrder, s Store) (*commonpb.LogPayload, error) {
	s.SetRequireSignatures(order.RequireSignatures)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetSigningConfig{
			SetSigningConfig: &commonpb.SetSigningConfigLog{
				RequireSignatures: order.RequireSignatures,
			},
		},
	}, nil
}

func (p *RequestProcessor) processSetMaintenanceMode(order *raftcmdpb.SetMaintenanceModeOrder, s Store) (*commonpb.LogPayload, error) {
	s.SetMaintenanceMode(order.Enabled)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetMaintenanceMode{
			SetMaintenanceMode: &commonpb.SetMaintenanceModeLog{
				Enabled: order.Enabled,
			},
		},
	}, nil
}

func (p *RequestProcessor) processAddEventsSink(order *raftcmdpb.AddEventsSinkOrder, s Store) (*commonpb.LogPayload, error) {
	existing, err := s.GetSinkConfig(order.Config.Name)
	if err != nil {
		return nil, fmt.Errorf("checking existing sink %q: %w", order.Config.Name, err)
	}
	if existing != nil {
		return nil, &ErrSinkAlreadyExists{Name: order.Config.Name}
	}

	s.AddSinkConfig(order.Config)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_AddedEventsSink{
			AddedEventsSink: &commonpb.AddedEventsSinkLog{
				Config: order.Config,
			},
		},
	}, nil
}

func (p *RequestProcessor) processRemoveEventsSink(order *raftcmdpb.RemoveEventsSinkOrder, s Store) (*commonpb.LogPayload, error) {
	existing, err := s.GetSinkConfig(order.Name)
	if err != nil {
		return nil, fmt.Errorf("checking existing sink %q: %w", order.Name, err)
	}
	if existing == nil {
		return nil, &ErrSinkNotFound{Name: order.Name}
	}

	s.RemoveSinkConfig(order.Name)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_RemovedEventsSink{
			RemovedEventsSink: &commonpb.RemovedEventsSinkLog{
				Name: order.Name,
			},
		},
	}, nil
}

func (p *RequestProcessor) processCreateLedger(order *raftcmdpb.CreateLedgerOrder, s Store) (*commonpb.LogPayload, error) {
	_, ok := s.GetLedger(order.Name)
	if ok {
		return nil, &ErrLedgerAlreadyExists{Name: order.Name}
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
		LedgerId:          ledgerID,
	})

	// Store initial metadata using LedgerMetadata attributes
	if order.Metadata != nil {
		for _, m := range order.Metadata.Metadata {
			s.PutLedgerMetadata(data.LedgerMetadataKey{
				LedgerID: ledgerID,
				Key:      m.Key,
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
		return nil, &ErrLedgerNotFound{Name: order.Name}
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
		return nil, &ErrLedgerNotFound{Name: apply.Ledger}
	}

	ledgerID := boundaries.LedgerId

	var (
		logPayload *commonpb.LedgerLogPayload
		err        error
	)
	switch applyData := apply.Data.(type) {
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		logPayload, err = p.processAddMetadata(ledgerID, boundaries, applyData.AddMetadata, s)
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		logPayload, err = p.processDeleteMetadata(ledgerID, boundaries, applyData.DeleteMetadata, s)
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		logPayload, err = p.processCreateTransaction(ledgerID, boundaries, applyData.CreateTransaction, s)
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		logPayload, err = p.processRevertTransaction(ledgerID, boundaries, applyData.RevertTransaction, s)
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

func (p *RequestProcessor) processAddMetadata(ledgerID uint32, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.SaveMetadataOrder, s Store) (*commonpb.LedgerLogPayload, error) {
	if order.Target == nil {
		return nil, ErrTargetRequired
	}

	switch target := order.Target.Target.(type) {
	case *commonpb.Target_Account:
		for _, entry := range order.Metadata.Metadata {
			s.PutAccountMetadata(data.MetadataKey{
				AccountKey: data.AccountKey{
					LedgerID: ledgerID,
					Account:  target.Account.Addr,
				},
				Key: entry.Key,
			}, entry.Value)
		}
	case *commonpb.Target_Transaction:
		if target.Transaction.Id >= boundaries.NextTransactionId {
			return nil, &ErrTransactionNotFound{TransactionID: target.Transaction.Id}
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
		s.AddTransactionUpdate(data.TransactionKey{LedgerID: ledgerID, ID: target.Transaction.Id}, &commonpb.TransactionUpdate{
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

func (p *RequestProcessor) processDeleteMetadata(ledgerID uint32, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.DeleteMetadataOrder, s Store) (*commonpb.LedgerLogPayload, error) {
	if order.Target == nil {
		return nil, ErrTargetRequired
	}
	if order.Key == "" {
		return nil, ErrMetadataKeyRequired
	}

	switch target := order.Target.Target.(type) {
	case *commonpb.Target_Account:
		// TODO: is it necessary to check if the metadata was already present?
		s.DeleteAccountMetadata(data.MetadataKey{
			AccountKey: data.AccountKey{
				LedgerID: ledgerID,
				Account:  target.Account.Addr,
			},
			Key: order.Key,
		})
	case *commonpb.Target_Transaction:
		if target.Transaction.Id >= boundaries.NextTransactionId {
			return nil, &ErrTransactionNotFound{TransactionID: target.Transaction.Id}
		}
		// Use global sequence ID for ByLog (consistent with processCreateTransaction)
		// This ensures each transaction update has a unique key in PebbleDB
		s.AddTransactionUpdate(data.TransactionKey{LedgerID: ledgerID, ID: target.Transaction.Id}, &commonpb.TransactionUpdate{
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

func (p *RequestProcessor) processCreateTransaction(ledgerID uint32, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.CreateTransactionOrder, s Store) (*commonpb.LedgerLogPayload, error) {
	// Check transaction reference uniqueness if reference is provided
	if order.Reference != "" {
		refKey := data.TransactionReferenceKey{LedgerID: ledgerID, Reference: order.Reference}
		existingRef, err := s.GetTransactionReference(refKey)
		if err != nil && !errors.Is(err, data.ErrNotFound) {
			return nil, fmt.Errorf("checking transaction reference: %w", err)
		}
		if existingRef != nil {
			return nil, &ErrTransactionReferenceConflict{
				LedgerID:  ledgerID,
				Reference: order.Reference,
			}
		}
	}

	// Select the appropriate posting producer
	var producer postingProducer
	if order.Script != nil && order.Script.Plain != "" {
		producer = &numscriptPostingProducer{cache: p.numscriptCache, featureFlags: numscriptFeatureFlags}
	} else {
		producer = &stdPostingProducer{}
	}

	// Produce postings (handles balance checks and buffer updates)
	result, err := producer.produce(s, ledgerID, order)
	if err != nil {
		return nil, err
	}

	nextTransactionID := boundaries.NextTransactionId
	boundaries.NextTransactionId = nextTransactionID + 1

	// Store the transaction init update for later indexing
	s.AddTransactionUpdate(data.TransactionKey{LedgerID: ledgerID, ID: nextTransactionID}, &commonpb.TransactionUpdate{
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

	// Store transaction reference if provided
	if order.Reference != "" {
		s.PutTransactionReference(
			data.TransactionReferenceKey{LedgerID: ledgerID, Reference: order.Reference},
			&commonpb.TransactionReferenceValue{TransactionId: nextTransactionID},
		)
	}

	// Use the user-provided timestamp, or fall back to the command date
	timestamp := order.Timestamp
	if timestamp == nil {
		timestamp = s.GetDate()
	}

	// Get the current open period ID for the receipt
	var periodID uint64
	if p, ok := s.GetCurrentOpenPeriod(); ok {
		periodID = p.Id
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{
					Postings:   result.Postings,
					Metadata:   finalMetadata,
					Timestamp:  timestamp,
					Reference:  order.Reference,
					Id:         nextTransactionID,
					InsertedAt: s.GetDate(),
					UpdatedAt:  s.GetDate(),
				},
				AccountMetadata: accountMetadata,
				PeriodId:        periodID,
			},
		},
	}, nil
}

func (p *RequestProcessor) processRevertTransaction(ledgerID uint32, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.RevertTransactionOrder, s Store) (*commonpb.LedgerLogPayload, error) {
	txKey := data.TransactionKey{
		LedgerID: ledgerID,
		ID:       order.TransactionId,
	}

	// Check if transaction exists (ID must be less than next transaction ID)
	if order.TransactionId >= boundaries.NextTransactionId {
		return nil, &ErrTransactionNotFound{TransactionID: order.TransactionId}
	}

	// Check if the transaction is already reverted
	reverted, err := s.GetReverted(txKey)
	if err != nil && !errors.Is(err, data.ErrNotFound) {
		return nil, fmt.Errorf("checking reverted status: %w", err)
	}
	if reverted {
		return nil, &ErrTransactionAlreadyReverted{TransactionID: order.TransactionId}
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
		if err := applyPosting(s, ledgerID, revertPostings[i], order.Force); err != nil {
			return nil, err
		}
	}

	// Mark the original transaction as reverted
	s.PutReverted(txKey, true)

	// Get new transaction ID for the revert transaction
	revertTxID := boundaries.NextTransactionId
	boundaries.NextTransactionId = revertTxID + 1

	// Add transaction update for the original transaction (mark as reverted)
	s.AddTransactionUpdate(data.TransactionKey{LedgerID: ledgerID, ID: order.TransactionId}, &commonpb.TransactionUpdate{
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
	s.AddTransactionUpdate(data.TransactionKey{LedgerID: ledgerID, ID: revertTxID}, &commonpb.TransactionUpdate{
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
func applyPosting(s Store, ledgerID uint32, posting *commonpb.Posting, skipBalanceCheck bool) error {
	sourceKey := data.VolumeKey{
		AccountKey: data.AccountKey{
			LedgerID: ledgerID,
			Account:  posting.Source,
		},
		Asset: posting.Asset,
	}

	// Decode posting amount into stack variable to avoid heap allocation
	var amount uint256.Int
	posting.Amount.IntoUint256(&amount)

	// Get current volume pair for source
	sourceVol, err := s.GetVolume(sourceKey)
	if err != nil && !errors.Is(err, data.ErrNotFound) {
		return err
	}
	if sourceVol == nil {
		sourceVol = &raftcmdpb.VolumePair{}
	}

	// Balance check (skip for "world" account and when skipBalanceCheck is true)
	if !skipBalanceCheck && posting.Source != "world" {
		if sourceVol.InputKnown == nil {
			return &ErrBalanceNotFound{Account: posting.Source, Asset: posting.Asset}
		}
		// Overflow-safe balance check: input >= output + amount
		var inputValue, outputValue, outputPlusAmount uint256.Int
		sourceVol.InputKnown.IntoUint256(&inputValue)
		if sourceVol.OutputKnown != nil {
			sourceVol.OutputKnown.IntoUint256(&outputValue)
		}
		sum, overflow := outputPlusAmount.AddOverflow(&outputValue, &amount)
		if overflow || inputValue.Lt(sum) {
			// Only compute signed balance for the error message
			balanceBig := new(big.Int).Sub(inputValue.ToBig(), outputValue.ToBig())
			return &ErrInsufficientFunds{
				Account: posting.Source,
				Asset:   posting.Asset,
				Amount:  amount.Dec(),
				Balance: balanceBig.String(),
			}
		}
	}

	// scratch is reused across both addToVolumeSide calls
	var scratch uint256.Int

	// Increase Output for source (money going out)
	addToVolumeSide(&sourceVol.OutputKnown, &sourceVol.OutputDiff, &amount, posting.Amount, &scratch)
	s.PutVolume(sourceKey, sourceVol)

	// Destination receives credit - increase Input
	destKey := data.VolumeKey{
		AccountKey: data.AccountKey{
			LedgerID: ledgerID,
			Account:  posting.Destination,
		},
		Asset: posting.Asset,
	}

	destVol, err := s.GetVolume(destKey)
	if err != nil && !errors.Is(err, data.ErrNotFound) {
		return err
	}
	if destVol == nil {
		destVol = &raftcmdpb.VolumePair{}
	}
	addToVolumeSide(&destVol.InputKnown, &destVol.InputDiff, &amount, posting.Amount, &scratch)
	s.PutVolume(destKey, destVol)

	return nil
}

// addToVolumeSide adds amount to one side (input or output) of a VolumePair.
// If Known is set, it updates Known (SetBase path). Otherwise, it updates Diff (AddDiff path).
// rawAmount is the original proto Uint256 used when Diff is nil to avoid re-encoding.
// scratch is a caller-provided uint256.Int to avoid heap allocation.
func addToVolumeSide(known **commonpb.Uint256, diff **commonpb.Uint256, amount *uint256.Int, rawAmount *commonpb.Uint256, scratch *uint256.Int) {
	if *known != nil {
		// Safe to mutate in-place: *known is always a cloned cache value, never shared.
		(*known).IntoUint256(scratch)
		scratch.Add(scratch, amount)
		(*known).SetFromUint256(scratch)
	} else {
		if *diff == nil {
			*diff = rawAmount
		} else {
			// Must create new *Uint256: *diff may point to a shared rawAmount (posting.Amount).
			(*diff).IntoUint256(scratch)
			scratch.Add(scratch, amount)
			*diff = commonpb.NewUint256(scratch)
		}
	}
}

// produceResult holds the result of producing postings from an order.
// It includes the postings and any metadata set by the script.
type produceResult struct {
	Postings            []*commonpb.Posting
	TransactionMetadata map[string]string            // Metadata from set_tx_meta in Numscript
	AccountsMetadata    map[string]map[string]string // Metadata from set_account_meta in Numscript
}

type postingProducer interface {
	produce(s Store, ledgerID uint32, order *raftcmdpb.CreateTransactionOrder) (*produceResult, error)
}

type stdPostingProducer struct{}

func (p *stdPostingProducer) produce(s Store, ledgerID uint32, order *raftcmdpb.CreateTransactionOrder) (*produceResult, error) {
	for _, posting := range order.Postings {
		// Skip balance check when Force is true
		if err := applyPosting(s, ledgerID, posting, order.Force); err != nil {
			return nil, err
		}
	}

	return &produceResult{
		Postings:            order.Postings,
		TransactionMetadata: nil, // No script metadata for standard postings
	}, nil
}
