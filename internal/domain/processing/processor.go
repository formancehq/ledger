package processing

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/zeebo/blake3"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing/numscript"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

type RequestProcessor struct {
	numscriptCache     *numscript.NumscriptCache
	hashBuf            []byte // reusable buffer for idempotency hash serialization
	compiledTypesCache map[string][]accounttype.CompiledType
	assetCache         map[string]cachedAssetPrecision // per-batch cache for ParseAssetPrecision
}

// NewRequestProcessor creates a new RequestProcessor with the given meter.
// If meter is nil, a noop meter is used. numscriptCacheSize controls the
// maximum number of parsed scripts kept in the LRU cache (0 = default 1024).
func NewRequestProcessor(m metric.Meter, numscriptCacheSize int) (*RequestProcessor, error) {
	if m == nil {
		m = noop.Meter{}
	}

	cache := numscript.NewNumscriptCache(numscriptCacheSize)

	err := cache.InitCacheMetrics(m)
	if err != nil {
		return nil, fmt.Errorf("creating numscript cache metrics: %w", err)
	}

	return &RequestProcessor{
		numscriptCache:     cache,
		hashBuf:            make([]byte, 0, 1024),
		compiledTypesCache: make(map[string][]accounttype.CompiledType),
		assetCache:         make(map[string]cachedAssetPrecision),
	}, nil
}

// getCompiledTypes returns compiled account types for the given ledger,
// using a per-batch cache to avoid redundant ParsePattern calls across orders.
func (p *RequestProcessor) getCompiledTypes(ledger string, info *commonpb.LedgerInfo) []accounttype.CompiledType {
	if info == nil || len(info.GetAccountTypes()) == 0 {
		return nil
	}

	if cached, ok := p.compiledTypesCache[ledger]; ok {
		return cached
	}

	compiled := accounttype.CompileTypes(info.GetAccountTypes())
	p.compiledTypesCache[ledger] = compiled

	return compiled
}

// invalidateCompiledTypes removes the cached compiled types for a ledger,
// forcing recompilation on the next access.
func (p *RequestProcessor) invalidateCompiledTypes(ledger string) {
	delete(p.compiledTypesCache, ledger)
}

// ProcessOrders processes a list of orders and returns the resulting logs.
func (p *RequestProcessor) ProcessOrders(orders []*raftcmdpb.Order, s InMemoryStore) ([]*raftcmdpb.CreatedLogOrReference, error) {
	clear(p.compiledTypesCache)
	clear(p.assetCache)

	logs := make([]*raftcmdpb.CreatedLogOrReference, len(orders))

	for i, order := range orders {
		// Compute idempotency hash once if needed (reused for check + store).
		hasIdempotency := order.GetIdempotency() != nil && order.GetIdempotency().GetKey() != ""

		var orderHash []byte
		if hasIdempotency {
			orderHash = p.computeOrderHash(order)

			ikKey := domain.IdempotencyKey{Key: order.GetIdempotency().GetKey()}

			storedValue, err := s.GetIdempotencyKey(ikKey)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return nil, fmt.Errorf("checking idempotency key: %w", err)
			}

			// Check if idempotency key exists
			if storedValue != nil {
				if !bytes.Equal(orderHash, storedValue.GetHash()) {
					return nil, &domain.ErrIdempotencyKeyConflict{Key: order.GetIdempotency().GetKey()}
				}

				// Hash matches - return reference to existing log without processing
				logs[i] = &raftcmdpb.CreatedLogOrReference{
					Type: &raftcmdpb.CreatedLogOrReference_ReferenceSequence{
						ReferenceSequence: storedValue.GetLogSequence(),
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
			Idempotency: order.GetIdempotency().CloneVT(),
			Signature:   order.GetSignature().CloneVT(),
		}

		logs[i] = &raftcmdpb.CreatedLogOrReference{
			Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{
				CreatedLog: log,
			},
		}

		// Store idempotency key (hash was computed before ProcessOrder)
		if hasIdempotency {
			s.PutIdempotencyKey(
				domain.IdempotencyKey{
					Key: order.GetIdempotency().GetKey(),
				},
				&commonpb.IdempotencyKeyValue{
					LogSequence: nextSequenceID,
					Hash:        orderHash,
				},
			)
		}
	}

	return logs, nil
}

// computeOrderHash computes a blake3 hash of the order content (excluding
// idempotency) for idempotency checking. It reuses p.hashBuf across calls
// to avoid allocations. Safe because ProcessOrders is single-threaded.
func (p *RequestProcessor) computeOrderHash(order *raftcmdpb.Order) []byte {
	// Temporarily nil the idempotency field to exclude it from the hash,
	// then restore it. This avoids a full CloneVT of the order.
	saved := order.GetIdempotency()
	order.Idempotency = nil

	p.hashBuf = order.MarshalDeterministicVT(p.hashBuf[:0])

	order.Idempotency = saved

	hash := blake3.Sum256(p.hashBuf)

	return hash[:]
}

// ProcessOrder processes an Order and returns the resulting LogPayload.
func (p *RequestProcessor) ProcessOrder(order *raftcmdpb.Order, s InMemoryStore) (*commonpb.LogPayload, error) {
	switch orderType := order.GetType().(type) {
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
	case *raftcmdpb.Order_SetPeriodSchedule:
		return p.processSetPeriodSchedule(orderType.SetPeriodSchedule, s)
	case *raftcmdpb.Order_DeletePeriodSchedule:
		return p.processDeletePeriodSchedule(s)
	case *raftcmdpb.Order_MirrorIngest:
		return p.processMirrorIngest(orderType.MirrorIngest, s)
	case *raftcmdpb.Order_PromoteLedger:
		return p.processPromoteLedger(orderType.PromoteLedger, s)
	case *raftcmdpb.Order_CreatePreparedQuery:
		return p.processCreatePreparedQuery(orderType.CreatePreparedQuery, s)
	case *raftcmdpb.Order_UpdatePreparedQuery:
		return p.processUpdatePreparedQuery(orderType.UpdatePreparedQuery, s)
	case *raftcmdpb.Order_DeletePreparedQuery:
		return p.processDeletePreparedQuery(orderType.DeletePreparedQuery, s)
	case *raftcmdpb.Order_SaveNumscript:
		return p.processSaveNumscript(orderType.SaveNumscript, s)
	case *raftcmdpb.Order_DeleteNumscript:
		return p.processDeleteNumscript(orderType.DeleteNumscript, s)
	case *raftcmdpb.Order_CreateQueryCheckpoint:
		return p.processCreateQueryCheckpoint(orderType.CreateQueryCheckpoint, s)
	case *raftcmdpb.Order_DeleteQueryCheckpoint:
		return p.processDeleteQueryCheckpoint(orderType.DeleteQueryCheckpoint, s)
	case *raftcmdpb.Order_SetQueryCheckpointSchedule:
		return p.processSetQueryCheckpointSchedule(orderType.SetQueryCheckpointSchedule, s)
	case *raftcmdpb.Order_DeleteQueryCheckpointSchedule:
		return p.processDeleteQueryCheckpointSchedule(s)
	case *raftcmdpb.Order_SaveLedgerMetadata:
		return p.processAddLedgerMetadata(orderType.SaveLedgerMetadata, s)
	case *raftcmdpb.Order_DeleteLedgerMetadata:
		return p.processDeleteLedgerMetadata(orderType.DeleteLedgerMetadata, s)
	default:
		return nil, errors.New("invalid order type")
	}
}
