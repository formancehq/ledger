package processing

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/zeebo/blake3"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
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
// scopeFactory is invoked once per order to build an independent gatedScope
// whose coverage map is the union of:
//   - the AttributePlans flagged by order.coverage_bits, and
//   - the resolved Productions flagged by order.production_bits.
//
// Successive calls return independent scopes — the previous scope's
// coverage map is never mutated. Per-order isolation is therefore
// structural: order N's scope cannot read keys declared by order M.
func (p *RequestProcessor) ProcessOrders(orders []*raftcmdpb.Order, scopeFactory ScopeFactory) ([]*raftcmdpb.CreatedLogOrReference, domain.Describable) {
	clear(p.compiledTypesCache)
	clear(p.assetCache)

	logs := make([]*raftcmdpb.CreatedLogOrReference, len(orders))

	for i, order := range orders {
		orderScope, scopeErr := scopeFactory.NewScope(order.GetCoverageBits())
		if scopeErr != nil {
			// Invariant violation surfaced by the FSM: the execution plan
			// shipped by admission is structurally inconsistent. Detected
			// BEFORE any cache mutation lands so the proposal is rejected
			// cleanly and the next one can apply. NewScope's contract is
			// to return *ErrInvalidExecutionPlan only.
			var invalid *domain.ErrInvalidExecutionPlan
			if !errors.As(scopeErr, &invalid) {
				return nil, &domain.ErrInvalidExecutionPlan{Reason_: scopeErr.Error()}
			}

			return nil, invalid
		}

		// Compute idempotency hash once if needed (reused for check + store).
		hasIdempotency := order.GetIdempotency() != nil && order.GetIdempotency().GetKey() != ""

		var orderHash []byte
		if hasIdempotency {
			orderHash = p.computeOrderHash(order)

			ikKey := domain.IdempotencyKey{Key: order.GetIdempotency().GetKey()}

			storedValue, err := orderScope.GetIdempotencyKey(ikKey)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrIdempotencyCheckFailed{Cause: err}
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
		payload, err := p.ProcessOrder(order, orderScope)
		if err != nil {
			return nil, err
		}

		nextSequenceID := orderScope.IncrementNextSequenceID()
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
			orderScope.PutIdempotencyKey(
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

// computeOrderHash computes a blake3 hash of the order content for
// idempotency checking. Per-attempt fields that admission derives from
// the proposal context (not from the user request) are excluded:
//   - Idempotency: nonce changes per attempt by definition.
//   - CoverageBits: admission rebuilds it from the proposal-wide
//     ExecutionPlan, so the same logical request in a different batch
//     produces a different bitset.
//   - ProductionBits: same as CoverageBits — admission rebuilds it from
//     ExecutionPlan.productions, which depends on which producer orders
//     happen to share the batch.
//
// Two retries of the same logical request MUST hash identically even
// when they land in different batches. Reuses p.hashBuf across calls
// to avoid allocations; safe because ProcessOrders is single-threaded.
func (p *RequestProcessor) computeOrderHash(order *raftcmdpb.Order) []byte {
	// Temporarily nil the per-attempt fields, marshal, then restore them.
	// This avoids a full CloneVT of the order.
	savedIdempotency := order.GetIdempotency()
	savedCoverage := order.GetCoverageBits()
	order.Idempotency = nil
	order.CoverageBits = nil

	p.hashBuf = order.MarshalDeterministicVT(p.hashBuf[:0])

	order.Idempotency = savedIdempotency
	order.CoverageBits = savedCoverage

	hash := blake3.Sum256(p.hashBuf)

	return hash[:]
}

// ProcessOrder processes an Order and returns the resulting LogPayload.
func (p *RequestProcessor) ProcessOrder(order *raftcmdpb.Order, s Scope) (*commonpb.LogPayload, domain.Describable) {
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
	case *raftcmdpb.Order_CloseChapter:
		return p.processCloseChapter(orderType.CloseChapter, s)
	case *raftcmdpb.Order_SealChapter:
		return p.processSealChapter(orderType.SealChapter, s)
	case *raftcmdpb.Order_ArchiveChapter:
		return p.processArchiveChapter(orderType.ArchiveChapter, s)
	case *raftcmdpb.Order_ConfirmArchiveChapter:
		return p.processConfirmArchiveChapter(orderType.ConfirmArchiveChapter, s)
	case *raftcmdpb.Order_SetChapterSchedule:
		return p.processSetChapterSchedule(orderType.SetChapterSchedule, s)
	case *raftcmdpb.Order_DeleteChapterSchedule:
		return p.processDeleteChapterSchedule(s)
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
		return nil, &domain.ErrInvalidOrderType{TypeName: fmt.Sprintf("%T", order.GetType())}
	}
}
