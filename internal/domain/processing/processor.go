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
// Dispatch is two-level: first the wrapper (ledger-scoped vs system-scoped),
// then the payload inside the wrapper. The wrapper-level split is the
// structural invariant that lets the audit log attribute each entry to a
// ledger via a single accessor.
func (p *RequestProcessor) ProcessOrder(order *raftcmdpb.Order, s Scope) (*commonpb.LogPayload, domain.Describable) {
	switch orderType := order.GetType().(type) {
	case *raftcmdpb.Order_LedgerScoped:
		return p.processLedgerScoped(orderType.LedgerScoped, s)
	case *raftcmdpb.Order_SystemScoped:
		return p.processSystemScoped(orderType.SystemScoped, s)
	default:
		return nil, &domain.ErrInvalidOrderType{TypeName: fmt.Sprintf("%T", order.GetType())}
	}
}

// processLedgerScoped dispatches a ledger-scoped order payload, threading the
// wrapper-level ledger name down to each processor (the sub-messages no
// longer carry it themselves).
func (p *RequestProcessor) processLedgerScoped(ls *raftcmdpb.LedgerScopedOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	ledger := ls.GetLedger()
	switch payload := ls.GetPayload().(type) {
	case *raftcmdpb.LedgerScopedOrder_Apply:
		return p.processApply(ledger, payload.Apply, s)
	case *raftcmdpb.LedgerScopedOrder_CreateLedger:
		return p.processCreateLedger(ledger, payload.CreateLedger, s)
	case *raftcmdpb.LedgerScopedOrder_DeleteLedger:
		return p.processDeleteLedger(ledger, s)
	case *raftcmdpb.LedgerScopedOrder_MirrorIngest:
		return p.processMirrorIngest(ledger, payload.MirrorIngest, s)
	case *raftcmdpb.LedgerScopedOrder_PromoteLedger:
		return p.processPromoteLedger(ledger, s)
	case *raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata:
		return p.processAddLedgerMetadata(ledger, payload.SaveLedgerMetadata, s)
	case *raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata:
		return p.processDeleteLedgerMetadata(ledger, payload.DeleteLedgerMetadata, s)
	case *raftcmdpb.LedgerScopedOrder_SaveNumscript:
		return p.processSaveNumscript(ledger, payload.SaveNumscript, s)
	case *raftcmdpb.LedgerScopedOrder_DeleteNumscript:
		return p.processDeleteNumscript(ledger, payload.DeleteNumscript, s)
	case *raftcmdpb.LedgerScopedOrder_CreatePreparedQuery:
		return p.processCreatePreparedQuery(ledger, payload.CreatePreparedQuery, s)
	case *raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery:
		return p.processUpdatePreparedQuery(ledger, payload.UpdatePreparedQuery, s)
	case *raftcmdpb.LedgerScopedOrder_DeletePreparedQuery:
		return p.processDeletePreparedQuery(ledger, payload.DeletePreparedQuery, s)
	default:
		return nil, &domain.ErrInvalidOrderType{TypeName: fmt.Sprintf("%T", ls.GetPayload())}
	}
}

// processSystemScoped dispatches a system-scoped order payload. These commands
// affect cluster or global state and are never attributed to a single ledger.
func (p *RequestProcessor) processSystemScoped(ss *raftcmdpb.SystemScopedOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	switch payload := ss.GetPayload().(type) {
	case *raftcmdpb.SystemScopedOrder_RegisterSigningKey:
		return p.processRegisterSigningKey(payload.RegisterSigningKey, s)
	case *raftcmdpb.SystemScopedOrder_RevokeSigningKey:
		return p.processRevokeSigningKey(payload.RevokeSigningKey, s)
	case *raftcmdpb.SystemScopedOrder_SetSigningConfig:
		return p.processSetSigningConfig(payload.SetSigningConfig, s)
	case *raftcmdpb.SystemScopedOrder_SetMaintenanceMode:
		return p.processSetMaintenanceMode(payload.SetMaintenanceMode, s)
	case *raftcmdpb.SystemScopedOrder_AddEventsSink:
		return p.processAddEventsSink(payload.AddEventsSink, s)
	case *raftcmdpb.SystemScopedOrder_RemoveEventsSink:
		return p.processRemoveEventsSink(payload.RemoveEventsSink, s)
	case *raftcmdpb.SystemScopedOrder_CloseChapter:
		return p.processCloseChapter(payload.CloseChapter, s)
	case *raftcmdpb.SystemScopedOrder_SealChapter:
		return p.processSealChapter(payload.SealChapter, s)
	case *raftcmdpb.SystemScopedOrder_ArchiveChapter:
		return p.processArchiveChapter(payload.ArchiveChapter, s)
	case *raftcmdpb.SystemScopedOrder_ConfirmArchiveChapter:
		return p.processConfirmArchiveChapter(payload.ConfirmArchiveChapter, s)
	case *raftcmdpb.SystemScopedOrder_SetChapterSchedule:
		return p.processSetChapterSchedule(payload.SetChapterSchedule, s)
	case *raftcmdpb.SystemScopedOrder_DeleteChapterSchedule:
		return p.processDeleteChapterSchedule(s)
	case *raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint:
		return p.processCreateQueryCheckpoint(payload.CreateQueryCheckpoint, s)
	case *raftcmdpb.SystemScopedOrder_DeleteQueryCheckpoint:
		return p.processDeleteQueryCheckpoint(payload.DeleteQueryCheckpoint, s)
	case *raftcmdpb.SystemScopedOrder_SetQueryCheckpointSchedule:
		return p.processSetQueryCheckpointSchedule(payload.SetQueryCheckpointSchedule, s)
	case *raftcmdpb.SystemScopedOrder_DeleteQueryCheckpointSchedule:
		return p.processDeleteQueryCheckpointSchedule(s)
	default:
		return nil, &domain.ErrInvalidOrderType{TypeName: fmt.Sprintf("%T", ss.GetPayload())}
	}
}
