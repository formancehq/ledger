package processing

import (
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

// Context bundles per-batch shared state (caches, ledger metadata) and
// per-order/per-apply state (Scope, Boundaries, LedgerInfo) that every
// handler receives uniformly. The dispatcher (RequestProcessor) is the
// only producer; handlers read what they need from ctx and nothing else.
//
// The wrapper-level ledger name is NOT stored on Context: ledger-scoped
// processors take it as an explicit first parameter, system-scoped
// processors don't take it at all. Keeping it out of Context avoids a
// per-order field that is always empty for half the dispatch table.
//
// Per-order fields (Scope) are set by ProcessOrder before invoking the
// dispatcher; per-apply fields (Boundaries, LedgerInfo) are set by
// processApply / processMirrorIngest before dispatching to their
// apply-child handlers. Per-batch fields (caches) are owned by the
// *RequestProcessor and live for the lifetime of a ProcessOrders call.
type Context struct {
	// Per-order — set by the dispatcher before calling the handler.
	Scope Scope
	// InputsResolutionHash is the admission-derived Numscript inputs hash for
	// THIS order (from OrderTechnical). It lives on the parent Order, not the
	// CreateTransactionOrder handlers see, so the dispatcher stages it here for
	// the stale-inputs check. Empty when the order carries no resolution hash.
	InputsResolutionHash []byte

	// Per-apply — set by processApply / processMirrorIngest before
	// dispatching to apply-child handlers; nil/empty otherwise.
	Boundaries *raftcmdpb.LedgerBoundaries
	LedgerInfo *commonpb.LedgerInfo

	// Per-batch — owned by *RequestProcessor; passed by reference so
	// handlers see the same cache across orders. NumscriptCache lives
	// for the lifetime of the processor; CompiledTypes and AssetCache
	// are cleared at each ProcessOrders call.
	NumscriptCache *numscript.NumscriptCache
	CompiledTypes  map[string][]accounttype.CompiledType
	AssetCache     map[string]cachedAssetPrecision

	// BornEmptyLedgers tracks ledgers created in THIS proposal that have not
	// yet emitted any indexable data log. An index declared while its ledger
	// is in this set is "initial" (EN-1564): the indexbuilder marks it live
	// immediately instead of scheduling a historical backfill. Transient,
	// per-ProcessOrders-call bookkeeping — never persisted.
	BornEmptyLedgers map[string]struct{}
}

// markBornEmpty records a freshly-created ledger as having no indexable data
// yet. Lazy-inits the map so callers holding a bare Context (ProcessOrder and
// unit tests) work without a constructor change.
func (c *Context) markBornEmpty(ledger string) {
	if c.BornEmptyLedgers == nil {
		c.BornEmptyLedgers = make(map[string]struct{})
	}

	c.BornEmptyLedgers[ledger] = struct{}{}
}

// isBornEmpty reports whether the ledger was created earlier in this proposal
// and has emitted no indexable data log since. Nil-safe.
func (c *Context) isBornEmpty(ledger string) bool {
	_, ok := c.BornEmptyLedgers[ledger]

	return ok
}

// updateBornEmpty folds a just-produced log into the born-empty set: a
// CreatedLedger marks the ledger empty; the first indexable data log clears it.
// Driven off the emitted top-level log so the apply, mirror-ingest and
// order-skip paths (all wrapped as LogPayload_Apply) are covered from one call
// site. delete on a nil map is a no-op.
func (c *Context) updateBornEmpty(payload *commonpb.LogPayload) {
	if cl := payload.GetCreateLedger(); cl != nil {
		c.markBornEmpty(cl.GetName())

		return
	}

	if apply := payload.GetApply(); apply != nil {
		if isIndexableDataPayload(apply.GetLog().GetData()) {
			delete(c.BornEmptyLedgers, apply.GetLedgerName())
		}
	}
}

// isIndexableDataPayload reports whether a ledger log payload carries data the
// read-side indexbuilder backfills (mirrors indexbuilder.isDataLog). A ledger
// that has emitted one of these is no longer "born empty" for EN-1564. Nil-safe.
func isIndexableDataPayload(p *commonpb.LedgerLogPayload) bool {
	switch p.GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction,
		*commonpb.LedgerLogPayload_RevertedTransaction,
		*commonpb.LedgerLogPayload_SavedMetadata,
		*commonpb.LedgerLogPayload_DeletedMetadata,
		*commonpb.LedgerLogPayload_OrderSkipped:
		return true
	default:
		return false
	}
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

// compiledTypesFor returns compiled account types for the given ledger,
// using the per-batch cache to avoid redundant ParsePattern calls
// across orders. The cache map is mutated in place. Free function (not
// a method on RequestProcessor) so handlers reach for it via explicit
// parameter — see the isolation goal of the processor refactor.
func compiledTypesFor(cache map[string][]accounttype.CompiledType, ledger string, info *commonpb.LedgerInfo) []accounttype.CompiledType {
	if info == nil || len(info.GetAccountTypes()) == 0 {
		return nil
	}

	if cached, ok := cache[ledger]; ok {
		return cached
	}

	compiled := accounttype.CompileTypes(info.GetAccountTypes())
	cache[ledger] = compiled

	return compiled
}

// invalidateCompiledTypes drops the cached entry for ledger so the next
// compiledTypesFor recompiles. Free function for the same reason as
// compiledTypesFor above.
func invalidateCompiledTypes(cache map[string][]accounttype.CompiledType, ledger string) {
	delete(cache, ledger)
}

// OrdersResult bundles the per-order log slice and the derived values
// applyProposal would otherwise rebuild by walking it again
// (extractLogSequenceRange, the createdLogs filter). Accumulating these
// during ProcessOrders' single pass eliminates the redundant post-orders
// walks on the FSM hot path.
type OrdersResult struct {
	// Logs has one entry per input order. Today every entry is a
	// CreatedLog: per-batch idempotency moved out of ProcessOrders into
	// the FSM apply path (which short-circuits replays before calling
	// the processor), so ReferenceSequence entries are no longer produced
	// here. The slice type is kept for proto compatibility.
	Logs []*raftcmdpb.CreatedLogOrReference

	// CreatedLogs is the parallel slice of just the created logs in
	// payload form. With idempotency out of ProcessOrders this is a
	// trivial fold over Logs, but exposing it as a field lets
	// applyProposal skip the rebuild walk.
	CreatedLogs []*commonpb.Log

	// MinLogSequence / MaxLogSequence are the min/max sequence among
	// CreatedLogs. Both are 0 when CreatedLogs is empty.
	MinLogSequence uint64
	MaxLogSequence uint64
}

// ProcessOrders processes a list of orders and returns the resulting logs
// plus the derived accumulators applyProposal needs (created-log filter,
// min/max sequence range). scopeFactory is invoked once per order to
// build an independent gatedScope whose coverage map is the union of:
//   - the AttributePlans flagged by order.coverage_bits, and
//   - the resolved Productions flagged by order.production_bits.
//
// Successive calls return independent scopes — the previous scope's
// coverage map is never mutated. Per-order isolation is therefore
// structural: order N's scope cannot read keys declared by order M.
func (p *RequestProcessor) ProcessOrders(orders []*raftcmdpb.Order, scopeFactory ScopeFactory, sink SignalSink) (*OrdersResult, domain.Describable) {
	clear(p.compiledTypesCache)
	clear(p.assetCache)

	// Build the per-call Context once with the persistent caches. Per-order
	// fields (Scope, Ledger) are reset by ProcessOrder before dispatch;
	// per-apply fields (Boundaries, LedgerInfo) are populated by the apply
	// orchestrators.
	ctx := &Context{
		NumscriptCache: p.numscriptCache,
		CompiledTypes:  p.compiledTypesCache,
		AssetCache:     p.assetCache,
	}

	result := &OrdersResult{
		Logs: make([]*raftcmdpb.CreatedLogOrReference, len(orders)),
	}
	logs := result.Logs

	for i, order := range orders {
		orderScope, scopeErr := scopeFactory.NewScope(order.GetTechnical().GetCoverageBits())
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

		// Tag subsequent volume touches with this order's index so the
		// WriteSet can compute per-log purged volumes at Merge time
		// (see Log.purged_volumes). Scopes that don't implement
		// OrderTagger (mocks, recovery, technical-only flows) skip the
		// tagging silently — those code paths don't need the per-log
		// accounting.
		if tagger, ok := orderScope.(OrderTagger); ok {
			tagger.BeginOrder(i)
		}

		// Per-order rollback for orders that opt in via skippable_reasons:
		// every Scope mutation made by the sub-processor goes through an
		// orderOverlayScope that buffers writes locally. If ProcessOrders
		// later converts the failure into an OrderSkippedLog, the overlay
		// is dropped without Commit() and the parent state stays untouched
		// — the order is effectively rolled back. Orders without
		// skippable_reasons retain the historical zero-overhead path.
		//
		// The overlay is wrapped in a skipSafeScope: any mutation whose
		// effect the overlay does NOT buffer (signing keys, maintenance
		// mode, chapter mutations, numscript library, query-checkpoint
		// state) panics via trapUnbuffered rather than silently leaking to
		// the parent — assert.Unreachable is layered on top so the same
		// call surfaces as a first-class finding under Antithesis, but
		// the panic is the hard-stop that catches the invariant outside
		// Antithesis where the SDK's assert.Unreachable is a no-op.
		// skipSafeScope does not embed Scope, so a future method added to
		// the interface fails to compile until it is explicitly
		// classified there.
		processScope := orderScope

		var overlay *orderOverlayScope
		if len(orderSkippableReasons(order)) > 0 {
			overlay = newOrderOverlayScope(orderScope)
			processScope = newSkipSafeScope(overlay)
		}

		payload, err := p.processOrder(order, processScope, ctx)
		if err != nil {
			if skippedPayload, matched := matchOrderSkip(order, err); matched {
				// Drop the overlay (no Commit) → all writes the sub-
				// processor staged are rolled back atomically. The
				// boundary slot (NextLogId / Date) IS consumed on the
				// PARENT scope so the skip log gets a per-ledger id
				// and date like every other ledger log — the read-side
				// index keys per-ledger logs by (ledger, log_id) and
				// silently overwrites if every skip lands at id 0.
				if err := assignSkipLogIDAndDate(orderScope, order, skippedPayload); err != nil {
					return nil, err
				}

				nextSequenceID := orderScope.IncrementNextSequenceID()
				skipLog := &commonpb.Log{
					Sequence: nextSequenceID,
					Payload:  skippedPayload,
				}
				logs[i] = &raftcmdpb.CreatedLogOrReference{
					Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{
						CreatedLog: skipLog,
					},
				}

				sink.Absorb(order, skipLog)
				ctx.updateBornEmpty(skippedPayload)

				result.CreatedLogs = append(result.CreatedLogs, skipLog)
				if result.MinLogSequence == 0 || nextSequenceID < result.MinLogSequence {
					result.MinLogSequence = nextSequenceID
				}
				if nextSequenceID > result.MaxLogSequence {
					result.MaxLogSequence = nextSequenceID
				}

				continue
			}

			return nil, err
		}

		// No-log outcome: a processor may deterministically decide the order
		// produces no fresh ledger log while still succeeding (payload==nil,
		// err==nil). The only such case today is an idempotent mirror replay
		// (processMirrorIngest guards on LastMirrorV2LogId), which mutates
		// nothing and must leave no audit-visible log. Skip the log slot
		// entirely — do NOT consume a sequence id, absorb into the sink, or
		// append a degenerate Log{Payload:nil}. The nil slot mirrors the
		// idempotency-replay ReferenceSequence path and is skipped identically
		// by WriteSet.Merge (GetCreatedLog()==nil) and checkCloseChapter.
		//
		// Any overlay staged by such an order is dropped without Commit(): a
		// no-log outcome makes no persistent mutation, so there is nothing to
		// flush. (Mirror ingest declares no skippable_reasons, so no overlay is
		// created here in practice.)
		if payload == nil {
			continue
		}

		if overlay != nil {
			if err := overlay.Commit(); err != nil {
				// Coverage-miss surfaced by a staged Delete (invariant #6):
				// the FSM apply path must not silently drop tombstones —
				// wrap as ErrStorageOperation so the order fails loudly
				// (mirrors buildPostCommitVolumes / applyPosting).
				return nil, &domain.ErrStorageOperation{Operation: "committing order overlay", Cause: err}
			}
		}

		nextSequenceID := orderScope.IncrementNextSequenceID()
		log := &commonpb.Log{
			Sequence: nextSequenceID,
			Payload:  payload,
		}

		logs[i] = &raftcmdpb.CreatedLogOrReference{
			Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{
				CreatedLog: log,
			},
		}

		// Absorb the (order, log) pair into the sink in the same
		// per-order pass that produced the log — no second walk over
		// `logs`. Processors stay focused on (a) mutating state via
		// Scope and (b) returning the log; the sink interprets the
		// log payload and updates whatever cross-order accumulator
		// the framework needs.
		sink.Absorb(order, log)
		ctx.updateBornEmpty(payload)

		// Accumulate the derivations applyProposal previously rebuilt
		// by walking the log slice again (createdLogs filter +
		// extractLogSequenceRange).
		result.CreatedLogs = append(result.CreatedLogs, log)
		if result.MinLogSequence == 0 || nextSequenceID < result.MinLogSequence {
			result.MinLogSequence = nextSequenceID
		}
		if nextSequenceID > result.MaxLogSequence {
			result.MaxLogSequence = nextSequenceID
		}
	}

	return result, nil
}

// HashProposal returns the idempotency hash of a proposal: a blake3 digest over
// its orders' content hashes, in order. Because the orders' composition and
// ordering are folded in, a retry of the SAME atomic batch hashes identically,
// while a differently-composed batch (e.g. a lone re-submission of one order)
// hashes differently. The FSM dedups and freezes a proposal under this hash.
// Single-threaded apply only (reuses p.hashBuf via computeOrderHash).
func (p *RequestProcessor) HashProposal(proposal *raftcmdpb.Proposal) []byte {
	h := blake3.New()
	for _, order := range proposal.GetOrders() {
		var oh []byte
		oh, p.hashBuf = hashOrder(order, p.hashBuf)
		_, _ = h.Write(oh)
	}

	return h.Sum(nil)
}

// HashOrders is the allocation-per-call form of HashProposal, for callers that
// do not have a reusable buffer — the integrity checker re-derives a frozen
// proposal's hash from its persisted audit orders. It must stay byte-identical
// to HashProposal so the recomputed hash matches what the FSM stored.
func HashOrders(orders []*raftcmdpb.Order) []byte {
	h := blake3.New()

	var buf []byte

	for _, order := range orders {
		var oh []byte
		oh, buf = hashOrder(order, buf)
		_, _ = h.Write(oh)
	}

	return h.Sum(nil)
}

// MarshalOrderBusinessIntent returns the deterministic wire bytes of an order's
// business intent: the order with its OrderTechnical sub-message excluded. This
// is the SINGLE definition of what both the audit hash chain
// (AuditItem.serialized_order) and the idempotency hash (hashOrder) bind — so a
// new technical field can never silently diverge the two.
//
// All admission-derived fields live on OrderTechnical, which is excluded so the
// SAME logical request always serializes identically (idempotency dedup / replay
// must match across retries, and the audit proves only accepted intent).
// OrderTechnical carries:
//
//   - coverage_bits: admission rebuilds it from the proposal-wide ExecutionPlan,
//     so the same order in a different batch would otherwise serialize differently.
//   - inputs_resolution_hash: admission recomputes it by re-resolving the
//     Numscript against CURRENT balances/metadata, so a retry of a state-reading
//     script re-resolves at a changed balance and would otherwise differ — turning
//     a legitimate replay into an IDEMPOTENCY_KEY_CONFLICT (EN-1406 P1-3). It is a
//     preload/staleness hint, not logical identity.
//   - preload_unavailable: an admission-forwarding marker, never logical identity.
//
// out is marshalled into buf[:0] (grown as needed) and returned; reuse it as buf
// on the next call to amortize allocations. Pass nil to allocate a fresh slice
// the caller can retain (the audit path stores one slice per order). Single-
// threaded apply only: it transiently nils order.Technical, marshals, then
// restores, leaving the live order byte-identical — mirroring the historical
// hashOrder behaviour and safe because the FSM apply path is the sole caller.
func MarshalOrderBusinessIntent(order *raftcmdpb.Order, buf []byte) []byte {
	savedTechnical := order.GetTechnical()
	order.Technical = nil

	out := order.MarshalDeterministicVT(buf[:0])

	order.Technical = savedTechnical

	return out
}

// hashOrder computes a blake3 hash of one order's business intent, returning the
// hash and the (grown) marshal buffer to reuse. The bytes hashed are exactly the
// business-intent projection MarshalOrderBusinessIntent produces, so the
// idempotency hash and the audit serialization bind the identical bytes.
func hashOrder(order *raftcmdpb.Order, buf []byte) (hash []byte, grownBuf []byte) {
	buf = MarshalOrderBusinessIntent(order, buf)

	sum := blake3.Sum256(buf)

	return sum[:], buf
}

// ProcessOrder processes an Order and returns the resulting LogPayload.
// Dispatch is two-level: first the wrapper (ledger-scoped vs system-scoped),
// then the payload inside the wrapper. The wrapper-level split is the
// structural invariant that lets the audit log attribute each entry to a
// ledger via a single accessor.
//
// This entry point is kept for callers that don't already hold a Context
// (tests, recovery flows). It allocates a transient Context wrapping the
// processor's per-batch caches and forwards to processOrder.
func (p *RequestProcessor) ProcessOrder(order *raftcmdpb.Order, s Scope) (*commonpb.LogPayload, domain.Describable) {
	ctx := &Context{
		NumscriptCache: p.numscriptCache,
		CompiledTypes:  p.compiledTypesCache,
		AssetCache:     p.assetCache,
	}

	return p.processOrder(order, s, ctx)
}

// processOrder is the internal dispatcher: it stages per-order ctx.Scope
// (and clears per-apply fields) before delegating to a wrapper-level
// dispatcher. The ledger name is passed explicitly to ledger-scoped
// handlers; system-scoped handlers don't receive it.
func (p *RequestProcessor) processOrder(order *raftcmdpb.Order, s Scope, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	ctx.Scope = s
	// Stage this order's admission-derived inputs hash (from OrderTechnical) for
	// the stale-inputs check in the numscript producer, which only sees the
	// CreateTransactionOrder.
	ctx.InputsResolutionHash = order.GetTechnical().GetInputsResolutionHash()
	// Reset per-apply fields — only processApply/processMirrorIngest set them.
	ctx.Boundaries = nil
	ctx.LedgerInfo = nil

	// preload_unavailable: admission couldn't build this order's preload and
	// forwarded it (idempotency key present) instead of failing fast, so the FSM
	// arbitrates. We reach here only for a NON-replay — the per-proposal
	// idempotency dedup runs before ProcessOrders and short-circuits a frozen
	// outcome, so a replay never reaches this dispatcher. Without a preload the
	// order MUST NOT execute (its coverage is empty); reject deterministically
	// with the retryable, non-frozen ErrPreloadUnavailable BEFORE any read, so
	// the outcome is intentional rather than a coverage-miss against drifted
	// apply-time state. Not frozen (Kind=Unavailable): a preload-unavailable
	// retry must never shadow the real outcome of a concurrent same-key proposal.
	// See EN-1406.
	if order.GetTechnical().GetPreloadUnavailable() {
		return nil, domain.ErrPreloadUnavailable
	}

	switch orderType := order.GetType().(type) {
	case *raftcmdpb.Order_LedgerScoped:
		return processLedgerScoped(orderType.LedgerScoped, ctx)
	case *raftcmdpb.Order_SystemScoped:
		return processSystemScoped(orderType.SystemScoped, ctx)
	default:
		return nil, &domain.ErrInvalidOrderType{TypeName: fmt.Sprintf("%T", order.GetType())}
	}
}

// processLedgerScoped dispatches a ledger-scoped order payload. It
// extracts the wrapper-level ledger name once and passes it explicitly
// to each handler — keeping ctx free of an "always present for half the
// dispatch table" field.
func processLedgerScoped(ls *raftcmdpb.LedgerScopedOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	ledger := ls.GetLedger()
	switch payload := ls.GetPayload().(type) {
	case *raftcmdpb.LedgerScopedOrder_Apply:
		return processApply(ledger, payload.Apply, ctx)
	case *raftcmdpb.LedgerScopedOrder_CreateLedger:
		return processCreateLedger(ledger, payload.CreateLedger, ctx)
	case *raftcmdpb.LedgerScopedOrder_DeleteLedger:
		return processDeleteLedger(ledger, ctx)
	case *raftcmdpb.LedgerScopedOrder_MirrorIngest:
		return processMirrorIngest(ledger, payload.MirrorIngest, ctx)
	case *raftcmdpb.LedgerScopedOrder_PromoteLedger:
		return processPromoteLedger(ledger, ctx)
	case *raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata:
		return processAddLedgerMetadata(ledger, payload.SaveLedgerMetadata, ctx)
	case *raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata:
		return processDeleteLedgerMetadata(ledger, payload.DeleteLedgerMetadata, ctx)
	case *raftcmdpb.LedgerScopedOrder_SaveNumscript:
		return processSaveNumscript(ledger, payload.SaveNumscript, ctx)
	case *raftcmdpb.LedgerScopedOrder_CreatePreparedQuery:
		return processCreatePreparedQuery(ledger, payload.CreatePreparedQuery, ctx)
	case *raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery:
		return processUpdatePreparedQuery(ledger, payload.UpdatePreparedQuery, ctx)
	case *raftcmdpb.LedgerScopedOrder_DeletePreparedQuery:
		return processDeletePreparedQuery(ledger, payload.DeletePreparedQuery, ctx)
	default:
		return nil, &domain.ErrInvalidOrderType{TypeName: fmt.Sprintf("%T", ls.GetPayload())}
	}
}

// processSystemScoped dispatches a system-scoped order payload. These commands
// affect cluster or global state and are never attributed to a single ledger.
func processSystemScoped(ss *raftcmdpb.SystemScopedOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	switch payload := ss.GetPayload().(type) {
	case *raftcmdpb.SystemScopedOrder_RegisterSigningKey:
		return processRegisterSigningKey(payload.RegisterSigningKey, ctx)
	case *raftcmdpb.SystemScopedOrder_RevokeSigningKey:
		return processRevokeSigningKey(payload.RevokeSigningKey, ctx)
	case *raftcmdpb.SystemScopedOrder_SetSigningConfig:
		return processSetSigningConfig(payload.SetSigningConfig, ctx)
	case *raftcmdpb.SystemScopedOrder_SetMaintenanceMode:
		return processSetMaintenanceMode(payload.SetMaintenanceMode, ctx)
	case *raftcmdpb.SystemScopedOrder_AddEventsSink:
		return processAddEventsSink(payload.AddEventsSink, ctx)
	case *raftcmdpb.SystemScopedOrder_RemoveEventsSink:
		return processRemoveEventsSink(payload.RemoveEventsSink, ctx)
	case *raftcmdpb.SystemScopedOrder_CloseChapter:
		return processCloseChapter(payload.CloseChapter, ctx)
	case *raftcmdpb.SystemScopedOrder_SealChapter:
		return processSealChapter(payload.SealChapter, ctx)
	case *raftcmdpb.SystemScopedOrder_ArchiveChapter:
		return processArchiveChapter(payload.ArchiveChapter, ctx)
	case *raftcmdpb.SystemScopedOrder_ConfirmArchiveChapter:
		return processConfirmArchiveChapter(payload.ConfirmArchiveChapter, ctx)
	case *raftcmdpb.SystemScopedOrder_SetChapterSchedule:
		return processSetChapterSchedule(payload.SetChapterSchedule, ctx)
	case *raftcmdpb.SystemScopedOrder_DeleteChapterSchedule:
		return processDeleteChapterSchedule(ctx)
	case *raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint:
		return processCreateQueryCheckpoint(payload.CreateQueryCheckpoint, ctx)
	case *raftcmdpb.SystemScopedOrder_DeleteQueryCheckpoint:
		return processDeleteQueryCheckpoint(payload.DeleteQueryCheckpoint, ctx)
	case *raftcmdpb.SystemScopedOrder_SetQueryCheckpointSchedule:
		return processSetQueryCheckpointSchedule(payload.SetQueryCheckpointSchedule, ctx)
	case *raftcmdpb.SystemScopedOrder_DeleteQueryCheckpointSchedule:
		return processDeleteQueryCheckpointSchedule(ctx)
	default:
		return nil, &domain.ErrInvalidOrderType{TypeName: fmt.Sprintf("%T", ss.GetPayload())}
	}
}
