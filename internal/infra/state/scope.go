package state

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ErrCoverageMiss is returned by gatedScope when the FSM reads a key the
// proposer did not declare in this scope's coverage_bits / production_bits.
// It indicates a malformed execution plan — admission failed to flag a key
// the FSM then reads — so the right outcome is a business-level rejection
// that surfaces the gap to the proposer. It is NOT an FSM invariant
// violation; the cache stays in lockstep with Pebble because gatedScope
// refuses to forward the read before any mutation lands.
//
// Implements domain.Describable (KindInternal) so applyProposal can wrap
// it in a domain.BusinessError and surface it via ApplyResult.Error
// without killing the FSM apply loop.
type ErrCoverageMiss struct {
	Attribute    string // sub-attribute name ("ledgers", "boundaries", …)
	CanonicalHex string
	IDHex        string
	RaftIndex    uint64
}

func (e *ErrCoverageMiss) Error() string {
	return fmt.Sprintf("preload coverage miss (kind=%s id=%s raft_index=%d)",
		e.Attribute, e.IDHex, e.RaftIndex)
}

func (*ErrCoverageMiss) Reason() string { return domain.ErrReasonCoverageMiss }
func (e *ErrCoverageMiss) Metadata() map[string]string {
	return map[string]string{
		"attribute":     e.Attribute,
		"canonical_hex": e.CanonicalHex,
		"id_hex":        e.IDHex,
		"raft_index":    strconv.FormatUint(e.RaftIndex, 10),
	}
}

// gatedScope decorates an embedded *WriteSet (the raw engine) with the
// per-scope coverage gate. Every cache-attribute Get opens with a
// coverage check against the scope's coverage slots; the engine never
// sees a coverage concept. gatedScope satisfies both processing.Scope
// (the handler-facing interface) AND processing.ScopeFactory: it acts
// as its own factory, returning itself reconfigured from each
// NewScope / NewProposalScope call.
//
// One gatedScope is allocated per proposal and reused across the
// sequential applyTechnicalUpdates → ProcessOrders →
// ValidateTransientVolumes phases. NewScope truncates the slots in
// place and re-applies the plans selected by coverage_bits; the slots'
// backing arrays survive across calls, so steady-state runs allocate
// nothing in the gate path. The FSM apply loop is strictly sequential
// so no two scopes are alive at once — the previous configuration is
// always done with by the time the next NewScope fires.
//
// Per-order isolation is enforced by the coverage_bits the proposer
// stamps on each order: order N's bits only flag the plans it
// declared, so order N's reconfigured scope cannot read keys declared
// by order M. Empty bits admit no plan; the only way to obtain a
// proposal-wide scope (admit every declared plan) is the separate
// NewProposalScope entry point, used only by ValidateTransientVolumes.
type gatedScope struct {
	*WriteSet // embedded: implicit forward for every engine method we don't override

	// plans is the proposal's ExecutionPlan. NewScope re-applies a
	// subset (selected by coverage_bits) into coverage every call.
	plans []*raftcmdpb.AttributeCoverage

	// coverage is a dense slot table — one entry per supported
	// sub-attribute kind (see cacheAttrKinds). coverageSlotIndex maps
	// a sub-attribute code to its slot index (or -1 when the kind is
	// not gated). Each slot is a slice of declared U128 ids; CheckCoverage
	// scans it linearly. The dense [N] layout (instead of [256])
	// keeps the struct under ~300 B (vs ~2 KB) so the per-NewScope
	// truncate/append cycle stays cache-resident.
	coverage  coverageSlots
	logger    logging.Logger
	miss      metric.Int64Counter
	raftIndex uint64

	// scratch is a reusable buffer for building canonical key bytes on
	// the fly (see CheckCoverage). The FSM apply path is single-threaded
	// under fsm.mu, and every consumer of the produced bytes (xxh3 hash
	// in attributes.MakeKey, hex encoding on miss) copies or consumes
	// them synchronously — safe to reuse across every gated read/write.
	scratch []byte

	// Per-kind gated accessors — built once at NewScopeFactory time,
	// wrap the embedded WriteSet's bare accessors with CheckCoverage on
	// reads. The accessor pointers carry the gatedScope pointer (for
	// CheckCoverage) and the sub-attribute code; the coverage map they
	// dispatch against is the gatedScope's own `coverage` field, which
	// NewScope/NewProposalScope rewrite in place between phases.
	gatedLedgers               *gatedAccessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader]
	gatedBoundaries            *gatedAccessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader]
	gatedVolumes               *gatedAccessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader]
	gatedAccountMetadata       *gatedAccessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]
	gatedLedgerMetadata        *gatedAccessor[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]
	gatedTransactionReferences *gatedAccessor[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader]
	gatedTransactionStates     *gatedAccessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]
	gatedPreparedQueries       *gatedAccessor[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader]
	gatedIndexes               *gatedAccessor[domain.IndexKey, *commonpb.Index, commonpb.IndexReader]
}

// validatePlan rejects AttributeCoverages whose envelope is malformed:
//   - missing AttributeID or an ID payload that is not the 16-byte U128
//     we expect (attributes.U128FromBytes would silently zero-pad);
//   - attr_code that the FSM does not handle (a seed intent's
//     MirrorPreload would route the write to an orphan 0xFF Pebble
//     slot; scope validation only catches selected plans later).
//
// Run at every gate that touches plans (Preload entry, applyPlans,
// applyAllPlans) so a forged ExecutionPlan never reaches a side-effecting
// call. The shared check keeps the three sites in lock-step.
func validatePlan(plan *raftcmdpb.AttributeCoverage, idx int) *domain.ErrInvalidExecutionPlan {
	id := plan.GetId()
	if id == nil || len(id.GetId()) != 16 {
		return &domain.ErrInvalidExecutionPlan{
			Reason_: fmt.Sprintf("plans[%d]: AttributeCoverage must carry a 16-byte AttributeID (got %d bytes)", idx, len(id.GetId())),
		}
	}

	kind := byte(plan.GetAttrCode())
	if coverageSlotIndex[kind] < 0 {
		return &domain.ErrInvalidExecutionPlan{
			Reason_: fmt.Sprintf("plans[%d]: AttributeCoverage declares attr_code 0x%02x which the FSM does not handle", idx, kind),
		}
	}

	return nil
}

// validateCoverageBits walks the bits set in coverageBits (LSB-first) and
// checks each flagged plan WITHOUT mutating any coverage slot: every bit
// must index a real plan entry, and every selected plan must carry a
// 16-byte AttributeID plus an attr_code the FSM handles. The optional
// visit callback fires once per set bit (in ascending bit order) so the
// mutating caller (applyPlans) can count selected plans per slot without
// re-walking the bitset.
//
// Extracted so both the scope-construction path (applyPlans) and the
// mutation-free technical-update preflight (preflightTechnicalUpdates)
// validate a coverage bitset against the same rules — the preflight must
// reject a malformed bitset BEFORE any handler runs, so it cannot afford
// applyPlans' slot mutation.
func validateCoverageBits(plans []*raftcmdpb.AttributeCoverage, coverageBits []byte, visit func(bit int)) *domain.ErrInvalidExecutionPlan {
	for byteIdx, b := range coverageBits {
		for b != 0 {
			bit := byteIdx*8 + lsbIndex(b)
			b &= b - 1

			if bit >= len(plans) {
				return &domain.ErrInvalidExecutionPlan{
					Reason_: fmt.Sprintf("coverage_bits flags position %d past plans length %d", bit, len(plans)),
				}
			}

			if err := validatePlan(plans[bit], bit); err != nil {
				return err
			}

			if visit != nil {
				visit(bit)
			}
		}
	}

	return nil
}

// applyPlans walks the plans slice narrowed to the entries flagged in
// coverageBits (LSB-first) and appends each selected plan's U128 to the
// matching coverage slot. Empty coverageBits means "admit no plan" —
// CheckCoverage on the resulting scope will miss every key. The
// proposal-wide variant is applyAllPlans (called from NewProposalScope).
//
// The caller (gatedScope.NewScope) reuses the same coverageSlots across
// calls within the same proposal: applyPlans starts by truncating each
// slot to length 0 (keeping its backing array), then only allocates a
// new backing array when the existing capacity is insufficient. After
// the first proposal-warmup, steady-state runs reuse the same arrays
// without allocation.
//
// Two passes:
//  1. validate each selected plan's attr_code (and range-check coverage
//     bits) and count selected plans per slot. The per-slot counter
//     sits on the stack ([len(cacheAttrKinds)]int).
//  2. grow each touched slot's backing array only when needed (cap < n),
//     then append. Sizing up front via make([]U128, 0, n) eliminates
//     runtime.growslice from the hot path.
//
// Returns *ErrInvalidExecutionPlan when a coverage bit indexes past the
// plans slice or when a plan declares an attr_code the FSM does not
// handle. On error, the partial reset is harmless: the caller does not
// use the scope and the next NewScope call truncates again.
func applyPlans(coverage *coverageSlots, plans []*raftcmdpb.AttributeCoverage, coverageBits []byte) *domain.ErrInvalidExecutionPlan {
	for i := range coverage {
		coverage[i] = coverage[i][:0]
	}

	if len(coverageBits) == 0 {
		return nil
	}

	var counts [len(cacheAttrKinds)]int

	if err := validateCoverageBits(plans, coverageBits, func(bit int) {
		counts[coverageSlotIndex[byte(plans[bit].GetAttrCode())]]++
	}); err != nil {
		return err
	}

	for i, n := range counts {
		if cap(coverage[i]) < n {
			coverage[i] = make([]attributes.U128, 0, n)
		}
	}

	for byteIdx, b := range coverageBits {
		for b != 0 {
			bit := byteIdx*8 + lsbIndex(b)
			b &= b - 1

			plan := plans[bit]
			slot := coverageSlotIndex[byte(plan.GetAttrCode())]
			coverage[slot] = append(coverage[slot], attributes.U128FromBytes(plan.GetId().GetId()))
		}
	}

	return nil
}

// applyAllPlans admits every plan in the slice (proposal-wide scope).
// Used by NewProposalScope for cross-order checks such as
// ValidateTransientVolumes that must reach into any ledger the proposal
// may have touched.
//
// Returns *ErrInvalidExecutionPlan when a plan declares an attr_code
// the FSM does not handle.
func applyAllPlans(coverage *coverageSlots, plans []*raftcmdpb.AttributeCoverage) *domain.ErrInvalidExecutionPlan {
	for i := range coverage {
		coverage[i] = coverage[i][:0]
	}

	var counts [len(cacheAttrKinds)]int

	for i, plan := range plans {
		if err := validatePlan(plan, i); err != nil {
			return err
		}

		counts[coverageSlotIndex[byte(plan.GetAttrCode())]]++
	}

	for i, n := range counts {
		if cap(coverage[i]) < n {
			coverage[i] = make([]attributes.U128, 0, n)
		}
	}

	for _, plan := range plans {
		slot := coverageSlotIndex[byte(plan.GetAttrCode())]
		coverage[slot] = append(coverage[slot], attributes.U128FromBytes(plan.GetId().GetId()))
	}

	return nil
}

// NewScopeFactory binds the engine and the proposal's ExecutionPlan.
// It returns a gatedScope that doubles as its own factory: NewScope
// reconfigures coverage in-place and returns the same pointer.
//
// Callers within applyProposal — applyTechnicalUpdates, ProcessOrders,
// then ValidateTransientVolumes — invoke NewScope sequentially, finishing
// with the returned scope before requesting another one. A single
// gatedScope per proposal therefore suffices; the previous coverage is
// overwritten on each call, and the slots' backing arrays live across
// calls (no makeslice in steady state).
func NewScopeFactory(
	inner *WriteSet,
	plan *raftcmdpb.ExecutionPlan,
	logger logging.Logger,
	missCounter metric.Int64Counter,
	raftIndex uint64,
) processing.ScopeFactory {
	var plans []*raftcmdpb.AttributeCoverage
	if plan != nil {
		plans = plan.GetAttributes()
	}

	g := &gatedScope{
		WriteSet:  inner,
		plans:     plans,
		logger:    logger,
		miss:      missCounter,
		raftIndex: raftIndex,
	}

	g.gatedLedgers = newGatedAccessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader](inner.ledgers, g, dal.SubAttrLedger)
	g.gatedBoundaries = newGatedAccessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader](inner.boundaries, g, dal.SubAttrBoundary)
	g.gatedVolumes = newGatedAccessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader](inner.volumes, g, dal.SubAttrVolume)
	g.gatedAccountMetadata = newGatedAccessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader](inner.accountMetadata, g, dal.SubAttrMetadata)
	g.gatedLedgerMetadata = newGatedAccessor[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader](inner.ledgerMetadata, g, dal.SubAttrLedgerMetadata)
	g.gatedTransactionReferences = newGatedAccessor[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader](inner.transactionReferences, g, dal.SubAttrReference)
	g.gatedTransactionStates = newGatedAccessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader](inner.transactionStates, g, dal.SubAttrTransaction)
	g.gatedPreparedQueries = newGatedAccessor[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader](inner.preparedQueries, g, dal.SubAttrPreparedQuery)
	g.gatedIndexes = newGatedAccessor[domain.IndexKey, *commonpb.Index, commonpb.IndexReader](inner.indexes, g, dal.SubAttrIndex)

	return g
}

// NewScope reconfigures the scope's coverage for the given bits and
// returns itself. The previous configuration is overwritten in place;
// callers must finish using the scope before requesting another one
// (the sequential FSM apply path guarantees this).
//
// Empty bits admit no plan — every CheckCoverage call will miss.
// Callers that need a proposal-wide scope must use NewProposalScope:
// distinct entry point so an order with no declared needs does not
// silently inherit coverage from other orders in the same proposal.
//
// Returns *domain.ErrInvalidExecutionPlan when the bits/plan combination
// is structurally inconsistent — the FSM rejects the proposal before any
// cache mutation lands so state stays coherent for the next attempt.
func (g *gatedScope) NewScope(coverageBits []byte) (processing.Scope, error) {
	if err := applyPlans(&g.coverage, g.plans, coverageBits); err != nil {
		return nil, err
	}

	return g, nil
}

// NewProposalScope reconfigures the scope to admit every AttributeCoverage
// the proposal declared. Used by ValidateTransientVolumes and other
// cross-order checks that must reach into any ledger the proposal may
// have touched.
func (g *gatedScope) NewProposalScope() (processing.Scope, error) {
	if err := applyAllPlans(&g.coverage, g.plans); err != nil {
		return nil, err
	}

	return g, nil
}

// CheckCoverage exposes the gate for paths that bypass the engine's
// overlay reads. ValidateTransientVolumes uses it before a direct
// Derived.Volumes.Parent().GetKey to keep the coverage invariant.
//
// The key's canonical bytes are built into g.scratch — a single
// per-scope buffer reused across every gated call on the FSM apply
// path (single-threaded under fsm.mu). The bytes are consumed
// synchronously by attributes.MakeKey (xxh3 hash) and, on miss, by
// hex.EncodeToString (which copies into a fresh string) — no consumer
// retains a reference past the return, so overwriting the scratch on
// the next call is safe.
func (g *gatedScope) CheckCoverage(kind byte, key processing.CoverageKey) error {
	g.scratch = key.AppendBytes(g.scratch[:0])

	id, _ := attributes.MakeKey(g.scratch)
	slot := coverageSlotIndex[kind]
	if slot < 0 {
		return g.coverageMiss(kind, g.scratch, id)
	}

	if slices.Contains(g.coverage[slot], id) {
		return nil
	}

	return g.coverageMiss(kind, g.scratch, id)
}

func (g *gatedScope) coverageMiss(kind byte, canonical []byte, id attributes.U128) *ErrCoverageMiss {
	kindName := kindLabel(kind)

	details := map[string]any{
		"kind":          kindName,
		"canonical_hex": hex.EncodeToString(canonical),
		"id_hex":        id.Hex(),
		"raft_index":    g.raftIndex,
	}

	g.logger.WithFields(details).Errorf("preload coverage miss: read of undeclared key")

	if g.miss != nil {
		g.miss.Add(context.Background(), 1, metric.WithAttributes(attribute.String("kind", kindName)))
	}

	return &ErrCoverageMiss{
		Attribute:    kindName,
		CanonicalHex: hex.EncodeToString(canonical),
		IDHex:        id.Hex(),
		RaftIndex:    g.raftIndex,
	}
}

// kindLabel maps a sub-attribute code to the OTel/log kind label.
func kindLabel(kind byte) string {
	switch kind {
	case dal.SubAttrVolume:
		return "volumes"
	case dal.SubAttrMetadata:
		return "account_metadata"
	case dal.SubAttrReference:
		return "references"
	case dal.SubAttrLedger:
		return "ledgers"
	case dal.SubAttrBoundary:
		return "boundaries"
	case dal.SubAttrSinkConfig:
		return "sink_configs"
	case dal.SubAttrNumscriptVersion:
		return "numscript_versions"
	case dal.SubAttrTransaction:
		return "transactions"
	case dal.SubAttrNumscriptContent:
		return "numscript_contents"
	case dal.SubAttrPreparedQuery:
		return "prepared_queries"
	case dal.SubAttrLedgerMetadata:
		return "ledger_metadata"
	case dal.SubAttrIndex:
		return "indexes"
	default:
		return fmt.Sprintf("unknown(0x%02x)", kind)
	}
}

// cacheAttrKinds enumerates the sub-attribute codes that own a coverage
// slot. coverageSlotIndex maps each one to its index inside the dense
// coverageSlots array; any other sub-attribute code resolves to -1 and
// is treated as an uncovered read by CheckCoverage.
var cacheAttrKinds = [...]byte{
	dal.SubAttrVolume,
	dal.SubAttrMetadata,
	dal.SubAttrReference,
	dal.SubAttrLedger,
	dal.SubAttrBoundary,
	dal.SubAttrSinkConfig,
	dal.SubAttrNumscriptVersion,
	dal.SubAttrTransaction,
	dal.SubAttrNumscriptContent,
	dal.SubAttrPreparedQuery,
	dal.SubAttrLedgerMetadata,
	dal.SubAttrIndex,
}

// coverageSlots holds one slice of declared U128 ids per gated
// sub-attribute kind. A nil slot means the proposal declared no plan
// for that kind, which CheckCoverage naturally treats as a miss (the
// linear scan over a zero-length slice finds nothing).
//
// A slice scales to the actual coverage size and avoids the Swiss-table
// header / dir / first-group allocations a map[U128]struct{} would pay
// for every used slot. For ≤ 10 entries — the common case — CheckCoverage's
// linear scan beats a map lookup (no hash compute, fully cache-resident).
type coverageSlots [len(cacheAttrKinds)][]attributes.U128

// coverageSlotIndex is the O(1) lookup table from sub-attribute code to
// dense slot index, derived from cacheAttrKinds. -1 means the kind is
// not gated (applyPlans rejects the plan; CheckCoverage treats the read
// as a miss).
var coverageSlotIndex = func() [256]int8 {
	var lookup [256]int8
	for i := range lookup {
		lookup[i] = -1
	}

	for i, k := range cacheAttrKinds {
		lookup[k] = int8(i)
	}

	return lookup
}()

func lsbIndex(b byte) int {
	for i := range 8 {
		if b&(1<<i) != 0 {
			return i
		}
	}

	return 0
}

// --- Gated accessor overrides ---
//
// gatedScope shadows the embedded *WriteSet's accessor-returning methods
// to surface the per-kind gatedAccessor instead of the bare rawAccessor.
// Hetero discrete methods (GetSinkConfig, GetNumscriptLatestVersion,
// NumscriptVersionExists, ResolveNumscriptContent) still need a manual
// CheckCoverage prelude because their signatures do not fit the Accessor
// contract.

func (g *gatedScope) Ledgers() processing.Accessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader] {
	return g.gatedLedgers
}

func (g *gatedScope) Boundaries() processing.Accessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader] {
	return g.gatedBoundaries
}

func (g *gatedScope) Volumes() processing.Accessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader] {
	return g.gatedVolumes
}

func (g *gatedScope) AccountMetadata() processing.Accessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader] {
	return g.gatedAccountMetadata
}

func (g *gatedScope) LedgerMetadata() processing.Accessor[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader] {
	return g.gatedLedgerMetadata
}

func (g *gatedScope) TransactionReferences() processing.Accessor[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader] {
	return g.gatedTransactionReferences
}

func (g *gatedScope) TransactionStates() processing.Accessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader] {
	return g.gatedTransactionStates
}

func (g *gatedScope) PreparedQueries() processing.Accessor[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader] {
	return g.gatedPreparedQueries
}

func (g *gatedScope) Indexes() processing.Accessor[domain.IndexKey, *commonpb.Index, commonpb.IndexReader] {
	return g.gatedIndexes
}

// --- Gated discrete reads (signatures that do not fit the Accessor trio) ---

func (g *gatedScope) GetSinkConfig(name string) (commonpb.SinkConfigReader, error) {
	if err := g.CheckCoverage(dal.SubAttrSinkConfig, domain.SinkConfigKey{Name: name}); err != nil {
		return nil, err
	}

	return g.WriteSet.GetSinkConfig(name)
}

func (g *gatedScope) GetNumscriptLatestVersion(ledgerName string, name string) (string, error) {
	if err := g.CheckCoverage(dal.SubAttrNumscriptVersion, domain.NumscriptVersionKey{LedgerName: ledgerName, Name: name}); err != nil {
		return "", err
	}

	return g.WriteSet.GetNumscriptLatestVersion(ledgerName, name)
}

func (g *gatedScope) ResolveNumscriptContent(ledgerName string, name string, version string) (commonpb.NumscriptInfoReader, error) {
	if err := g.CheckCoverage(dal.SubAttrNumscriptContent, domain.NumscriptEntryKey{LedgerName: ledgerName, Name: name, Version: version}); err != nil {
		return nil, err
	}

	return g.WriteSet.ResolveNumscriptContent(ledgerName, name, version)
}

func (g *gatedScope) NumscriptVersionExists(ledgerName string, name, version string) (bool, error) {
	if err := g.CheckCoverage(dal.SubAttrNumscriptContent, domain.NumscriptEntryKey{LedgerName: ledgerName, Name: name, Version: version}); err != nil {
		return false, err
	}

	return g.WriteSet.NumscriptVersionExists(ledgerName, name, version)
}
