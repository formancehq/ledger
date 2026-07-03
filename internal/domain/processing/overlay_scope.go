package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// orderOverlayScope wraps a Scope so every mutation made by a single order
// can be either committed to the parent (Commit()) or silently discarded.
// ProcessOrders allocates one per order that opts in via
// Order.skippable_reasons; on a successful order it calls Commit() and the
// staged writes flush to the parent; on an order that is converted to a
// skip the overlay is dropped without committing and the parent never sees
// any of the writes.
//
// The overlay overrides the per-kind Accessor methods (Ledgers, Boundaries,
// Volumes, AccountMetadata, LedgerMetadata, TransactionReferences,
// TransactionStates) to return a stagedAccessor that buffers writes
// locally. The remaining Scope methods delegate to the embedded parent via
// interface promotion — kinds the sub-processor cannot mutate (signing,
// chapter, numscript, query-checkpoint, sink) don't need overlay buffering
// because no skip-tolerant order writes to them today.
//
// Counter increments are buffered as deltas. Each Increment* call records
// itself in the overlay so the order sees a monotonic sequence while the
// parent only learns about the increments on Commit.
//
// Reverted is bool-valued and stays discrete (Scope still exposes it as
// dedicated Get/Put methods rather than an Accessor).
type orderOverlayScope struct {
	Scope

	ledgers           *stagedAccessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader]
	boundaries        *stagedAccessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader]
	volumes           *stagedAccessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader]
	accountMetadata   *stagedAccessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]
	ledgerMetadata    *stagedAccessor[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]
	transactionRefs   *stagedAccessor[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader]
	transactionStates *stagedAccessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]

	// Reverted is bool-valued (no Reader). Kept as a discrete map.
	stagedReverted map[domain.TransactionKey]bool

	// Counter increments are buffered as deltas. Each `Increment*` call
	// records itself in the overlay so the order sees a monotonic sequence
	// while the parent only learns about the increments on Commit.
	seqIDDelta           uint64
	ledgerIDDelta        uint32
	chapterIDDelta       uint64
	queryCheckpointDelta uint64
	baseSeqID            uint64
	baseLedgerID         uint32
	baseChapterID        uint64
	baseQueryCheckpoint  uint64
	baseCaptured         bool
}

func newOrderOverlayScope(parent Scope) *orderOverlayScope {
	return &orderOverlayScope{
		Scope: parent,
		ledgers: newStagedAccessor(parent.Ledgers(),
			func(v *commonpb.LedgerInfo) commonpb.LedgerInfoReader { return v.AsReader() }),
		boundaries: newStagedAccessor(parent.Boundaries(),
			func(v *raftcmdpb.LedgerBoundaries) raftcmdpb.LedgerBoundariesReader { return v.AsReader() }),
		volumes: newStagedAccessor(parent.Volumes(),
			func(v *raftcmdpb.VolumePair) raftcmdpb.VolumePairReader { return v.AsReader() }),
		accountMetadata: newStagedAccessor(parent.AccountMetadata(),
			func(v *commonpb.MetadataValue) commonpb.MetadataValueReader { return v.AsReader() }),
		ledgerMetadata: newStagedAccessor(parent.LedgerMetadata(),
			func(v *commonpb.MetadataValue) commonpb.MetadataValueReader { return v.AsReader() }),
		transactionRefs: newStagedAccessor(parent.TransactionReferences(),
			func(v *commonpb.TransactionReferenceValue) commonpb.TransactionReferenceValueReader {
				return v.AsReader()
			}),
		transactionStates: newStagedAccessor(parent.TransactionStates(),
			func(v *commonpb.TransactionState) commonpb.TransactionStateReader { return v.AsReader() }),
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Accessor overrides
// ──────────────────────────────────────────────────────────────────────────

func (o *orderOverlayScope) Ledgers() Accessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader] {
	return o.ledgers
}

func (o *orderOverlayScope) Boundaries() Accessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader] {
	return o.boundaries
}

func (o *orderOverlayScope) Volumes() Accessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader] {
	return o.volumes
}

func (o *orderOverlayScope) AccountMetadata() Accessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader] {
	return o.accountMetadata
}

func (o *orderOverlayScope) LedgerMetadata() Accessor[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader] {
	return o.ledgerMetadata
}

func (o *orderOverlayScope) TransactionReferences() Accessor[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader] {
	return o.transactionRefs
}

func (o *orderOverlayScope) TransactionStates() Accessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader] {
	return o.transactionStates
}

// ──────────────────────────────────────────────────────────────────────────
// Reverted (bool-valued, no Reader — kept discrete on the Scope interface)
// ──────────────────────────────────────────────────────────────────────────

func (o *orderOverlayScope) GetReverted(key domain.TransactionKey) (bool, error) {
	if v, ok := o.stagedReverted[key]; ok {
		return v, nil
	}

	return o.Scope.GetReverted(key)
}

func (o *orderOverlayScope) PutReverted(key domain.TransactionKey, reverted bool) {
	if o.stagedReverted == nil {
		o.stagedReverted = map[domain.TransactionKey]bool{}
	}

	o.stagedReverted[key] = reverted
}

// ──────────────────────────────────────────────────────────────────────────
// Counters
// ──────────────────────────────────────────────────────────────────────────

func (o *orderOverlayScope) GetNextSequenceID() uint64 {
	o.captureBaseCounters()

	return o.baseSeqID + o.seqIDDelta
}

func (o *orderOverlayScope) IncrementNextSequenceID() uint64 {
	o.captureBaseCounters()
	next := o.baseSeqID + o.seqIDDelta
	o.seqIDDelta++

	return next
}

func (o *orderOverlayScope) GetNextLedgerID() uint32 {
	o.captureBaseCounters()

	return o.baseLedgerID + o.ledgerIDDelta
}

func (o *orderOverlayScope) IncrementNextLedgerID() uint32 {
	o.captureBaseCounters()
	next := o.baseLedgerID + o.ledgerIDDelta
	o.ledgerIDDelta++

	return next
}

func (o *orderOverlayScope) GetNextChapterID() uint64 {
	o.captureBaseCounters()

	return o.baseChapterID + o.chapterIDDelta
}

func (o *orderOverlayScope) IncrementNextChapterID() uint64 {
	o.captureBaseCounters()
	next := o.baseChapterID + o.chapterIDDelta
	o.chapterIDDelta++

	return next
}

func (o *orderOverlayScope) GetNextQueryCheckpointID() uint64 {
	o.captureBaseCounters()

	return o.baseQueryCheckpoint + o.queryCheckpointDelta
}

func (o *orderOverlayScope) IncrementNextQueryCheckpointID() uint64 {
	o.captureBaseCounters()
	next := o.baseQueryCheckpoint + o.queryCheckpointDelta
	o.queryCheckpointDelta++

	return next
}

func (o *orderOverlayScope) captureBaseCounters() {
	if o.baseCaptured {
		return
	}

	o.baseSeqID = o.Scope.GetNextSequenceID()
	o.baseLedgerID = o.Scope.GetNextLedgerID()
	o.baseChapterID = o.Scope.GetNextChapterID()
	o.baseQueryCheckpoint = o.Scope.GetNextQueryCheckpointID()
	o.baseCaptured = true
}

// ──────────────────────────────────────────────────────────────────────────
// Commit
// ──────────────────────────────────────────────────────────────────────────

// Commit flushes every staged write and counter increment to the parent
// Scope. Idempotent: calling it twice would double-apply counter deltas,
// so the orderOverlayScope is expected to be used at most once between
// allocation and Commit.
//
// If Commit is never called (because ProcessOrders decided to convert the
// order to a skip), the overlay is dropped by going out of scope and the
// parent state is never touched — the order is effectively rolled back.
//
// Returns the first flush error encountered — a per-kind Delete may
// surface a coverage-miss (invariant #6) and the caller (ProcessOrders)
// must bubble that up rather than silently drop staged tombstones.
func (o *orderOverlayScope) Commit() error {
	for _, flush := range []func() error{
		o.ledgers.flush,
		o.boundaries.flush,
		o.volumes.flush,
		o.accountMetadata.flush,
		o.ledgerMetadata.flush,
		o.transactionRefs.flush,
		o.transactionStates.flush,
	} {
		if err := flush(); err != nil {
			return err
		}
	}

	for k, v := range o.stagedReverted {
		o.Scope.PutReverted(k, v)
	}

	for range o.seqIDDelta {
		o.Scope.IncrementNextSequenceID()
	}

	for range o.ledgerIDDelta {
		o.Scope.IncrementNextLedgerID()
	}

	for range o.chapterIDDelta {
		o.Scope.IncrementNextChapterID()
	}

	for range o.queryCheckpointDelta {
		o.Scope.IncrementNextQueryCheckpointID()
	}

	return nil
}

var _ Scope = (*orderOverlayScope)(nil)

// stagedAccessor wraps a parent Accessor with per-order buffering: Put and
// Delete go into a local staged map / tombstone set; Get prefers the
// staged value (or surfaces ErrNotFound for a tombstone) before falling
// back to the parent. flush() replays the buffer onto the parent in a
// single pass — called by orderOverlayScope.Commit().
type stagedAccessor[K AccessorKey, V any, R any] struct {
	parent   Accessor[K, V, R]
	staged   map[K]V
	deletes  map[K]struct{}
	asReader func(V) R
}

func newStagedAccessor[K AccessorKey, V any, R any](
	parent Accessor[K, V, R],
	asReader func(V) R,
) *stagedAccessor[K, V, R] {
	return &stagedAccessor[K, V, R]{
		parent:   parent,
		asReader: asReader,
	}
}

func (a *stagedAccessor[K, V, R]) Get(key K) (R, error) {
	if _, deleted := a.deletes[key]; deleted {
		var zero R

		return zero, domain.ErrNotFound
	}

	if v, ok := a.staged[key]; ok {
		return a.asReader(v), nil
	}

	return a.parent.Get(key)
}

func (a *stagedAccessor[K, V, R]) Put(key K, value V) {
	if a.staged == nil {
		a.staged = make(map[K]V)
	}

	delete(a.deletes, key)
	a.staged[key] = value
}

func (a *stagedAccessor[K, V, R]) Delete(key K) error {
	if a.deletes == nil {
		a.deletes = make(map[K]struct{})
	}

	delete(a.staged, key)
	a.deletes[key] = struct{}{}

	return nil
}

// flush replays every staged write and tombstone onto the parent accessor.
// Idempotent in the sense that calling it twice produces the same parent
// state — but the caller is expected to invoke it at most once per
// orderOverlayScope lifecycle. Maps iterate in random order; the parent
// implementations do not depend on per-key order, so this is safe.
//
// Returns the first Delete error encountered — a coverage miss (invariant
// #6) at flush time is a real failure to surface, not a silent swallow.
func (a *stagedAccessor[K, V, R]) flush() error {
	for k, v := range a.staged {
		a.parent.Put(k, v)
	}

	for k := range a.deletes {
		if err := a.parent.Delete(k); err != nil {
			return err
		}
	}

	return nil
}
