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
// Read methods that participate in a Get/Put pair are overridden to read
// from the staged map first (read-your-writes consistency within the same
// order). The remaining read methods delegate to the embedded Scope via
// interface promotion. Counters that an order may increment are tracked as
// a delta so the order observes a monotonic sequence while the increments
// are flushed to the parent on Commit only.
//
// The overlay does NOT speculate beyond the order boundary: it copies the
// parent's current state lazily (on first read of a staged-eligible key)
// so an order that never reads or writes a category pays no allocation
// cost for that category.
type orderOverlayScope struct {
	Scope

	stagedBoundaries        map[string]*raftcmdpb.LedgerBoundaries
	stagedLedgers           map[string]*commonpb.LedgerInfo
	stagedTxStates          map[domain.TransactionKey]*commonpb.TransactionState
	stagedAcctMetaPuts      map[domain.MetadataKey]*commonpb.MetadataValue
	stagedAcctMetaDeletes   map[domain.MetadataKey]struct{}
	stagedLedgerMetaPuts    map[domain.LedgerMetadataKey]*commonpb.MetadataValue
	stagedLedgerMetaDeletes map[domain.LedgerMetadataKey]struct{}
	stagedReverted          map[domain.TransactionKey]bool
	stagedIdempotency       map[domain.IdempotencyKey]*commonpb.IdempotencyKeyValue
	stagedTxRefs            map[domain.TransactionReferenceKey]*commonpb.TransactionReferenceValue
	stagedVolumes           map[domain.VolumeKey]*raftcmdpb.VolumePair

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
	return &orderOverlayScope{Scope: parent}
}

// ──────────────────────────────────────────────────────────────────────────
// Ledger
// ──────────────────────────────────────────────────────────────────────────

func (o *orderOverlayScope) GetLedger(name string) (commonpb.LedgerInfoReader, error) {
	if v, ok := o.stagedLedgers[name]; ok {
		return v.AsReader(), nil
	}

	return o.Scope.GetLedger(name)
}

func (o *orderOverlayScope) PutLedger(name string, info *commonpb.LedgerInfo) {
	if o.stagedLedgers == nil {
		o.stagedLedgers = map[string]*commonpb.LedgerInfo{}
	}

	o.stagedLedgers[name] = info
}

// ──────────────────────────────────────────────────────────────────────────
// Boundaries
// ──────────────────────────────────────────────────────────────────────────

func (o *orderOverlayScope) GetBoundaries(ledger string) (raftcmdpb.LedgerBoundariesReader, error) {
	if b, ok := o.stagedBoundaries[ledger]; ok {
		return b.AsReader(), nil
	}

	return o.Scope.GetBoundaries(ledger)
}

func (o *orderOverlayScope) PutBoundaries(ledger string, boundaries *raftcmdpb.LedgerBoundaries) {
	if o.stagedBoundaries == nil {
		o.stagedBoundaries = map[string]*raftcmdpb.LedgerBoundaries{}
	}

	o.stagedBoundaries[ledger] = boundaries
}

// ──────────────────────────────────────────────────────────────────────────
// Volumes
// ──────────────────────────────────────────────────────────────────────────

func (o *orderOverlayScope) GetVolume(key domain.VolumeKey) (raftcmdpb.VolumePairReader, error) {
	if v, ok := o.stagedVolumes[key]; ok {
		return v.AsReader(), nil
	}

	return o.Scope.GetVolume(key)
}

func (o *orderOverlayScope) PutVolume(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
	if o.stagedVolumes == nil {
		o.stagedVolumes = map[domain.VolumeKey]*raftcmdpb.VolumePair{}
	}

	o.stagedVolumes[key] = value
}

// ──────────────────────────────────────────────────────────────────────────
// Account metadata
// ──────────────────────────────────────────────────────────────────────────

func (o *orderOverlayScope) GetAccountMetadata(key domain.MetadataKey) (commonpb.MetadataValueReader, error) {
	if _, deleted := o.stagedAcctMetaDeletes[key]; deleted {
		return nil, domain.ErrNotFound
	}

	if v, ok := o.stagedAcctMetaPuts[key]; ok {
		return v.AsReader(), nil
	}

	return o.Scope.GetAccountMetadata(key)
}

func (o *orderOverlayScope) PutAccountMetadata(key domain.MetadataKey, value *commonpb.MetadataValue) {
	if o.stagedAcctMetaPuts == nil {
		o.stagedAcctMetaPuts = map[domain.MetadataKey]*commonpb.MetadataValue{}
	}

	delete(o.stagedAcctMetaDeletes, key)
	o.stagedAcctMetaPuts[key] = value
}

func (o *orderOverlayScope) DeleteAccountMetadata(key domain.MetadataKey) {
	if o.stagedAcctMetaDeletes == nil {
		o.stagedAcctMetaDeletes = map[domain.MetadataKey]struct{}{}
	}

	delete(o.stagedAcctMetaPuts, key)
	o.stagedAcctMetaDeletes[key] = struct{}{}
}

// GetAccountMetadataEntry delegates: the entry surface is read-only and
// returning a staged metadata value as an attributes.Entry would require
// fabricating a canonical encoding that the engine alone owns. Sub-
// processors that read this entry today do not also Put the same key in
// the same order, so the delegated read suffices.

// ──────────────────────────────────────────────────────────────────────────
// Ledger metadata
// ──────────────────────────────────────────────────────────────────────────

func (o *orderOverlayScope) GetLedgerMetadata(key domain.LedgerMetadataKey) (commonpb.MetadataValueReader, error) {
	if _, deleted := o.stagedLedgerMetaDeletes[key]; deleted {
		return nil, domain.ErrNotFound
	}

	if v, ok := o.stagedLedgerMetaPuts[key]; ok {
		return v.AsReader(), nil
	}

	return o.Scope.GetLedgerMetadata(key)
}

func (o *orderOverlayScope) PutLedgerMetadata(key domain.LedgerMetadataKey, value *commonpb.MetadataValue) {
	if o.stagedLedgerMetaPuts == nil {
		o.stagedLedgerMetaPuts = map[domain.LedgerMetadataKey]*commonpb.MetadataValue{}
	}

	delete(o.stagedLedgerMetaDeletes, key)
	o.stagedLedgerMetaPuts[key] = value
}

func (o *orderOverlayScope) DeleteLedgerMetadata(key domain.LedgerMetadataKey) {
	if o.stagedLedgerMetaDeletes == nil {
		o.stagedLedgerMetaDeletes = map[domain.LedgerMetadataKey]struct{}{}
	}

	delete(o.stagedLedgerMetaPuts, key)
	o.stagedLedgerMetaDeletes[key] = struct{}{}
}

// ──────────────────────────────────────────────────────────────────────────
// Reverted / Idempotency / TransactionReference / TransactionState
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

func (o *orderOverlayScope) GetIdempotencyKey(key domain.IdempotencyKey) (commonpb.IdempotencyKeyValueReader, error) {
	if v, ok := o.stagedIdempotency[key]; ok {
		return v.AsReader(), nil
	}

	return o.Scope.GetIdempotencyKey(key)
}

func (o *orderOverlayScope) PutIdempotencyKey(key domain.IdempotencyKey, value *commonpb.IdempotencyKeyValue) {
	if o.stagedIdempotency == nil {
		o.stagedIdempotency = map[domain.IdempotencyKey]*commonpb.IdempotencyKeyValue{}
	}

	o.stagedIdempotency[key] = value
}

func (o *orderOverlayScope) GetTransactionReference(key domain.TransactionReferenceKey) (commonpb.TransactionReferenceValueReader, error) {
	if v, ok := o.stagedTxRefs[key]; ok {
		return v.AsReader(), nil
	}

	return o.Scope.GetTransactionReference(key)
}

func (o *orderOverlayScope) PutTransactionReference(key domain.TransactionReferenceKey, value *commonpb.TransactionReferenceValue) {
	if o.stagedTxRefs == nil {
		o.stagedTxRefs = map[domain.TransactionReferenceKey]*commonpb.TransactionReferenceValue{}
	}

	o.stagedTxRefs[key] = value
}

func (o *orderOverlayScope) GetTransactionState(key domain.TransactionKey) (commonpb.TransactionStateReader, error) {
	if v, ok := o.stagedTxStates[key]; ok {
		return v.AsReader(), nil
	}

	return o.Scope.GetTransactionState(key)
}

func (o *orderOverlayScope) PutTransactionState(key domain.TransactionKey, state *commonpb.TransactionState) {
	if o.stagedTxStates == nil {
		o.stagedTxStates = map[domain.TransactionKey]*commonpb.TransactionState{}
	}

	o.stagedTxStates[key] = state
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
func (o *orderOverlayScope) Commit() {
	for name, info := range o.stagedLedgers {
		o.Scope.PutLedger(name, info)
	}

	for ledger, b := range o.stagedBoundaries {
		o.Scope.PutBoundaries(ledger, b)
	}

	for k, v := range o.stagedVolumes {
		o.Scope.PutVolume(k, v)
	}

	for k, v := range o.stagedAcctMetaPuts {
		o.Scope.PutAccountMetadata(k, v)
	}

	for k := range o.stagedAcctMetaDeletes {
		o.Scope.DeleteAccountMetadata(k)
	}

	for k, v := range o.stagedLedgerMetaPuts {
		o.Scope.PutLedgerMetadata(k, v)
	}

	for k := range o.stagedLedgerMetaDeletes {
		o.Scope.DeleteLedgerMetadata(k)
	}

	for k, v := range o.stagedReverted {
		o.Scope.PutReverted(k, v)
	}

	for k, v := range o.stagedIdempotency {
		o.Scope.PutIdempotencyKey(k, v)
	}

	for k, v := range o.stagedTxRefs {
		o.Scope.PutTransactionReference(k, v)
	}

	for k, v := range o.stagedTxStates {
		o.Scope.PutTransactionState(k, v)
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
}

var _ Scope = (*orderOverlayScope)(nil)
