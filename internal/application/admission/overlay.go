package admission

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// numscriptEntryKey identifies a specific numscript version.
type numscriptEntryKey struct {
	Ledger  string
	Name    string
	Version string
}

// numscriptNameKey identifies a numscript by ledger and name (without version).
type numscriptNameKey struct {
	Ledger string
	Name   string
}

// overlay is a generic write-through overlay for intra-bulk data resolution.
// It tracks puts and deletes within a bulk so that later requests can see
// data written by earlier requests in the same bulk, before Pebble commit.
type overlay[K comparable, V any] struct {
	entries map[K]V
	deleted map[K]bool
}

func newOverlay[K comparable, V any]() *overlay[K, V] {
	return &overlay[K, V]{
		entries: make(map[K]V),
		deleted: make(map[K]bool),
	}
}

// Put stores a value in the overlay and clears any prior delete marker for this key.
func (o *overlay[K, V]) Put(key K, value V) {
	delete(o.deleted, key)
	o.entries[key] = value
}

// Delete marks a key as deleted and removes it from the entries.
// Callers should check IsDeleted before falling back to the external store.
func (o *overlay[K, V]) Delete(key K) {
	o.deleted[key] = true
	delete(o.entries, key)
}

// Get returns the value and true if the key exists in the overlay.
func (o *overlay[K, V]) Get(key K) (V, bool) {
	v, ok := o.entries[key]

	return v, ok
}

// IsDeleted returns true if the key was explicitly deleted in this bulk.
func (o *overlay[K, V]) IsDeleted(key K) bool {
	return o.deleted[key]
}

// Range calls fn for every live entry in the overlay.
// Iteration order is non-deterministic.
func (o *overlay[K, V]) Range(fn func(K, V) bool) {
	for k, v := range o.entries {
		if !fn(k, v) {
			return
		}
	}
}

// bulkOverlay groups all typed overlays for a single bulk request.
// Add new fields here when future data types need intra-bulk resolution.
type bulkOverlay struct {
	numscriptEntries *overlay[numscriptEntryKey, string]
	numscriptLatest  *overlay[numscriptNameKey, string]
	sinks            *overlay[string, *commonpb.SinkConfig]
	// What admission observed of each revert target, resolved once at
	// order-build time from the transaction attribute and reused by the preload
	// and intra-bulk effect passes. The postings stay off the wire order: the
	// FSM re-derives them from the coverage-gated TransactionState, and only
	// caller intent is bound into the audit chain. The observation itself is
	// bound, as a digest, so apply can tell a stale admission view from an
	// under-declaration.
	revertOriginalPostings map[domain.TransactionKey]revertTargetObservation
}

// revertTargetObservation is what admission saw when it looked up a revert
// target in the local store. found=false records that it looked and found
// nothing, which is different from never having looked.
type revertTargetObservation struct {
	postings []*commonpb.Posting
	found    bool
}

// bindRevertTargetDigest stamps a revert order's technical sub-message with a
// digest of what admission observed of its target.
//
// It is a no-op for every other order type, so a non-revert order keeps an empty
// digest and the FSM skips the check. OrderTechnical is excluded wholesale from
// the idempotency and business-intent hashes, so writing here cannot change the
// order's logical identity.
func bindRevertTargetDigest(
	order *raftcmdpb.Order,
	ledgerName string,
	applyOrder *raftcmdpb.LedgerApplyOrder,
	overlay *bulkOverlay,
) {
	revert, ok := applyOrder.GetData().(*raftcmdpb.LedgerApplyOrder_RevertTransaction)
	if !ok {
		return
	}

	observation, recorded := overlay.revertOriginalPostingsFor(domain.TransactionKey{
		LedgerName: ledgerName,
		ID:         revert.RevertTransaction.GetTransactionId(),
	})
	if !recorded {
		// Unreachable by construction: convertApplyRequest records an
		// observation for every revert it builds. Leaving the digest empty here
		// would silently disable the apply-time check, so say so loudly rather
		// than binding a digest for a lookup that never happened.
		assert.Unreachable("revert order reached digest binding without a recorded target observation", map[string]any{
			"ledger":        ledgerName,
			"transactionId": revert.RevertTransaction.GetTransactionId(),
		})

		return
	}

	if order.GetTechnical() == nil {
		order.Technical = &raftcmdpb.OrderTechnical{}
	}

	order.Technical.RevertTargetDigest = domain.RevertTargetDigest(observation.postings, observation.found)
}

func newBulkOverlay() *bulkOverlay {
	return &bulkOverlay{
		numscriptEntries:       newOverlay[numscriptEntryKey, string](),
		numscriptLatest:        newOverlay[numscriptNameKey, string](),
		sinks:                  newOverlay[string, *commonpb.SinkConfig](),
		revertOriginalPostings: make(map[domain.TransactionKey]revertTargetObservation),
	}
}

// recordRevertOriginalPostings stores what admission observed of a revert
// target so later passes read it without re-fetching. found is false when the
// transaction was not in the local store — an observation in its own right, not
// an absence of one.
func (o *bulkOverlay) recordRevertOriginalPostings(
	key domain.TransactionKey,
	postings []*commonpb.Posting,
	found bool,
) {
	o.revertOriginalPostings[key] = revertTargetObservation{postings: postings, found: found}
}

// revertOriginalPostingsFor returns the postings recorded for a revert target
// and whether admission resolved that target at all.
//
// The two conditions must stay distinguishable. Nil postings with ok=true is an
// observation — admission looked and the transaction was not in the local store
// (a missing tx, or one committed but not yet applied here) — and it is bound
// into the order's revert_target_digest so apply can reject a stale view before
// it reads an undeclared volume. ok=false means no revert order referenced this
// key, which carries no observation at all.
func (o *bulkOverlay) revertOriginalPostingsFor(key domain.TransactionKey) (revertTargetObservation, bool) {
	observation, ok := o.revertOriginalPostings[key]

	return observation, ok
}

// recordNumscriptSave records an immutable save in the overlay and advances the
// per-name latest to the greatest semver seen so far in this bulk, mirroring the
// FSM's max-pointer maintenance.
func (o *bulkOverlay) recordNumscriptSave(ledger, name, version, content string) {
	o.numscriptEntries.Put(numscriptEntryKey{Ledger: ledger, Name: name, Version: version}, content)

	nameKey := numscriptNameKey{Ledger: ledger, Name: name}
	if cur, ok := o.numscriptLatest.Get(nameKey); ok && !greaterSemver(version, cur) {
		return
	}

	o.numscriptLatest.Put(nameKey, version)
}

// greaterSemver reports whether a is a strictly greater full semver than b.
// A non-semver b (or empty) is treated as smaller so a valid save always wins.
func greaterSemver(a, b string) bool {
	av, aerr := semver.Parse(a)
	bv, berr := semver.Parse(b)
	if aerr != nil {
		return false
	}

	if berr != nil {
		return true
	}

	return av.Compare(bv) > 0
}
