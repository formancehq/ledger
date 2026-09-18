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
	revertTargets map[domain.TransactionKey]revertTargetObservation
}

// revertTargetState is what admission knows about a revert target. The three
// states must stay distinguishable, because each one means something different
// downstream:
//
//   - revertTargetUnobserved — no revert order referenced this target, so there
//     is nothing to bind. bindRevertTargetDigest refuses such an order rather
//     than stamping a digest for a lookup that never happened.
//   - revertTargetAbsent — admission looked and the transaction was not in the
//     local store (missing, or committed but not yet applied here). That is an
//     observation in its own right: it is bound into revert_target_digest so
//     apply rejects a stale view before reading an undeclared volume.
//   - revertTargetPresent — admission read the target and declares volume
//     coverage from its postings, exactly as stored.
//
// Present carries the stored posting set verbatim, including an empty one. That
// is not folded into absent on purpose: the two must stay distinguishable in the
// digest, and a stored transaction with no postings is a broken projection that
// processRevertTransaction rejects as ErrTransactionStateInconsistent before the
// observation check ever runs (invariant #7). Create refuses an empty
// transaction, so the case does not arise; reporting it as absent would classify
// a broken projection as a stale or same-batch mismatch instead.
type revertTargetState uint8

const (
	revertTargetUnobserved revertTargetState = iota
	revertTargetAbsent
	revertTargetPresent
)

// revertTargetObservation is what admission saw when it looked up a revert
// target in the local store. Its zero value is the unobserved state, so a miss
// on the overlay map reads correctly without a second return value.
type revertTargetObservation struct {
	state    revertTargetState
	postings []*commonpb.Posting
}

// absentRevertTarget records that admission looked and the transaction was not
// in the local store. Distinct from never having looked — see revertTargetState.
func absentRevertTarget() revertTargetObservation {
	return revertTargetObservation{state: revertTargetAbsent}
}

// presentRevertTarget records the postings admission read for the target.
func presentRevertTarget(postings []*commonpb.Posting) revertTargetObservation {
	return revertTargetObservation{state: revertTargetPresent, postings: postings}
}

// observed reports whether a lookup happened at all, as opposed to finding
// something.
func (o revertTargetObservation) observed() bool { return o.state != revertTargetUnobserved }

// found reports whether the target was in the local store.
func (o revertTargetObservation) found() bool { return o.state == revertTargetPresent }

// bindRevertTargetDigest stamps a revert order's technical sub-message with a
// digest of what admission observed of its target.
//
// It is a no-op for every other order type, so a non-revert order keeps an empty
// digest and the FSM skips the check. OrderTechnical is excluded wholesale from
// the idempotency and business-intent hashes, so writing here cannot change the
// order's logical identity.
//
// A revert with no recorded observation is rejected here rather than shipped
// with an empty digest. The FSM refuses that order anyway, but only after a Raft
// round-trip, and assert.Unreachable is a no-op outside Antithesis — so the
// assertion alone would let a future producer that skips recordRevertTarget
// reach consensus before failing. Same reason class either way, so the caller
// sees no difference beyond the earlier rejection.
func bindRevertTargetDigest(
	order *raftcmdpb.Order,
	ledgerName string,
	applyOrder *raftcmdpb.LedgerApplyOrder,
	overlay *bulkOverlay,
) error {
	revert, ok := applyOrder.GetData().(*raftcmdpb.LedgerApplyOrder_RevertTransaction)
	if !ok {
		return nil
	}

	observation := overlay.revertTarget(ledgerName, revert.RevertTransaction)
	if !observation.observed() {
		// Unreachable by construction: convertApplyRequest records an
		// observation for every revert it builds. Leaving the digest empty here
		// would silently disable the apply-time check, so say so loudly rather
		// than binding a digest for a lookup that never happened.
		assert.Unreachable("revert order reached digest binding without a recorded target observation", map[string]any{
			"ledger":        ledgerName,
			"transactionId": revert.RevertTransaction.GetTransactionId(),
		})

		return &domain.ErrInvalidExecutionPlan{
			Reason_: "revert order built without a recorded target observation",
		}
	}

	orderTechnical(order).RevertTargetDigest = domain.RevertTargetDigest(observation.postings, observation.found())

	return nil
}

func newBulkOverlay() *bulkOverlay {
	return &bulkOverlay{
		numscriptEntries: newOverlay[numscriptEntryKey, string](),
		numscriptLatest:  newOverlay[numscriptNameKey, string](),
		sinks:            newOverlay[string, *commonpb.SinkConfig](),
		revertTargets:    make(map[domain.TransactionKey]revertTargetObservation),
	}
}

// recordRevertTarget stores what admission observed of a revert target so later
// passes read it without re-fetching.
func (o *bulkOverlay) recordRevertTarget(key domain.TransactionKey, observation revertTargetObservation) {
	o.revertTargets[key] = observation
}

// revertTarget returns what admission observed of a revert order's target. A
// target no revert order referenced reads back as the zero value, which is the
// unobserved state — see revertTargetState for why that is not the same as an
// absent one.
//
// This is the only accessor, and it is keyed off the order rather than a
// caller-built key, so the three passes that consult the observation — coverage
// extraction, intra-bulk effect folding, and digest binding — cannot drift from
// the key convertApplyRequest recorded under.
func (o *bulkOverlay) revertTarget(
	ledgerName string,
	revert *raftcmdpb.RevertTransactionOrder,
) revertTargetObservation {
	return o.revertTargets[revertTargetKey(ledgerName, revert)]
}

// revertTargetKey is the overlay key for a revert order's target.
func revertTargetKey(ledgerName string, revert *raftcmdpb.RevertTransactionOrder) domain.TransactionKey {
	return domain.TransactionKey{LedgerName: ledgerName, ID: revert.GetTransactionId()}
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
