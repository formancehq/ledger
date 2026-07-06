package state

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
)

// readable is the constraint every cache-attribute value type must
// satisfy to back a rawAccessor. `comparable` so the typed-nil sentinel
// (MirrorPreload's bloom-confirmed-absent marker) can be detected via
// `v == zeroV` — wrapping in `any(v)` keeps the dynamic type and never
// compares nil. AsReader returns the per-kind Reader view; the generated
// receiver is nil-safe so a hit Get can call it unconditionally.
type readable[R any] interface {
	comparable
	AsReader() R
}

// mutator is a per-Reader constraint used by rawAccessor to obtain a
// mutable clone when Mutate falls through to the parent store. Every
// generated proto reader satisfies it (Mutate() *ProtoT), so the
// instantiation is a no-op at every call site.
type mutator[V any] interface {
	Mutate() V
}

// accessorKey is the combined constraint for the per-attribute key K:
// comparable + AppendBytes (so attributes.DerivedKeyStore can serialize
// the key into Pebble form) + Bytes() (so the gate can hash the
// canonical lookup form). Every domain key type (LedgerKey, VolumeKey,
// MetadataKey, …) satisfies it.
type accessorKey interface {
	attributes.Key
	Bytes() []byte
}

// rawAccessor wraps a generic attributes.DerivedKeyStore and exposes it
// as a processing.Accessor with no coverage gate. Used by the bare
// *WriteSet for recovery, technical updates, and tests.
//
// Get normalises every miss to (zero R, domain.ErrNotFound) — matching
// the documented Scope contract — and surfaces hits as (R, nil).
// "Miss" covers two sources: a tombstone / fall-through that the
// DerivedKeyStore already reports as ErrNotFound, AND a present cache
// entry holding a typed-nil value (MirrorPreload writes this when
// bloom-confirms-absent; see cache_snapshotter.go protoSnapshotSlot
// MirrorPreload). Without the second normalisation, callers that read
// strictly via `if err != nil` would dereference a nil Reader and panic.
type rawAccessor[K accessorKey, V readable[R], R mutator[V]] struct {
	store *attributes.DerivedKeyStore[K, V]
}

func newRawAccessor[K accessorKey, V readable[R], R mutator[V]](store *attributes.DerivedKeyStore[K, V]) *rawAccessor[K, V, R] {
	return &rawAccessor[K, V, R]{store: store}
}

func (a *rawAccessor[K, V, R]) Get(key K) (R, error) {
	v, err := a.store.Get(key)
	if err != nil {
		var zero R

		return zero, err
	}

	// Typed-nil entry: MirrorPreload's bloom-confirmed-absent signal
	// (see protoSnapshotSlot.MirrorPreload). The KeyStore returns
	// (nil V, nil err), not ErrNotFound — normalise to the documented
	// miss form so callers can use the uniform errors.Is(err, ErrNotFound)
	// pattern. Compare against V's zero value (not `any(v) == nil`)
	// because wrapping a typed-nil pointer in an interface yields a
	// non-nil interface — the dynamic type descriptor masks the nil.
	var zeroV V
	if v == zeroV {
		var zero R

		return zero, domain.ErrNotFound
	}

	return v.AsReader(), nil
}

func (a *rawAccessor[K, V, R]) Put(key K, value V) {
	a.store.Put(key, value)
}

func (a *rawAccessor[K, V, R]) Delete(key K) error {
	a.store.Delete(key)

	return nil
}

// Mutate returns a mutable V for key. Overlay-owned values are returned
// directly (no clone); parent-only values are cloned once via R.Mutate()
// and stored back in the overlay. See processing.Accessor.Mutate for the
// full contract.
func (a *rawAccessor[K, V, R]) Mutate(key K) (V, error) {
	if v, ok := a.store.GetOwned(key); ok {
		var zeroV V
		if v == zeroV {
			// Typed-nil overlay entry (MirrorPreload bloom-confirmed-absent).
			// Fall through to the Get-based clone path so the caller sees
			// the same ErrNotFound it would from a plain Get.
			return a.getAndClone(key)
		}

		return v, nil
	}

	return a.getAndClone(key)
}

// getAndClone is the parent-fallback path for Mutate: Get the parent's
// reader (or bail out on error), CloneVT to obtain a mutable value, and
// Put it into the overlay so subsequent Mutate/Get calls observe the
// same pointer.
func (a *rawAccessor[K, V, R]) getAndClone(key K) (V, error) {
	r, err := a.Get(key)
	if err != nil {
		var zero V

		return zero, err
	}

	v := r.Mutate()
	a.store.Put(key, v)

	return v, nil
}

// gatedAccessor decorates an inner processing.Accessor with the per-scope
// coverage gate. Get and Delete both check CheckCoverage(sub, key.Bytes())
// before delegating — every FSM hot-path read AND deletion is bound to
// admission's declared preload set (invariants #6, #9). Put is not gated
// because in-batch writes to keys admission did not preload are still
// covered by the batch's own mutation overlay (Derived) and never race
// with the cache-mem/disk equality invariant that strict Del guards.
type gatedAccessor[K accessorKey, V any, R any] struct {
	processing.Accessor[K, V, R]

	g   *gatedScope
	sub byte
}

func newGatedAccessor[K accessorKey, V any, R any](inner processing.Accessor[K, V, R], g *gatedScope, sub byte) *gatedAccessor[K, V, R] {
	return &gatedAccessor[K, V, R]{Accessor: inner, g: g, sub: sub}
}

func (a *gatedAccessor[K, V, R]) Get(key K) (R, error) {
	if err := a.g.CheckCoverage(a.sub, key); err != nil {
		var zero R

		return zero, err
	}

	return a.Accessor.Get(key)
}

func (a *gatedAccessor[K, V, R]) Delete(key K) error {
	if err := a.g.CheckCoverage(a.sub, key); err != nil {
		return err
	}

	return a.Accessor.Delete(key)
}

// Mutate gates the coverage check on the read half. The underlying
// Accessor's Mutate performs the clone-on-first-touch and returns the
// overlay pointer on subsequent calls; the gate here mirrors what Get
// enforces so an undeclared key can't smuggle a mutation into the
// overlay via the Mutate path.
func (a *gatedAccessor[K, V, R]) Mutate(key K) (V, error) {
	if err := a.g.CheckCoverage(a.sub, key); err != nil {
		var zero V

		return zero, err
	}

	return a.Accessor.Mutate(key)
}

// recorderAccessor decorates an inner Accessor by recording every
// Put-touched key under a caller-controlled slot index. The slots
// themselves are an opaque [][]K vector; the caller decides what the
// indices mean (today: per-order indices set by WriteSet.BeginOrder so
// Merge can compute the per-log subset of purged ephemeral accounts).
//
// Get and Delete inherit from the embedded Accessor unchanged. Only Put
// is overridden to fan out into the slots table. BeginSlot(-1) — the
// zero value — disables recording; out-of-band Puts (recovery, technical
// updates, ValidateTransientVolumes) flow through untracked.
type recorderAccessor[K accessorKey, V any, R any] struct {
	processing.Accessor[K, V, R]

	slot  int
	slots [][]K
}

func newRecorderAccessor[K accessorKey, V any, R any](inner processing.Accessor[K, V, R]) *recorderAccessor[K, V, R] {
	return &recorderAccessor[K, V, R]{Accessor: inner, slot: -1}
}

func (a *recorderAccessor[K, V, R]) Put(key K, value V) {
	a.Accessor.Put(key, value)

	if a.slot < 0 {
		return
	}

	for len(a.slots) <= a.slot {
		a.slots = append(a.slots, nil)
	}
	a.slots[a.slot] = append(a.slots[a.slot], key)
}

// BeginSlot advances the recording slot. -1 disables recording for
// subsequent Puts.
func (a *recorderAccessor[K, V, R]) BeginSlot(idx int) {
	a.slot = idx
}

// Slots returns the per-slot keys captured so far. The returned slices
// alias the recorder's internal state; callers MUST NOT mutate them.
func (a *recorderAccessor[K, V, R]) Slots() [][]K {
	return a.slots
}

// Reset prepares the recorder for a new proposal: clears the slot pointer
// and truncates the outer slice (keeping its backing array). Inner slices
// are nil'd so the next Put for a slot allocates fresh; the prior backing
// array is released to GC. Mirrors the pre-refactor WriteSet.Reset logic.
func (a *recorderAccessor[K, V, R]) Reset() {
	a.slot = -1

	for i := range a.slots {
		a.slots[i] = nil
	}
	a.slots = a.slots[:0]
}
