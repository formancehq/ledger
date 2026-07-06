package processing

// AccessorKey is the constraint every per-attribute key type must satisfy
// to back an Accessor: comparable (so it can index the in-batch overlay
// map inside attributes.DerivedKeyStore) and Bytes() for the coverage
// gate to compute the canonical lookup form.
type AccessorKey interface {
	comparable
	Bytes() []byte
}

// Accessor is the per-kind read/write/delete facade exposed by Scope —
// one instance per cache-attribute kind. The three type parameters fix
// the operating types end-to-end:
//
//   - K is the canonical key (e.g. domain.LedgerKey, domain.VolumeKey).
//   - V is the in-memory value the FSM writes (e.g. *commonpb.LedgerInfo).
//   - R is the Reader view returned on read so callers cannot mutate
//     cached state in place; call R.Mutate() to obtain a writeable clone
//     before passing back through Put.
//
// Two implementations live in package state:
//
//   - raw: pure delegation to the underlying attributes.DerivedKeyStore.
//     Used by the bare *WriteSet (recovery, technical updates, tests).
//   - gated: prepends CheckCoverage(sub, key.Bytes()) so the FSM hot
//     path never reads OR deletes a key the proposer did not declare in
//     coverage_bits.
//
// Get returns:
//   - (reader, nil)              when the key is present in the overlay or parent.
//   - (zero, domain.ErrNotFound) when the parent reports a tombstone or
//     legitimately absent key (kinds that surface ErrNotFound directly).
//   - (zero, nil)                when the parent simply has no entry and the
//     kind's convention is the silent-miss form (e.g. AccountMetadata).
//   - (zero, *ErrCoverageMiss)   (gated implementation only) when the read
//     key is outside the scope's coverage map.
//   - (zero, err)                on any other underlying engine error.
//
// Delete returns:
//   - nil                        on success (entry marked for deletion in
//     the batch overlay; final tombstone happens at Merge/apply time).
//   - *ErrCoverageMiss           (gated implementation only) when the key
//     is outside the scope's coverage map — the FSM hot path never
//     deletes an undeclared key, per invariant #6.
//
// The per-kind miss convention is preserved verbatim from the pre-refactor
// surface — call sites that already discriminated on err vs nil-reader
// continue to work unchanged.
type Accessor[K AccessorKey, V any, R any] interface {
	Get(key K) (R, error)
	Put(key K, value V)
	Delete(key K) error

	// Mutate returns a mutable V for key, suitable for in-place mutation
	// by the caller. If the batch overlay already owns a value for key,
	// the overlay pointer is returned directly (no clone). Otherwise the
	// parent's value is cloned into the overlay via the R.Mutate() pattern
	// and the fresh clone is returned. Subsequent Mutate / Get calls on
	// the same key within the batch observe the caller's mutations.
	//
	// Returns the same error semantics as Get for the parent-fallback path
	// (ErrNotFound / *ErrCoverageMiss / kind-specific silent miss). On a
	// hit, err is nil and the returned V is the overlay-owned pointer.
	//
	// Callers must NOT Put the returned pointer under a different key; the
	// overlay associates it with the key passed to Mutate. Callers MAY Put
	// the same key back with the same pointer to trigger side effects
	// (slot recording via recorderAccessor) — this is a no-op for the
	// underlying map assignment.
	Mutate(key K) (V, error)
}
