package attributes

// ParentReader is the read-only capability that DerivedKeyStore consults on
// overlay miss. Splitting it out from *KeyStore lets DerivedKeyStore accept a
// state.Plan sub-reader (which enforces preload-coverage on every
// read and panics on a miss) without losing the ability to use the bare
// *KeyStore for paths that have no preload concept (recovery, synchronizer,
// tests).
//
// *KeyStore satisfies ParentReader directly — its Get, GetKey, and GetEntry
// methods have matching signatures.
type ParentReader[K Key, T any] interface {
	Get(canonical []byte) (T, U128, error)
	GetKey(key K) (T, U128, error)
	GetEntry(canonical []byte) (Entry[T], bool)
}
