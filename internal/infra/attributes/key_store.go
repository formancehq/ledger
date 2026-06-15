package attributes

import (
	"errors"
	"fmt"

	"github.com/zeebo/xxh3"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/kv"
)

type ErrCollisionDetected struct {
	Bytes       []byte
	OriginalTag uint64
	NewTag      uint64
}

func (e *ErrCollisionDetected) Error() string {
	return fmt.Sprintf("collision detected, bytes: %02X, original_tag: %d, new_tag: %d", e.Bytes, e.OriginalTag, e.NewTag)
}

func newErrCollisionDetected(bytes []byte, originalTag, newTag uint64) *ErrCollisionDetected {
	return &ErrCollisionDetected{
		Bytes:       bytes,
		OriginalTag: originalTag,
		NewTag:      newTag,
	}
}

// KeyHasher computes (U128, tag64) from canonical bytes using unseeded XXH3.
// The cache U128 layer does not need per-cluster keying — see
// processing.HashGenerator for the structure that does.
type KeyHasher struct{}

// NewKeyHasher creates a new KeyHasher.
func NewKeyHasher() *KeyHasher {
	return &KeyHasher{}
}

// MakeKey computes (U128, tag64) from canonical bytes using XXH3.
// Lock-free: XXH3 functions are stateless.
func (kh *KeyHasher) MakeKey(canonical []byte) (U128, uint64) {
	u := xxh3.Hash128(canonical)
	tag := xxh3.Hash(canonical)

	return NewU128(u.Hi, u.Lo), tag
}

// Store is a thin wrapper around map[U128]Entry[T] that enforces
// the (u128, tag64) collision check on Get/Put.
type KeyStore[K Key, T any] struct {
	hasher  *KeyHasher
	M       kv.KV[U128, Entry[T]]
	scratch []byte // reusable buffer for GetKey — single-goroutine use only
}

func NewKeyStore[K Key, T any](m kv.KV[U128, Entry[T]]) *KeyStore[K, T] {
	return &KeyStore[K, T]{
		hasher: NewKeyHasher(),
		M:      m,
	}
}

// GetKey retrieves a value by typed key, reusing an internal scratch buffer
// to avoid allocating canonical bytes on every call.
// Not safe for concurrent use.
func (s *KeyStore[K, T]) GetKey(key K) (value T, id U128, err error) {
	s.scratch = key.AppendBytes(s.scratch[:0])

	return s.Get(s.scratch)
}

// Put inserts or overwrites the entry for canonical key.
// If an entry already exists under the same U128 but with a different tag,
// ErrCollisionDetected is returned and the store is left unchanged.
// Returns the old value (if any) and the new ID+tag.
func (s *KeyStore[K, T]) Put(canonical []byte, value T) (oldValue kv.Optional[T], idWithTag IDWithTag, err error) {
	id, tag := s.hasher.MakeKey(canonical)

	newEntry := Entry[T]{Tag: tag, Data: value}

	existing, existed := s.M.GetAndPut(id, newEntry)
	if existed {
		if existing.Deleted {
			// Tombstone: overwrite without collision check.
			return kv.None[T](), IDWithTag{
				ID:  id,
				Tag: tag,
			}, nil
		}

		if existing.Tag != tag {
			// Collision: roll back by restoring the old entry.
			s.M.Put(id, existing)

			return kv.None[T](), IDWithTag{}, newErrCollisionDetected(canonical, existing.Tag, tag)
		}

		return kv.Some(existing.Data), IDWithTag{
			ID:  id,
			Tag: tag,
		}, nil
	}

	return kv.None[T](), IDWithTag{
		ID:  id,
		Tag: tag,
	}, nil
}

// Get retrieves a value by canonical key.
// Tombstoned entries (Deleted=true) are treated as not found.
// If U128 exists but tag mismatches, collision is detected locally.
func (s *KeyStore[K, T]) Get(canonical []byte) (value T, id U128, err error) {
	id, tag := s.hasher.MakeKey(canonical)

	entry, ok := s.M.Get(id)
	if !ok || entry.Deleted {
		var zero T

		return zero, id, domain.ErrNotFound
	}

	if entry.Tag != tag {
		var zero T

		return zero, id, newErrCollisionDetected(canonical, entry.Tag, tag)
	}

	return entry.Data, id, nil
}

// GetEntry returns the raw Entry for canonical key, including the
// Deleted tombstone flag. Unlike Get, it does NOT treat tombstones as
// absent — that distinction is what callers like the metadata-conversion
// apply path rely on to skip keys deleted between scan and apply without
// also skipping cache-evicted keys (#359).
//
// Returns (entry, true) when the key is in the underlying map AND the
// stored tag matches the canonical key. A tag mismatch (U128 collision
// with a different canonical key) returns (zero, false) — treating it
// as "not in cache for this key" is the only safe answer: returning the
// other entry would let the caller make a decision against unrelated
// data (paul-nicolas review on #359). Mirrors Get's collision handling.
func (s *KeyStore[K, T]) GetEntry(canonical []byte) (Entry[T], bool) {
	id, tag := s.hasher.MakeKey(canonical)

	entry, ok := s.M.Get(id)
	if !ok {
		return Entry[T]{}, false
	}

	if entry.Tag != tag {
		return Entry[T]{}, false
	}

	return entry, true
}

// Delete marks the entry as a tombstone instead of removing it.
// The entry stays in the cache (surviving MirrorTouch during pipelined
// proposals) but reads via Get return nil. Tombstones age out naturally
// via cache generation rotation.
func (s *KeyStore[K, T]) Delete(canonical []byte) (id U128, tag uint64, err error) {
	id, tag = s.hasher.MakeKey(canonical)

	entry, ok := s.M.Get(id)
	if !ok {
		return id, tag, domain.ErrNotFound
	}

	if entry.Tag != tag {
		return id, tag, newErrCollisionDetected(canonical, entry.Tag, tag)
	}

	entry.Deleted = true
	s.M.Put(id, entry)

	return id, tag, nil
}

type Entry[T any] struct {
	Tag     uint64
	Data    T
	Deleted bool
}

type DerivedKeyStore[K Key, T any] struct {
	*KeyStore[K, T]

	values    map[K]T
	deletions map[K]struct{}
	scratch   []byte // reusable buffer for Get — single-goroutine use only
}

func (s *DerivedKeyStore[K, T]) Put(canonical K, value T) {
	delete(s.deletions, canonical)
	s.values[canonical] = value
}

// Get returns a value from local writes, or falls back to the parent store.
// The returned value MUST NOT be mutated in place — use AsReader()/Mutate()
// on the proto type to obtain a safe mutable clone.
func (s *DerivedKeyStore[K, T]) Get(canonical K) (value T, err error) {
	if _, ok := s.deletions[canonical]; ok {
		var zero T

		return zero, nil
	}

	if localV, ok := s.values[canonical]; ok {
		return localV, nil
	}

	s.scratch = canonical.AppendBytes(s.scratch[:0])

	v, _, err := s.KeyStore.Get(s.scratch)

	return v, err
}

func (s *DerivedKeyStore[K, T]) Delete(canonical K) {
	delete(s.values, canonical)
	s.deletions[canonical] = struct{}{}
}

func (s *DerivedKeyStore[K, T]) Merge() ([]Update[K, T], []Deletion[K], error) {
	// Reuse scratch for serializing canonical keys during Put/Delete,
	// but allocate independent copies for each Update.CanonicalKey.
	// The scratch buffer is shared with Get(), which may overwrite it
	// when processing a subsequent entry in the same PrepareEntries batch.

	touched := make([]Update[K, T], 0, len(s.values))
	for k, v := range s.values {
		s.scratch = k.AppendBytes(s.scratch[:0])
		canonical := append([]byte(nil), s.scratch...)

		overwrite, idWithTag, err := s.KeyStore.Put(canonical, v)
		if err != nil {
			return nil, nil, err
		}

		touched = append(touched, Update[K, T]{
			Key:          k,
			ID:           idWithTag.ID,
			Tag:          idWithTag.Tag,
			CanonicalKey: canonical,
			Old:          overwrite,
			New:          v,
		})
	}

	deletions := make([]Deletion[K], 0, len(s.deletions))
	for k := range s.deletions {
		s.scratch = k.AppendBytes(s.scratch[:0])
		canonical := append([]byte(nil), s.scratch...)

		id, tag, err := s.KeyStore.Delete(canonical)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return nil, nil, err
		}

		deletions = append(deletions, Deletion[K]{
			Key:          k,
			ID:           id,
			Tag:          tag,
			CanonicalKey: canonical,
		})
	}

	return touched, deletions, nil
}

// DirtyValues returns the uncommitted local values written during the current batch.
func (s *DerivedKeyStore[K, T]) DirtyValues() map[K]T {
	return s.values
}

// Parent returns the underlying KeyStore.
func (s *DerivedKeyStore[K, T]) Parent() *KeyStore[K, T] {
	return s.KeyStore
}

func NewDerivedKeyStore[K Key, T any](store *KeyStore[K, T]) *DerivedKeyStore[K, T] {
	return &DerivedKeyStore[K, T]{
		KeyStore:  store,
		values:    make(map[K]T),
		deletions: make(map[K]struct{}),
	}
}

// Reset clears the local overlay for reuse without reallocating maps.
func (s *DerivedKeyStore[K, T]) Reset() {
	clear(s.values)
	clear(s.deletions)
}

type Update[K Key, T any] struct {
	Key          K
	ID           U128
	Tag          uint64
	CanonicalKey []byte
	Old          kv.Optional[T]
	New          T
}

type Deletion[K Key] struct {
	Key          K
	ID           U128
	Tag          uint64
	CanonicalKey []byte
}
