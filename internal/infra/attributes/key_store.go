package attributes

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/kv"
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

// Seeds holds domain-separated XXH3 seeds for ID (128-bit) and Tag (64-bit).
// Use a single MasterKey (32 bytes) and derive both seeds once at startup.
type Seeds struct {
	IDSeed  uint64 // seed for xxh3.Hash128Seed (128-bit ID)
	TagSeed uint64 // seed for xxh3.HashSeed (64-bit collision tag)
}

var DefaultSeeds = DeriveSeeds([32]byte{
	0x3C, 0x3C, 0x69, 0x66, 0x79, 0x6F, 0x75, 0x72,
	0x65, 0x61, 0x64, 0x74, 0x68, 0x69, 0x73, 0x79,
	0x6F, 0x75, 0x66, 0x6F, 0x75, 0x6E, 0x64, 0x61,
	0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x3E, 0x3E,
})

// DeriveSeeds derives two independent uint64 seeds from a single master key
// using domain-separated BLAKE3. MasterKey MUST be 32 bytes.
func DeriveSeeds(masterKey [32]byte) Seeds {
	idHash := blake3.Sum256(append([]byte("attrid:v1:id128:"), masterKey[:]...))
	tagHash := blake3.Sum256(append([]byte("attrid:v1:tag64:"), masterKey[:]...))

	return Seeds{
		IDSeed:  binary.LittleEndian.Uint64(idHash[:8]),
		TagSeed: binary.LittleEndian.Uint64(tagHash[:8]),
	}
}

// KeyHasher provides efficient hashing using XXH3.
// XXH3 functions are stateless, so no mutex or pre-allocated buffers are needed.
type KeyHasher struct {
	seeds Seeds
}

// NewKeyHasher creates a new KeyHasher with the given seeds.
func NewKeyHasher(seeds Seeds) *KeyHasher {
	return &KeyHasher{seeds: seeds}
}

// MakeKey computes (U128, tag64) from canonical bytes using XXH3.
// Lock-free: XXH3 functions are stateless.
func (kh *KeyHasher) MakeKey(canonical []byte) (U128, uint64) {
	u := xxh3.Hash128Seed(canonical, kh.seeds.IDSeed)
	tag := xxh3.HashSeed(canonical, kh.seeds.TagSeed)

	return NewU128(u.Hi, u.Lo), tag
}

// Store is a thin wrapper around map[U128]Entry[T] that enforces
// the (u128, tag64) collision check on Get/Put.
type KeyStore[K Key, T any] struct {
	hasher  *KeyHasher
	M       kv.KV[U128, Entry[T]]
	scratch []byte // reusable buffer for GetKey — single-goroutine use only
}

func NewKeyStore[K Key, T any](seeds Seeds, m kv.KV[U128, Entry[T]]) *KeyStore[K, T] {
	return &KeyStore[K, T]{
		hasher: NewKeyHasher(seeds),
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

// Delete marks the entry as a tombstone instead of removing it.
// The entry stays in the cache (surviving MirrorTouch during pipelined
// proposals) but reads via Get return nil. Tombstones age out naturally
// via cache generation rotation.
func (s *KeyStore[K, T]) Delete(canonical []byte) (id U128, err error) {
	id, tag := s.hasher.MakeKey(canonical)

	entry, ok := s.M.Get(id)
	if !ok {
		return id, domain.ErrNotFound
	}

	if entry.Tag != tag {
		return id, newErrCollisionDetected(canonical, entry.Tag, tag)
	}

	entry.Deleted = true
	s.M.Put(id, entry)

	return id, nil
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
	readCache map[K]T   // cloned values from parent, avoids re-cloning on repeated reads
	cloneFn   func(T) T // optional: clone on Get from parent to protect against in-place mutation
	scratch   []byte    // reusable buffer for Get — single-goroutine use only
}

func (s *DerivedKeyStore[K, T]) Put(canonical K, value T) {
	delete(s.deletions, canonical)
	delete(s.readCache, canonical)
	s.values[canonical] = value
}

func (s *DerivedKeyStore[K, T]) Get(canonical K) (value T, err error) {
	// Check if deleted in this batch
	if _, ok := s.deletions[canonical]; ok {
		var zero T

		return zero, nil
	}

	// Check local values (uncommitted changes)
	if localV, ok := s.values[canonical]; ok {
		return localV, nil
	}

	// Check read cache (previously cloned from parent in this batch)
	if cached, ok := s.readCache[canonical]; ok {
		return cached, nil
	}

	// Then check underlying store (reuse scratch buffer to avoid allocation)
	s.scratch = canonical.AppendBytes(s.scratch[:0])

	v, _, err := s.KeyStore.Get(s.scratch)
	if err != nil {
		return v, err
	}

	// Clone once and cache to avoid re-cloning on subsequent reads.
	if s.cloneFn != nil {
		v = s.cloneFn(v)
		s.readCache[canonical] = v
	}

	return v, nil
}

func (s *DerivedKeyStore[K, T]) Delete(canonical K) {
	delete(s.values, canonical)
	delete(s.readCache, canonical)
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

		id, err := s.KeyStore.Delete(canonical)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return nil, nil, err
		}

		deletions = append(deletions, Deletion[K]{
			Key:          k,
			ID:           id,
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

func NewDerivedKeyStore[K Key, T any](store *KeyStore[K, T], cloneFn func(T) T) *DerivedKeyStore[K, T] {
	return &DerivedKeyStore[K, T]{
		KeyStore:  store,
		values:    make(map[K]T),
		deletions: make(map[K]struct{}),
		readCache: make(map[K]T),
		cloneFn:   cloneFn,
	}
}

// Reset clears the local overlay for reuse without reallocating maps.
func (s *DerivedKeyStore[K, T]) Reset() {
	clear(s.values)
	clear(s.deletions)
	clear(s.readCache)
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
	CanonicalKey []byte
}
