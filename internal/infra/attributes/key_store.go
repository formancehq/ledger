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
	hasher *KeyHasher
	M      kv.KV[U128, Entry[T]]
}

func NewKeyStore[K Key, T any](seeds Seeds, m kv.KV[U128, Entry[T]]) *KeyStore[K, T] {
	return &KeyStore[K, T]{
		hasher: NewKeyHasher(seeds),
		M:      m,
	}
}

// Put inserts or overwrites the entry for canonical key.
// If an entry already exists under the same U128 but with a different tag,
// ErrCollisionDetected is returned and the store is left unchanged.
// Returns the old value (if any) and the new ID+tag.
func (s *KeyStore[K, T]) Put(canonical []byte, value T) (oldValue kv.Optional[T], idWithTag IDWithTag, err error) {
	id, tag := s.hasher.MakeKey(canonical)

	if existing, ok := s.M.Get(id); ok {
		if existing.Tag != tag {
			return kv.None[T](), IDWithTag{}, newErrCollisionDetected(canonical, existing.Tag, tag)
		}
		// same key (as far as we can tell) -> overwrite data
		s.M.Put(id, Entry[T]{Tag: tag, Data: value})

		return kv.Some(existing.Data), IDWithTag{
			ID:  id,
			Tag: tag,
		}, nil
	}

	s.M.Put(id, Entry[T]{Tag: tag, Data: value})

	return kv.None[T](), IDWithTag{
		ID:  id,
		Tag: tag,
	}, nil
}

// Get retrieves a value by canonical key.
// If U128 exists but tag mismatches, collision is detected locally.
func (s *KeyStore[K, T]) Get(canonical []byte) (value T, id U128, err error) {
	id, tag := s.hasher.MakeKey(canonical)

	entry, ok := s.M.Get(id)
	if !ok {
		var zero T

		return zero, id, domain.ErrNotFound
	}

	if entry.Tag != tag {
		var zero T

		return zero, id, newErrCollisionDetected(canonical, entry.Tag, tag)
	}

	return entry.Data, id, nil
}

// Delete removes the entry for canonical key.
// If U128 exists but tag mismatches, collision is detected locally.
func (s *KeyStore[K, T]) Delete(canonical []byte) (id U128, err error) {
	id, tag := s.hasher.MakeKey(canonical)

	entry, ok := s.M.Get(id)
	if !ok {
		return id, domain.ErrNotFound
	}

	if entry.Tag != tag {
		return id, newErrCollisionDetected(canonical, entry.Tag, tag)
	}

	s.M.Del(id)

	return id, nil
}

type Entry[T any] struct {
	Tag  uint64
	Data T
}

type DerivedKeyStore[K Key, T any] struct {
	*KeyStore[K, T]

	values    map[K]T
	deletions map[K]struct{}
	cloneFn   func(T) T
}

func (s *DerivedKeyStore[K, T]) Put(canonical K, value T) {
	delete(s.deletions, canonical)
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

	// Then check underlying store
	v, _, err := s.KeyStore.Get(canonical.Bytes())
	if err != nil {
		return v, err
	}

	// Clone to prevent in-place modifications affecting the underlying store
	if s.cloneFn != nil {
		v = s.cloneFn(v)
	}

	return v, nil
}

func (s *DerivedKeyStore[K, T]) Delete(canonical K) {
	delete(s.values, canonical)
	s.deletions[canonical] = struct{}{}
}

func (s *DerivedKeyStore[K, T]) Merge() ([]Update[K, T], []Deletion[K], error) {
	touched := make([]Update[K, T], 0, len(s.values))
	for k, v := range s.values {
		canonical := k.Bytes()

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
		canonical := k.Bytes()

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

func NewDerivedKeyStore[K Key, T any](store *KeyStore[K, T], cloneFn func(T) T) *DerivedKeyStore[K, T] {
	return &DerivedKeyStore[K, T]{
		KeyStore:  store,
		values:    make(map[K]T),
		deletions: make(map[K]struct{}),
		cloneFn:   cloneFn,
	}
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
