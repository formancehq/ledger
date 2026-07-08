package attributes

import (
	"errors"
	"fmt"
	"sync"

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

// Delete tombstones the entry in the underlying store. On the dual-generation
// AttributeCache the tombstone lands in Gen0 (in-place when Gen0 has the
// entry, or lazily fabricated from Gen1's tag when only Gen1 has it) — this
// mirrors the single-byte writeCacheTombstone the FSM issues in the same
// batch, keeping the in-memory cache equal to disk for the same applied
// index (invariant #1). Any pre-existing Gen1 entry is left untouched: the
// Gen0 tombstone shadows it on every read, and the stale Gen1 row is
// purged on the next rotation.
//
// Returns domain.ErrNotFound only when the key is absent from both
// generations. The caller (DerivedKeyStore.Merge) treats this as an
// idempotent no-op — legitimate for mirror-ingest paths that apply Delete
// logs without a prior existence check, and safe under invariant #1 (no
// downstream tombstone is written, so cache and disk stay aligned).
//
// Tombstones age out naturally via rotation.
func (s *KeyStore[K, T]) Delete(canonical []byte) (id U128, tag uint64, err error) {
	id, tag = s.hasher.MakeKey(canonical)

	entry, ok := s.M.Get(id)
	if !ok {
		return id, tag, domain.ErrNotFound
	}

	if entry.Tag != tag {
		return id, tag, newErrCollisionDetected(canonical, entry.Tag, tag)
	}

	if delErr := s.M.Del(id); delErr != nil {
		return id, tag, delErr
	}

	return id, tag, nil
}

type Entry[T any] struct {
	Tag     uint64
	Data    T
	Deleted bool
}

// DerivedKeyStore overlays a local map of writes/deletions on top of a
// ParentReader for reads and a *KeyStore for the eventual Merge writes.
//
// The two roles are split so the read side can be wrapped by the
// coverage gate (state.gatedScope) without touching the writer path.
// Writes always go through the concrete *KeyStore: Merge mutates the
// parent registry directly, not via the interface.
type DerivedKeyStore[K Key, T any] struct {
	parent ParentReader[K, T]
	writer *KeyStore[K, T]

	values    map[K]T
	deletions map[K]struct{}
	scratch   []byte // reusable buffer for Get — single-goroutine use only
}

func (s *DerivedKeyStore[K, T]) Put(canonical K, value T) {
	delete(s.deletions, canonical)
	s.values[canonical] = value
}

// Get returns a value from local writes, or falls back to the parent
// KeyStore. A key deleted in this batch returns ErrNotFound, like a
// committed tombstone in the parent store. The returned value MUST NOT
// be mutated in place — use AsReader()/Mutate() on the proto type to
// obtain a safe mutable clone.
//
// The coverage gate is enforced one layer up (state.gatedScope wrapping
// the WriteSet that owns this DerivedKeyStore), so the read here does
// not double-check coverage. parent is the underlying KeyStore — a
// fallthrough miss returns the bare ErrNotFound, not *ErrCoverageMiss.
func (s *DerivedKeyStore[K, T]) Get(canonical K) (value T, err error) {
	if _, ok := s.deletions[canonical]; ok {
		var zero T

		return zero, domain.ErrNotFound
	}

	if localV, ok := s.values[canonical]; ok {
		return localV, nil
	}

	s.scratch = canonical.AppendBytes(s.scratch[:0])

	v, _, err := s.parent.Get(s.scratch)

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

		overwrite, idWithTag, err := s.writer.Put(canonical, v)
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

		id, tag, err := s.writer.Delete(canonical)
		if err != nil {
			// Genuinely absent: writer.Delete returns ErrNotFound when the
			// underlying AttributeCache misses on the key in BOTH generations
			// (Get uses a gen0→gen1 fallback, so ErrNotFound here is a "not
			// anywhere" signal). Skip the deletion entirely — writing a
			// downstream disk tombstone would drift the persisted state past
			// the in-memory cache (invariant #1). Legitimate for mirror-
			// ingest paths that apply Delete logs without a prior existence
			// check; handlers that do a Get-first business check surface
			// their own domain error before Delete is queued here.
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}

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

// MergeParallel drains the overlay across k worker goroutines. Each worker
// owns a disjoint slice of the values / deletions maps and hits the underlying
// KeyStore (ShardedMap with 256 shards) concurrently. Since KeyStore.Put and
// KeyStore.Delete only touch the ShardedMap (per-shard RWMutex) and the
// stateless XXH3 hasher — never DerivedKeyStore.scratch — parallel writers
// are safe as long as each worker owns its own scratch buffer.
//
// For k <= 1 or overlays below parallelDrainThreshold, delegates to the
// serial Merge — the goroutine spawn overhead outweighs any parallel win
// on small batches.
//
// The returned slices concatenate per-worker outputs in worker order; no
// downstream consumer depends on Update ordering (bloom updates are set-based,
// flushAttributeAndCache's per-key writes commute in a Pebble batch).
func (s *DerivedKeyStore[K, T]) MergeParallel(k int) ([]Update[K, T], []Deletion[K], error) {
	total := len(s.values) + len(s.deletions)
	if k <= 1 || total < parallelDrainThreshold {
		return s.Merge()
	}

	valueKeys := make([]K, 0, len(s.values))
	for kk := range s.values {
		valueKeys = append(valueKeys, kk)
	}

	deletionKeys := make([]K, 0, len(s.deletions))
	for kk := range s.deletions {
		deletionKeys = append(deletionKeys, kk)
	}

	type workerResult struct {
		touched   []Update[K, T]
		deletions []Deletion[K]
		err       error
	}

	results := make([]workerResult, k)

	var wg sync.WaitGroup
	for w := 0; w < k; w++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			vStart := (len(valueKeys) * idx) / k
			vEnd := (len(valueKeys) * (idx + 1)) / k
			dStart := (len(deletionKeys) * idx) / k
			dEnd := (len(deletionKeys) * (idx + 1)) / k

			var scratch []byte

			touched := make([]Update[K, T], 0, vEnd-vStart)
			for _, kk := range valueKeys[vStart:vEnd] {
				scratch = kk.AppendBytes(scratch[:0])
				canonical := append([]byte(nil), scratch...)

				v := s.values[kk]

				overwrite, idWithTag, err := s.writer.Put(canonical, v)
				if err != nil {
					results[idx].err = err

					return
				}

				touched = append(touched, Update[K, T]{
					Key:          kk,
					ID:           idWithTag.ID,
					Tag:          idWithTag.Tag,
					CanonicalKey: canonical,
					Old:          overwrite,
					New:          v,
				})
			}

			deletions := make([]Deletion[K], 0, dEnd-dStart)
			for _, kk := range deletionKeys[dStart:dEnd] {
				scratch = kk.AppendBytes(scratch[:0])
				canonical := append([]byte(nil), scratch...)

				id, tag, err := s.writer.Delete(canonical)
				if err != nil {
					if errors.Is(err, domain.ErrNotFound) {
						continue
					}

					results[idx].err = err

					return
				}

				deletions = append(deletions, Deletion[K]{
					Key:          kk,
					ID:           id,
					Tag:          tag,
					CanonicalKey: canonical,
				})
			}

			results[idx].touched = touched
			results[idx].deletions = deletions
		}(w)
	}
	wg.Wait()

	touchedTotal := 0
	deletionsTotal := 0
	for _, r := range results {
		if r.err != nil {
			return nil, nil, r.err
		}

		touchedTotal += len(r.touched)
		deletionsTotal += len(r.deletions)
	}

	touched := make([]Update[K, T], 0, touchedTotal)
	deletions := make([]Deletion[K], 0, deletionsTotal)
	for _, r := range results {
		touched = append(touched, r.touched...)
		deletions = append(deletions, r.deletions...)
	}

	return touched, deletions, nil
}

// parallelDrainThreshold is the minimum overlay size (values + deletions)
// at which MergeParallel spawns goroutines. Below this, the serial Merge
// path wins on the goroutine-spawn overhead.
const parallelDrainThreshold = 512

// DirtyValues returns the uncommitted local values written during the current batch.
func (s *DerivedKeyStore[K, T]) DirtyValues() map[K]T {
	return s.values
}

// Parent returns the read-side parent of this overlay. May be a *KeyStore
// (recovery/sync) or a preload.View sub-reader (FSM hot path).
func (s *DerivedKeyStore[K, T]) Parent() ParentReader[K, T] {
	return s.parent
}

// NewDerivedKeyStore builds a DerivedKeyStore whose read-side parent is the
// underlying KeyStore directly. The coverage gate is enforced one layer up
// (state.gatedScope) rather than threaded through the parent, so the
// DerivedKeyStore stays a pure in-batch overlay that falls through to the
// underlying store on miss.
func NewDerivedKeyStore[K Key, T any](store *KeyStore[K, T]) *DerivedKeyStore[K, T] {
	return &DerivedKeyStore[K, T]{
		parent:    store,
		writer:    store,
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
