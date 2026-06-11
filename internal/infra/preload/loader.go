package preload

import (
	"sync"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

const loaderShards = 256

// loadedEntry stores a loaded attribute value with the cache state that made
// the value safe to reuse.
type loadedEntry[T any] struct {
	boundary   uint64
	cacheEpoch uint64
	value      T
}

func (e *loadedEntry[T]) validFor(boundary, cacheEpoch uint64) bool {
	return e.cacheEpoch == cacheEpoch && e.boundary >= boundary
}

// loaderShard is one of loaderShards independent partitions, each with its own
// mutex and maps. Cache-line padding prevents false sharing between shards.
type loaderShard[T any] struct {
	mu      sync.RWMutex
	loading map[attributes.U128]chan struct{}
	loaded  map[attributes.U128]*loadedEntry[T]
	_       [64]byte // cache-line padding
}

// AttributeLoader coordinates loading of attributes to prevent duplicate loads from store.
// It uses per-key locks to ensure only one goroutine loads a given attribute at a time.
// T is the type of the loaded value.
//
// Internally sharded into 256 partitions keyed by U128.Lo() to reduce contention
// on the RWMutex under high concurrency.
type AttributeLoader[T any] struct {
	shards [loaderShards]loaderShard[T]
}

// LoadResult represents the result of loading an attribute.
type LoadResult[T any] struct {
	Value    T
	FromLoad bool // true if we actually loaded from store, false if from loader cache
}

// NewAttributeLoader creates a new AttributeLoader for the given type.
func NewAttributeLoader[T any]() *AttributeLoader[T] {
	al := &AttributeLoader[T]{}
	for i := range al.shards {
		al.shards[i].loading = make(map[attributes.U128]chan struct{})
		al.shards[i].loaded = make(map[attributes.U128]*loadedEntry[T])
	}

	return al
}

// shard returns the shard for the given key. U128 keys are BLAKE3 hashes,
// so Lo() is uniformly distributed and a simple bit-mask suffices.
func (al *AttributeLoader[T]) shard(key attributes.U128) *loaderShard[T] {
	return &al.shards[key.Lo()&(loaderShards-1)]
}

// LoadOrWait loads an attribute value or waits for an ongoing load.
// It returns the value and whether we actually performed a load (vs using cached).
// The loadFn is called only if the value needs to be loaded from store.
func (al *AttributeLoader[T]) LoadOrWait(key attributes.U128, boundary, cacheEpoch uint64, loadFn func() (T, error)) (*LoadResult[T], error) {
	s := al.shard(key)

	// Fast path: check if already loaded using read lock
	s.mu.RLock()

	if cached, ok := s.loaded[key]; ok && cached.validFor(boundary, cacheEpoch) {
		s.mu.RUnlock()

		return &LoadResult[T]{Value: cached.value, FromLoad: false}, nil
	}
	// Check if someone is already loading this key
	waitCh, isLoading := s.loading[key]
	s.mu.RUnlock()

	if isLoading {
		// Wait for the ongoing load to complete
		<-waitCh
		// Re-check with read lock
		s.mu.RLock()

		if cached, ok := s.loaded[key]; ok && cached.validFor(boundary, cacheEpoch) {
			s.mu.RUnlock()

			return &LoadResult[T]{Value: cached.value, FromLoad: false}, nil
		}

		s.mu.RUnlock()
		// Load failed or cached entry does not match this cache state.
		// Fall through to try loading ourselves.
	}

	// Slow path: need to load - acquire write lock
	s.mu.Lock()

	// Double-check after acquiring write lock (another goroutine might have loaded it)
	if cached, ok := s.loaded[key]; ok && cached.validFor(boundary, cacheEpoch) {
		s.mu.Unlock()

		return &LoadResult[T]{Value: cached.value, FromLoad: false}, nil
	}

	// Check again if someone started loading while we were waiting for the lock
	if waitCh, ok := s.loading[key]; ok {
		s.mu.Unlock()
		// Wait and retry from the beginning
		<-waitCh

		return al.LoadOrWait(key, boundary, cacheEpoch, loadFn)
	}

	// We're the one who will load - mark as loading
	waitCh = make(chan struct{})
	s.loading[key] = waitCh
	s.mu.Unlock()

	// Perform the actual load (outside of lock)
	value, err := loadFn()

	// Update state with write lock
	s.mu.Lock()
	delete(s.loading, key)

	if err == nil {
		s.loaded[key] = &loadedEntry[T]{boundary: boundary, cacheEpoch: cacheEpoch, value: value}
	}

	close(waitCh)
	s.mu.Unlock()

	if err != nil {
		var zero T

		return &LoadResult[T]{Value: zero, FromLoad: false}, err
	}

	return &LoadResult[T]{Value: value, FromLoad: true}, nil
}

// Release removes the loaded entry for the given key.
// This should be called after the command has been applied and the cache updated.
func (al *AttributeLoader[T]) Release(key attributes.U128) {
	s := al.shard(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.loaded, key)
}

// Loaders groups all attribute loaders by type.
type Loaders struct {
	Volumes           *AttributeLoader[*raftcmdpb.VolumePair]
	References        *AttributeLoader[*commonpb.TransactionReferenceValue]
	Ledgers           *AttributeLoader[*commonpb.LedgerInfo]
	Boundaries        *AttributeLoader[*raftcmdpb.LedgerBoundaries]
	SinkConfigs       *AttributeLoader[*commonpb.SinkConfig]
	AccountMetadata   *AttributeLoader[*commonpb.MetadataValue]
	NumscriptVersions *AttributeLoader[*commonpb.NumscriptVersionValue]
	Transactions      *AttributeLoader[*commonpb.TransactionState]
	NumscriptContents *AttributeLoader[*commonpb.NumscriptInfo]
	PreparedQueries   *AttributeLoader[*commonpb.PreparedQuery]
	LedgerMetadata    *AttributeLoader[*commonpb.MetadataValue]
}

// NewLoaders creates a new Loaders instance with all attribute loaders initialized.
func NewLoaders() *Loaders {
	return &Loaders{
		Volumes:           NewAttributeLoader[*raftcmdpb.VolumePair](),
		References:        NewAttributeLoader[*commonpb.TransactionReferenceValue](),
		Ledgers:           NewAttributeLoader[*commonpb.LedgerInfo](),
		Boundaries:        NewAttributeLoader[*raftcmdpb.LedgerBoundaries](),
		SinkConfigs:       NewAttributeLoader[*commonpb.SinkConfig](),
		AccountMetadata:   NewAttributeLoader[*commonpb.MetadataValue](),
		NumscriptVersions: NewAttributeLoader[*commonpb.NumscriptVersionValue](),
		Transactions:      NewAttributeLoader[*commonpb.TransactionState](),
		NumscriptContents: NewAttributeLoader[*commonpb.NumscriptInfo](),
		PreparedQueries:   NewAttributeLoader[*commonpb.PreparedQuery](),
		LedgerMetadata:    NewAttributeLoader[*commonpb.MetadataValue](),
	}
}

// LoaderOps is the non-generic interface satisfied by every AttributeLoader[T].
// It captures the Release operation needed by CleanupToken.
type LoaderOps interface {
	Release(attributes.U128)
}

// trackedLoader pairs a loader with the keys that were loaded through it.
type trackedLoader struct {
	loader LoaderOps
	keys   []attributes.U128
}

// CleanupToken tracks which keys were loaded for each attribute type.
// Used to clean up loaded entries after a command is applied.
type CleanupToken struct {
	tracked []trackedLoader
}

// Release cleans up all tracked keys from their respective loaders.
func (t *CleanupToken) Release() {
	for i := range t.tracked {
		for _, key := range t.tracked[i].keys {
			t.tracked[i].loader.Release(key)
		}
	}
}
