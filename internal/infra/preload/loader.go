package preload

import (
	"sync"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

const loaderShards = 256

// loadedEntry stores a loaded attribute value with its boundary.
type loadedEntry[T any] struct {
	boundary  uint64
	value     T
	baseIndex uint64 // Raft index of the Pebble entry (for point deletes)
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
	Value     T
	BaseIndex uint64 // Raft index of the Pebble entry (for point deletes)
	FromLoad  bool   // true if we actually loaded from store, false if from loader cache
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
// It returns the value, the base index from Pebble, and whether we actually
// performed a load (vs using cached).
// The loadFn is called only if the value needs to be loaded from store.
// It returns (value, baseIndex, error) where baseIndex is the raft index of the
// Pebble entry (used for point deletes instead of range tombstones).
func (al *AttributeLoader[T]) LoadOrWait(key attributes.U128, boundary uint64, loadFn func() (T, uint64, error)) (*LoadResult[T], error) {
	s := al.shard(key)

	// Fast path: check if already loaded using read lock
	s.mu.RLock()

	if cached, ok := s.loaded[key]; ok && cached.boundary >= boundary {
		s.mu.RUnlock()

		return &LoadResult[T]{Value: cached.value, BaseIndex: cached.baseIndex, FromLoad: false}, nil
	}
	// Check if someone is already loading this key
	waitCh, isLoading := s.loading[key]
	s.mu.RUnlock()

	if isLoading {
		// Wait for the ongoing load to complete
		<-waitCh
		// Re-check with read lock
		s.mu.RLock()

		if cached, ok := s.loaded[key]; ok && cached.boundary >= boundary {
			s.mu.RUnlock()

			return &LoadResult[T]{Value: cached.value, BaseIndex: cached.baseIndex, FromLoad: false}, nil
		}

		s.mu.RUnlock()
		// Load failed or boundary mismatch - fall through to try loading ourselves
	}

	// Slow path: need to load - acquire write lock
	s.mu.Lock()

	// Double-check after acquiring write lock (another goroutine might have loaded it)
	if cached, ok := s.loaded[key]; ok && cached.boundary >= boundary {
		s.mu.Unlock()

		return &LoadResult[T]{Value: cached.value, BaseIndex: cached.baseIndex, FromLoad: false}, nil
	}

	// Check again if someone started loading while we were waiting for the lock
	if waitCh, ok := s.loading[key]; ok {
		s.mu.Unlock()
		// Wait and retry from the beginning
		<-waitCh

		return al.LoadOrWait(key, boundary, loadFn)
	}

	// We're the one who will load - mark as loading
	waitCh = make(chan struct{})
	s.loading[key] = waitCh
	s.mu.Unlock()

	// Perform the actual load (outside of lock)
	value, baseIndex, err := loadFn()

	// Update state with write lock
	s.mu.Lock()
	delete(s.loading, key)

	if err == nil {
		s.loaded[key] = &loadedEntry[T]{boundary: boundary, value: value, baseIndex: baseIndex}
	}

	close(waitCh)
	s.mu.Unlock()

	if err != nil {
		var zero T

		return &LoadResult[T]{Value: zero, FromLoad: false}, err
	}

	return &LoadResult[T]{Value: value, BaseIndex: baseIndex, FromLoad: true}, nil
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
	IdempotencyKeys   *AttributeLoader[*commonpb.IdempotencyKeyValue]
	References        *AttributeLoader[*commonpb.TransactionReferenceValue]
	Ledgers           *AttributeLoader[*commonpb.LedgerInfo]
	Boundaries        *AttributeLoader[*raftcmdpb.LedgerBoundaries]
	SinkConfigs       *AttributeLoader[*commonpb.SinkConfig]
	AccountMetadata   *AttributeLoader[*commonpb.MetadataValue]
	NumscriptVersions *AttributeLoader[string]
	NumscriptEntries  *AttributeLoader[bool]
	NumscriptParsed   *AttributeLoader[string]
	Transactions      *AttributeLoader[*commonpb.TransactionState]
}

// NewLoaders creates a new Loaders instance with all attribute loaders initialized.
func NewLoaders() *Loaders {
	return &Loaders{
		Volumes:           NewAttributeLoader[*raftcmdpb.VolumePair](),
		IdempotencyKeys:   NewAttributeLoader[*commonpb.IdempotencyKeyValue](),
		References:        NewAttributeLoader[*commonpb.TransactionReferenceValue](),
		Ledgers:           NewAttributeLoader[*commonpb.LedgerInfo](),
		Boundaries:        NewAttributeLoader[*raftcmdpb.LedgerBoundaries](),
		SinkConfigs:       NewAttributeLoader[*commonpb.SinkConfig](),
		AccountMetadata:   NewAttributeLoader[*commonpb.MetadataValue](),
		NumscriptVersions: NewAttributeLoader[string](),
		NumscriptEntries:  NewAttributeLoader[bool](),
		NumscriptParsed:   NewAttributeLoader[string](),
		Transactions:      NewAttributeLoader[*commonpb.TransactionState](),
	}
}

// CleanupToken tracks which keys were loaded for each attribute type.
// Used to clean up loaded entries after a command is applied.
type CleanupToken struct {
	Volumes           []attributes.U128
	IdempotencyKeys   []attributes.U128
	References        []attributes.U128
	Ledgers           []attributes.U128
	Boundaries        []attributes.U128
	SinkConfigs       []attributes.U128
	AccountMetadata   []attributes.U128
	NumscriptVersions []attributes.U128
	NumscriptEntries  []attributes.U128
	NumscriptParsed   []attributes.U128
	Transactions      []attributes.U128
}

// Release cleans up all tracked keys from their respective loaders.
func (t *CleanupToken) Release(loaders *Loaders) {
	for _, key := range t.Volumes {
		loaders.Volumes.Release(key)
	}

	for _, key := range t.IdempotencyKeys {
		loaders.IdempotencyKeys.Release(key)
	}

	for _, key := range t.References {
		loaders.References.Release(key)
	}

	for _, key := range t.Ledgers {
		loaders.Ledgers.Release(key)
	}

	for _, key := range t.Boundaries {
		loaders.Boundaries.Release(key)
	}

	for _, key := range t.SinkConfigs {
		loaders.SinkConfigs.Release(key)
	}

	for _, key := range t.AccountMetadata {
		loaders.AccountMetadata.Release(key)
	}

	for _, key := range t.NumscriptVersions {
		loaders.NumscriptVersions.Release(key)
	}

	for _, key := range t.NumscriptEntries {
		loaders.NumscriptEntries.Release(key)
	}

	for _, key := range t.NumscriptParsed {
		loaders.NumscriptParsed.Release(key)
	}

	for _, key := range t.Transactions {
		loaders.Transactions.Release(key)
	}
}
