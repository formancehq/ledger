package preload

import (
	"sync"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

const loaderShards = 256

// CacheStamp identifies the attribute-cache state a loaded value was read
// under. A memoized value is reusable only for a preload built against the
// same cache incarnation (Epoch and ResetSeq unchanged) at a boundary no
// later than the one it was loaded for.
type CacheStamp struct {
	// Boundary is the cache generation boundary the preload is built for
	// (cache.BoundaryIndex of the predicted apply index).
	Boundary uint64
	// Epoch is the replicated cache epoch, bumped by a threshold change.
	Epoch uint64
	// ResetSeq is the local count of cache resets (snapshot install,
	// restore), which replace the cache's contents without a new epoch.
	ResetSeq uint64
}

// loadedEntry stores a loaded attribute value with the cache state that made
// the value safe to reuse.
type loadedEntry[T any] struct {
	stamp CacheStamp
	value T
}

func (e *loadedEntry[T]) validFor(s CacheStamp) bool {
	return e.stamp.Epoch == s.Epoch && e.stamp.ResetSeq == s.ResetSeq && e.stamp.Boundary >= s.Boundary
}

// loaderShard is one of loaderShards independent partitions, each with its own
// mutex and maps. Cache-line padding prevents false sharing between shards.
type loaderShard[T any] struct {
	mu      sync.RWMutex
	loading map[attributes.U128]*inflightLoad
	loaded  map[attributes.U128]*loadedEntry[T]
	// fenced counts the commits in progress for a key (Fence … Unfence) and
	// fencedAll those in progress for every key (FenceAll … UnfenceAll). A
	// load that starts, runs or completes under either may predate the
	// write being committed and is not memoized.
	fenced    map[attributes.U128]int
	fencedAll int
	_         [64]byte // cache-line padding
}

// inflightLoad is a load in progress: waiters block on done; released marks
// a load that started under a fence or saw a Fence or Release while running,
// in which case the loaded value may predate a commit and is returned to its
// caller but not memoized.
type inflightLoad struct {
	done     chan struct{}
	released bool
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
		al.shards[i].loading = make(map[attributes.U128]*inflightLoad)
		al.shards[i].loaded = make(map[attributes.U128]*loadedEntry[T])
		al.shards[i].fenced = make(map[attributes.U128]int)
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
//
// A memoized value stays valid until a Fence, FenceAll or Release drops it or
// stamp no longer matches (validFor). The FSM fences every key a proposal
// covers before committing its batch and unfences them after (every key, for
// a batch that deletes a ledger), so no preload is ever served a value from
// before a write that has been committed.
func (al *AttributeLoader[T]) LoadOrWait(key attributes.U128, stamp CacheStamp, loadFn func() (T, error)) (*LoadResult[T], error) {
	s := al.shard(key)

	// Fast path: check if already loaded using read lock
	s.mu.RLock()

	if cached, ok := s.loaded[key]; ok && cached.validFor(stamp) {
		s.mu.RUnlock()

		return &LoadResult[T]{Value: cached.value, FromLoad: false}, nil
	}
	// Check if someone is already loading this key
	inflight, isLoading := s.loading[key]
	s.mu.RUnlock()

	if isLoading {
		// Wait for the ongoing load to complete
		<-inflight.done
		// Re-check with read lock
		s.mu.RLock()

		if cached, ok := s.loaded[key]; ok && cached.validFor(stamp) {
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
	if cached, ok := s.loaded[key]; ok && cached.validFor(stamp) {
		s.mu.Unlock()

		return &LoadResult[T]{Value: cached.value, FromLoad: false}, nil
	}

	// Check again if someone started loading while we were waiting for the lock
	if inflight, ok := s.loading[key]; ok {
		s.mu.Unlock()
		// Wait and retry from the beginning
		<-inflight.done

		return al.LoadOrWait(key, stamp, loadFn)
	}

	// We're the one who will load - mark as loading. A load that starts under
	// a fence is not memoizable even if the fence lifts before it completes:
	// its store read may predate the commit the fence brackets.
	inflight = &inflightLoad{done: make(chan struct{}), released: s.fenced[key] > 0 || s.fencedAll > 0}
	s.loading[key] = inflight
	s.mu.Unlock()

	// Perform the actual load (outside of lock)
	value, err := loadFn()

	// Update state with write lock. A fence at any point of the load's
	// lifetime, or a Release during it, means a proposal covering the key is
	// committing or has committed since loadFn read the store: the value is
	// still right for this caller's plan (the apply reconciles it against the
	// cache), but memoizing it would serve the pre-write value to every later
	// preload.
	s.mu.Lock()
	delete(s.loading, key)

	if err == nil && !inflight.released && s.fenced[key] == 0 && s.fencedAll == 0 {
		s.loaded[key] = &loadedEntry[T]{stamp: stamp, value: value}
	}

	close(inflight.done)
	s.mu.Unlock()

	if err != nil {
		var zero T

		return &LoadResult[T]{Value: zero, FromLoad: false}, err
	}

	return &LoadResult[T]{Value: value, FromLoad: true}, nil
}

// Release removes the loaded entry for the given key and marks a load in
// flight as not memoizable. The proposer calls it when its handler returns,
// which covers proposals that never applied; the FSM's commit path uses
// Fence and Unfence instead.
func (al *AttributeLoader[T]) Release(key attributes.U128) {
	s := al.shard(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.dropLocked(key)
}

// Fence drops the loaded entry for the key and blocks memoization of any load
// that completes before the matching Unfence. The FSM fences every key a
// proposal covers immediately before committing the batch that writes it:
// from then until the commit is visible, a load may read the store before or
// after the write, so nothing it returns may be memoized.
func (al *AttributeLoader[T]) Fence(key attributes.U128) {
	s := al.shard(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.dropLocked(key)
	s.fenced[key]++
}

// Unfence lifts one Fence on the key once the commit that motivated it has
// returned (or failed); loads completing from now on read the committed
// store and memoize again.
func (al *AttributeLoader[T]) Unfence(key attributes.U128) {
	s := al.shard(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fenced[key] <= 1 {
		delete(s.fenced, key)

		return
	}

	s.fenced[key]--
}

// FenceAll drops every memo and blocks memoization of every load until the
// matching UnfenceAll. The FSM brackets with it the commit of a batch that
// range-deletes keys no plan enumerates (a ledger deletion).
func (al *AttributeLoader[T]) FenceAll() {
	for i := range al.shards {
		s := &al.shards[i]

		s.mu.Lock()
		clear(s.loaded)

		for _, inflight := range s.loading {
			inflight.released = true
		}

		s.fencedAll++
		s.mu.Unlock()
	}
}

// UnfenceAll lifts one FenceAll once the commit that motivated it has
// returned (or failed).
func (al *AttributeLoader[T]) UnfenceAll() {
	for i := range al.shards {
		s := &al.shards[i]

		s.mu.Lock()

		if s.fencedAll > 0 {
			s.fencedAll--
		}

		s.mu.Unlock()
	}
}

// dropLocked forgets the key's memo and marks any load in flight as not
// memoizable. Caller holds s.mu.
func (s *loaderShard[T]) dropLocked(key attributes.U128) {
	delete(s.loaded, key)

	if inflight, ok := s.loading[key]; ok {
		inflight.released = true
	}
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
	Indexes           *AttributeLoader[*commonpb.Index]
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
		Indexes:           NewAttributeLoader[*commonpb.Index](),
	}
}

// LoaderOps is the non-generic interface satisfied by every AttributeLoader[T]:
// the operations CleanupToken and plan.Builder need.
type LoaderOps interface {
	Release(attributes.U128)
	Fence(attributes.U128)
	Unfence(attributes.U128)
	FenceAll()
	UnfenceAll()
}

// TrackedLoader pairs a loader with the keys that were loaded through it.
type TrackedLoader struct {
	Loader LoaderOps
	Keys   []attributes.U128
}

// CleanupToken tracks which keys were loaded for each attribute type.
// Used to clean up loaded entries after a command is applied. Tracked is
// exposed so callers in other packages (notably plan.Builder) can
// append entries directly; Release walks the slice in order.
type CleanupToken struct {
	Tracked []TrackedLoader
}

// Release cleans up all tracked keys from their respective loaders.
func (t *CleanupToken) Release() {
	for i := range t.Tracked {
		for _, key := range t.Tracked[i].Keys {
			t.Tracked[i].Loader.Release(key)
		}
	}
}
