package preload

import (
	"sync"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// loadedEntry stores a loaded attribute value with its boundary.
type loadedEntry[T any] struct {
	boundary uint64
	value    T
}

// AttributeLoader coordinates loading of attributes to prevent duplicate loads from store.
// It uses per-key locks to ensure only one goroutine loads a given attribute at a time.
// T is the type of the loaded value.
//
// Uses RWMutex for optimization:
// - RLock for reading cached values (fast path, allows concurrent reads)
// - Lock for modifications (adding to loading/loaded, deleting).
type AttributeLoader[T any] struct {
	mu sync.RWMutex
	// loading tracks keys currently being loaded (value is a channel closed when done)
	loading map[attributes.U128]chan struct{}
	// loaded tracks keys that have been loaded with their boundary and value
	loaded map[attributes.U128]*loadedEntry[T]
}

// LoadResult represents the result of loading an attribute.
type LoadResult[T any] struct {
	Value    T
	FromLoad bool // true if we actually loaded from store, false if from loader cache
}

// NewAttributeLoader creates a new AttributeLoader for the given type.
func NewAttributeLoader[T any]() *AttributeLoader[T] {
	return &AttributeLoader[T]{
		loading: make(map[attributes.U128]chan struct{}),
		loaded:  make(map[attributes.U128]*loadedEntry[T]),
	}
}

// LoadOrWait loads an attribute value or waits for an ongoing load.
// It returns the value and whether we actually performed a load (vs using cached).
// The loadFn is called only if the value needs to be loaded from store.
func (al *AttributeLoader[T]) LoadOrWait(key attributes.U128, boundary uint64, loadFn func() (T, error)) (*LoadResult[T], error) {
	// Fast path: check if already loaded using read lock
	al.mu.RLock()

	if cached, ok := al.loaded[key]; ok && cached.boundary >= boundary {
		al.mu.RUnlock()

		return &LoadResult[T]{Value: cached.value, FromLoad: false}, nil
	}
	// Check if someone is already loading this key
	waitCh, isLoading := al.loading[key]
	al.mu.RUnlock()

	if isLoading {
		// Wait for the ongoing load to complete
		<-waitCh
		// Re-check with read lock
		al.mu.RLock()

		if cached, ok := al.loaded[key]; ok && cached.boundary >= boundary {
			al.mu.RUnlock()

			return &LoadResult[T]{Value: cached.value, FromLoad: false}, nil
		}

		al.mu.RUnlock()
		// Load failed or boundary mismatch - fall through to try loading ourselves
	}

	// Slow path: need to load - acquire write lock
	al.mu.Lock()

	// Double-check after acquiring write lock (another goroutine might have loaded it)
	if cached, ok := al.loaded[key]; ok && cached.boundary >= boundary {
		al.mu.Unlock()

		return &LoadResult[T]{Value: cached.value, FromLoad: false}, nil
	}

	// Check again if someone started loading while we were waiting for the lock
	if waitCh, ok := al.loading[key]; ok {
		al.mu.Unlock()
		// Wait and retry from the beginning
		<-waitCh

		return al.LoadOrWait(key, boundary, loadFn)
	}

	// We're the one who will load - mark as loading
	waitCh = make(chan struct{})
	al.loading[key] = waitCh
	al.mu.Unlock()

	// Perform the actual load (outside of lock)
	value, err := loadFn()

	// Update state with write lock
	al.mu.Lock()
	delete(al.loading, key)

	if err == nil {
		al.loaded[key] = &loadedEntry[T]{boundary: boundary, value: value}
	}

	close(waitCh)
	al.mu.Unlock()

	if err != nil {
		var zero T

		return &LoadResult[T]{Value: zero, FromLoad: false}, err
	}

	return &LoadResult[T]{Value: value, FromLoad: true}, nil
}

// Release removes the loaded entry for the given key.
// This should be called after the command has been applied and the cache updated.
func (al *AttributeLoader[T]) Release(key attributes.U128) {
	al.mu.Lock()
	defer al.mu.Unlock()

	delete(al.loaded, key)
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
}
