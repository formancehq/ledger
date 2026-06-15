package state

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/kv"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// CacheAwareEntry bundles a KeyStore, its Pebble Attribute, and the 0xFF cache
// type byte into a single type. This is the ONLY sanctioned way to access a
// KeyStore from Machine/technical updates — direct KeyStore.Put() is hidden
// behind KeyStore() which is reserved for DerivedKeyStore construction and tests.
//
// Methods:
//   - Get / GetKey: read from the in-memory cache (delegates to KeyStore)
//   - PutWithCache: atomically writes to KeyStore + 0xF1 (Attribute) + 0xFF (cache zone)
//   - PutCacheOnly: writes to KeyStore + 0xFF only (no 0xF1), for ephemeral purge pattern
//   - KeyStore: returns the inner KeyStore for DerivedKeyStore overlay construction
//   - Attr / CacheType: expose internals for the WriteSet.Merge() pipeline
type CacheAwareEntry[K attributes.Key, V proto.Message] struct {
	store     *attributes.KeyStore[K, V]
	attr      *attributes.Attribute[V]
	cacheType byte
}

// NewCacheAwareEntry pairs a KeyStore with its Attribute and cache type byte.
func NewCacheAwareEntry[K attributes.Key, V proto.Message](
	store *attributes.KeyStore[K, V],
	attr *attributes.Attribute[V],
	cacheType byte,
) *CacheAwareEntry[K, V] {
	return &CacheAwareEntry[K, V]{
		store:     store,
		attr:      attr,
		cacheType: cacheType,
	}
}

// Get retrieves a value by canonical key bytes (delegates to KeyStore.Get).
func (c *CacheAwareEntry[K, V]) Get(canonical []byte) (V, attributes.U128, error) {
	return c.store.Get(canonical)
}

// GetKey retrieves a value by typed key (delegates to KeyStore.GetKey).
func (c *CacheAwareEntry[K, V]) GetKey(key K) (V, attributes.U128, error) {
	return c.store.GetKey(key)
}

// PutWithCache atomically writes a value to the in-memory KeyStore, the 0xF1
// attribute zone, and the 0xFF cache zone within the same Pebble batch.
// This is the safe replacement for direct KeyStore.Put() calls.
func (c *CacheAwareEntry[K, V]) PutWithCache(
	batch *dal.WriteSession,
	genByte byte,
	canonical []byte,
	value V,
) (kv.Optional[V], attributes.IDWithTag, error) {
	old, idWithTag, err := c.store.Put(canonical, value)
	if err != nil {
		return old, idWithTag, fmt.Errorf("keystore put: %w", err)
	}

	valueBytes, err := c.attr.Set(batch, canonical, value)
	if err != nil {
		return old, idWithTag, fmt.Errorf("attribute set (0xF1): %w", err)
	}

	if err := writeCacheRaw(batch, genByte, c.cacheType, idWithTag.ID, idWithTag.Tag, valueBytes); err != nil {
		return old, idWithTag, fmt.Errorf("cache write (0xFF): %w", err)
	}

	return old, idWithTag, nil
}

// PutCacheOnly writes a value to the in-memory KeyStore and the 0xFF cache zone
// WITHOUT writing to the 0xF1 attribute zone. Used for the ephemeral purge
// pattern where the 0xF1 entry is deleted separately but the cache must stay
// populated for co-batched CacheGuaranteed proposals.
func (c *CacheAwareEntry[K, V]) PutCacheOnly(
	batch *dal.WriteSession,
	genByte byte,
	canonical []byte,
	value V,
	valueBytes []byte,
) error {
	_, idWithTag, err := c.store.Put(canonical, value)
	if err != nil {
		return fmt.Errorf("keystore put: %w", err)
	}

	if err := writeCacheRaw(batch, genByte, c.cacheType, idWithTag.ID, idWithTag.Tag, valueBytes); err != nil {
		return fmt.Errorf("cache write (0xFF): %w", err)
	}

	return nil
}

// KeyStore returns the inner KeyStore. Reserved for:
//   - DerivedKeyStore construction (NewDerivedKeyStore needs *KeyStore)
//   - Parent().M.Iter() access in GetLedgerByID
//   - Test setup that populates the store without needing 0xFF consistency
//
// Production code outside the DerivedKeyStore→Merge pipeline must NOT call
// KeyStore().Put() — use PutWithCache or PutCacheOnly instead.
func (c *CacheAwareEntry[K, V]) KeyStore() *attributes.KeyStore[K, V] {
	return c.store
}

// Attr returns the inner Attribute for the WriteSet.Merge() pipeline.
// Used by mergeAndTrackBloom and mergeSimpleWithCache.
func (c *CacheAwareEntry[K, V]) Attr() *attributes.Attribute[V] {
	return c.attr
}

// CacheType returns the 0xFF cache sub-attribute byte.
// Used by mergeAndTrackBloom for writeCacheTombstone calls.
func (c *CacheAwareEntry[K, V]) CacheType() byte {
	return c.cacheType
}
