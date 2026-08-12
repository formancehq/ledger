package ledger

import "sync/atomic"

func (store *Store) ResetIndexedMetadataKeysForTest() {
	store.indexedKeysResolved = false
	store.indexedMetadataKeys = nil
	store.indexedMetadataKeysUnscoped = nil
}

// SetAloneInBucketForTest forces the alone-in-bucket hint, allocating the shared
// flag when the store was built without going through the factory.
func (store *Store) SetAloneInBucketForTest(alone bool) {
	if store.aloneInBucket == nil {
		store.aloneInBucket = &atomic.Bool{}
	}
	store.aloneInBucket.Store(alone)
}
