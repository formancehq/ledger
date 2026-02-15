package cache

import "github.com/formancehq/ledger-v3-poc/internal/service/attributes"

// IsGuaranteed is a generic helper that checks if a key will still be in a given
// AttributeCache at the specified future index.
func IsGuaranteed[T any](cache *AttributeCache[T], at uint64, keyBytes []byte) bool {
	// TODO: Cache the underlying hasher
	id, _ := attributes.MakeKey(attributes.DefaultKeys, keyBytes)
	return cache.IsGuaranteedInCache(at, id)
}
