package kv

import (
	"iter"
)

type KV[K any, V any] interface {
	Get(K) (V, bool)
	Put(K, V)
	// GetAndPut atomically reads the old value and writes the new one.
	// Returns the old value and whether it existed.
	GetAndPut(K, V) (V, bool)
	// Del removes K from the store. Returns an error when the implementation
	// can detect the key is genuinely absent (e.g. the dual-generation
	// AttributeCache returns domain.ErrNotFound when both Gen0 and Gen1 miss,
	// so callers can treat delete-on-absent as an idempotent no-op).
	// Simple implementations (ShardedMap) always return nil.
	Del(K) error
	Size() uint64
	Iter() iter.Seq2[K, V]
}
