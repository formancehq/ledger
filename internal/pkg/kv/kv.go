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
	// can detect a contract violation (e.g. the dual-generation cache rejects
	// a Del on a key that is not in the primary generation — invariant #6:
	// every FSM-apply Delete is backed by a preload that places the entry in
	// Gen0). Simple implementations (ShardedMap) always return nil.
	Del(K) error
	Size() uint64
	Iter() iter.Seq2[K, V]
}
