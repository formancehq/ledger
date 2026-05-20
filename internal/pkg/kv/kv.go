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
	Del(K)
	Size() uint64
	Iter() iter.Seq2[K, V]
}
