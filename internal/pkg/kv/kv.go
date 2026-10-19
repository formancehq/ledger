package kv

import (
	"iter"
)

type KV[K any, V any] interface {
	Get(K) (V, bool)
	// Put returns the previous value, whether it existed, and whether the write
	// was accepted. A rejected write leaves the store unchanged.
	Put(K, V) (old V, existed, written bool)
	Size() uint64
	Iter() iter.Seq2[K, V]
}
