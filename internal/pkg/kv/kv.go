package kv

import (
	"iter"
)

type KV[K any, V any] interface {
	Get(K) (V, bool)
	Put(K, V)
	Del(K)
	Size() uint64
	Iter() iter.Seq2[K, V]
}
