package kv

import "iter"

type KV[K any, V any] interface {
	Get(K) (V, bool)
	Put(K, V)
	Del(K)
	Size() uint64
}

type Map[K comparable, V any] map[K]V

func (d Map[K, V]) Get(i K) (V, bool) {
	v, ok := d[i]
	return v, ok
}

func (d Map[K, V]) Put(i K, a V) {
	d[i] = a
}

func (d Map[K, V]) Del(u128 K) {
	delete(d, u128)
}

func (d Map[K, V]) Size() uint64 {
	return uint64(len(d))
}

func (d Map[K, V]) Iter() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for u128, entry := range d {
			yield(u128, entry)
		}
	}
}

func NewMap[K comparable, V any]() Map[K, V] {
	return Map[K, V]{}
}
