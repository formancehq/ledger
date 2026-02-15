package kv

import (
	"iter"
	"sync"
	"sync/atomic"
)

type KV[K any, V any] interface {
	Get(K) (V, bool)
	Put(K, V)
	Del(K)
	Size() uint64
	Iter() iter.Seq2[K, V]
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

// SyncMap is a concurrent-safe KV store backed by sync.Map.
// Reads are lock-free, making it ideal for high-contention scenarios
// with many concurrent readers and infrequent writers.
type SyncMap[K comparable, V any] struct {
	m    sync.Map
	size atomic.Int64
}

func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{}
}

func (s *SyncMap[K, V]) Get(k K) (V, bool) {
	v, ok := s.m.Load(k)
	if !ok {
		var zero V
		return zero, false
	}
	return v.(V), true
}

func (s *SyncMap[K, V]) Put(k K, v V) {
	_, loaded := s.m.Swap(k, v)
	if !loaded {
		s.size.Add(1)
	}
}

func (s *SyncMap[K, V]) Del(k K) {
	_, loaded := s.m.LoadAndDelete(k)
	if loaded {
		s.size.Add(-1)
	}
}

func (s *SyncMap[K, V]) Size() uint64 {
	return uint64(s.size.Load())
}

func (s *SyncMap[K, V]) Iter() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		s.m.Range(func(key, value any) bool {
			return yield(key.(K), value.(V))
		})
	}
}
