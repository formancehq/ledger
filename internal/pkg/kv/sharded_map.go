package kv

import (
	"iter"
	"sync"
	"sync/atomic"
)

const numShards = 64

// shard is a single partition of a ShardedMap, holding its own lock and map.
type shard[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
	_  [64]byte // cache-line padding to prevent false sharing
}

// ShardedMap is a concurrent-safe KV store using 64 shards with per-shard RWMutex.
// No interface boxing on keys (unlike sync.Map), so there is zero heap allocation
// for value-type keys such as [16]byte.
// Readers (Get) take a shared RLock; writers (Put, Del) take an exclusive Lock.
// With 64 shards the probability of a reader being blocked by a writer is ~1.6%.
type ShardedMap[K comparable, V any] struct {
	shards [numShards]shard[K, V]
	hashFn func(K) uint64
	size   atomic.Int64
}

// NewShardedMap creates a ShardedMap with the given hash function.
// hashFn must return a well-distributed uint64 for shard selection.
func NewShardedMap[K comparable, V any](hashFn func(K) uint64) *ShardedMap[K, V] {
	sm := &ShardedMap[K, V]{
		hashFn: hashFn,
	}
	for i := range sm.shards {
		sm.shards[i].m = make(map[K]V)
	}

	return sm
}

func (s *ShardedMap[K, V]) shard(k K) *shard[K, V] {
	return &s.shards[s.hashFn(k)&(numShards-1)]
}

func (s *ShardedMap[K, V]) Get(k K) (V, bool) {
	sh := s.shard(k)
	sh.mu.RLock()
	v, ok := sh.m[k]
	sh.mu.RUnlock()

	return v, ok
}

func (s *ShardedMap[K, V]) Put(k K, v V) {
	sh := s.shard(k)
	sh.mu.Lock()
	_, exists := sh.m[k]
	sh.m[k] = v
	sh.mu.Unlock()

	if !exists {
		s.size.Add(1)
	}
}

func (s *ShardedMap[K, V]) Del(k K) {
	sh := s.shard(k)
	sh.mu.Lock()

	_, exists := sh.m[k]
	if exists {
		delete(sh.m, k)
	}
	sh.mu.Unlock()

	if exists {
		s.size.Add(-1)
	}
}

func (s *ShardedMap[K, V]) Size() uint64 {
	return uint64(s.size.Load())
}

// Iter iterates over all entries across all shards.
// Each shard is RLocked during its iteration. This is intended for
// infrequent operations like snapshot serialization, not the hot path.
func (s *ShardedMap[K, V]) Iter() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for i := range s.shards {
			sh := &s.shards[i]
			sh.mu.RLock()

			for k, v := range sh.m {
				if !yield(k, v) {
					sh.mu.RUnlock()

					return
				}
			}

			sh.mu.RUnlock()
		}
	}
}
