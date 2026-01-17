package raft

import "sync"

type SyncMap[K comparable, V any] struct {
	m sync.Map
}

func (m *SyncMap[K, V]) Store(k K, v V) {
	m.m.Store( k, v)
}

func (m *SyncMap[K, V]) Delete(k K) {
	m.m.Delete(k)
}

func (m *SyncMap[K, V]) Load(id uint64) (V, bool) {
	v, ok := m.m.Load(id)
	if !ok {
		var zero V
		return zero, false
	}

	return v.(V), true
}



