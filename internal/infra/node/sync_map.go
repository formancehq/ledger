package node

import "sync"

type SyncMap[K comparable, V any] struct {
	m sync.Map
}

func (m *SyncMap[K, V]) Store(k K, v V) {
	m.m.Store(k, v)
}

func (m *SyncMap[K, V]) Delete(k K) {
	m.m.Delete(k)
}

func (m *SyncMap[K, V]) Load(id K) (V, bool) {
	v, ok := m.m.Load(id)
	if !ok {
		var zero V

		return zero, false
	}

	return v.(V), true
}

func (m *SyncMap[K, V]) LoadAndDelete(k K) (V, bool) {
	v, loaded := m.m.LoadAndDelete(k)
	if !loaded {
		var zero V

		return zero, false
	}

	return v.(V), true
}

func (m *SyncMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	v, loaded := m.m.LoadOrStore(key, value)

	return v.(V), loaded
}

func (m *SyncMap[K, V]) Range(f func(K, V) bool) {
	m.m.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}
