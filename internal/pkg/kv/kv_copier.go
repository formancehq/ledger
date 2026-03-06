package kv

import (
	"iter"
)

type Copier[K comparable, V any] struct {
	sources         []KV[K, V]
	originalSources KV[K, V]
	updated         Map[K, V]
	merged          bool
	copyFunc        func(V) V
}

func (m *Copier[K, V]) Get(k K) (V, bool) {
	v, ok := m.updated.Get(k)
	if ok {
		return v, true
	}

	for _, src := range m.sources {
		val, ok := src.Get(k)
		if !ok {
			continue // Try next source
		}
		// Deep copy to avoid sharing pointers
		clone := m.copyFunc(val)
		m.originalSources.Put(k, val)
		m.updated.Put(k, clone)

		return clone, true
	}

	var zero V

	return zero, false
}

func (m *Copier[K, V]) LoadOrInit(k K, init func() V) V {
	v, ok := m.Get(k)
	if !ok {
		v = init()
		m.updated.Put(k, v)
	}

	return v
}

func (m *Copier[K, V]) Put(k K, v V) {
	if m.merged {
		panic("already merged")
	}

	m.updated.Put(k, v)
}

func (m *Copier[K, V]) Merge() KV[K, V] {
	if m.merged {
		return m.sources[0]
	}

	ret := m.sources[0]
	for k, v := range m.updated.Iter() {
		ret.Put(k, v)
	}

	m.updated = nil

	return ret
}

func (m *Copier[K, V]) Updates() []CopierUpdate[K, V] {
	if m.merged {
		panic("already merged")
	}

	ret := make([]CopierUpdate[K, V], 0, m.updated.Size())
	for k, v := range m.updated.Iter() {
		old, isDefined := m.originalSources.Get(k)
		ret = append(ret, CopierUpdate[K, V]{
			Old: Optional[V]{
				value:     old,
				isDefined: isDefined,
			},
			New: v,
			Key: k,
		})
	}

	return ret
}

func (m *Copier[K, V]) Del(key K) {
	var zero V
	m.Put(key, zero)
}

func (m *Copier[K, V]) Size() uint64 {
	return m.updated.Size()
}

func (m *Copier[K, V]) Iter() iter.Seq2[K, V] {
	return m.updated.Iter()
}

func NewCopier[K comparable, V any](copyFunc func(V) V, sources ...KV[K, V]) *Copier[K, V] {
	return &Copier[K, V]{
		sources:         sources,
		originalSources: NewMap[K, V](),
		updated:         NewMap[K, V](),
		copyFunc:        copyFunc,
	}
}

var _ KV[any, any] = (*Copier[any, any])(nil)

type CopierUpdate[K comparable, V any] struct {
	Key K
	Old Optional[V]
	New V
}

type Optional[T any] struct {
	value     T
	isDefined bool
}

func (o *Optional[T]) Value() T {
	return o.value
}

func (o *Optional[T]) SetValue(v T) {
	o.value = v
}

func (o *Optional[T]) IsDefined() bool {
	return o.isDefined
}

func None[T any]() Optional[T] {
	return Optional[T]{}
}

func Some[T any](v T) Optional[T] {
	return Optional[T]{
		value:     v,
		isDefined: true,
	}
}
