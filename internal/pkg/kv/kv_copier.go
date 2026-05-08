package kv

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
