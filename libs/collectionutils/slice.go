package collectionutils

import (
	"reflect"
)

func Map[FROM any, TO any](input []FROM, mapper func(FROM) TO) []TO {
	ret := make([]TO, len(input))
	for i, input := range input {
		ret[i] = mapper(input)
	}
	return ret
}

func CopyMap[KEY comparable, VALUE any](m map[KEY]VALUE) map[KEY]VALUE {
	ret := make(map[KEY]VALUE)
	for k, v := range m {
		ret[k] = v
	}
	return ret
}

func Filter[TYPE any](input []TYPE, filter func(TYPE) bool) []TYPE {
	ret := make([]TYPE, 0)
	for _, i := range input {
		if filter(i) {
			ret = append(ret, i)
		}
	}
	return ret
}

func Flatten[TYPE any](input [][]TYPE) []TYPE {
	ret := make([]TYPE, 0)
	for _, types := range input {
		ret = append(ret, types...)
	}
	return ret
}

func First[TYPE any](input []TYPE, filter func(TYPE) bool) TYPE {
	var zero TYPE
	ret := Filter(input, filter)
	if len(ret) >= 1 {
		return ret[0]
	}
	return zero
}

func FilterEq[T any](t T) func(T) bool {
	return func(t2 T) bool {
		return reflect.DeepEqual(t, t2)
	}
}

func FilterNot[T any](t func(T) bool) func(T) bool {
	return func(t2 T) bool {
		return !t(t2)
	}
}

func Contains[T any](slice []T, t T) bool {
	for _, t2 := range slice {
		if reflect.DeepEqual(t, t2) {
			return true
		}
	}
	return false
}

type Set[T comparable] map[T]struct{}

func (s Set[T]) Put(t T) {
	s[t] = struct{}{}
}

func (s Set[T]) Contains(t T) bool {
	_, ok := s[t]
	return ok
}

func NewSet[T comparable]() Set[T] {
	return make(Set[T], 0)
}
