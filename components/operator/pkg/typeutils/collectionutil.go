package typeutils

import (
	"encoding/json"
)

// +kubebuilder:object:generate=false
type Array[T any] []T

func (f Array[T]) Filter(checkFn func(t T) bool) []T {
	ret := make([]T, 0)
	for _, item := range f {
		if checkFn(item) {
			ret = append(ret, item)
		}
	}
	return ret
}

func (f Array[T]) First(checkFn func(t T) bool) *T {
	ret := f.Filter(checkFn)
	if len(ret) > 0 {
		return &ret[0]
	}
	return nil
}

func (f *Array[T]) Append(t T) {
	*f = append(*f, t)
}

func Filter[T any](items []T, checkFn func(t T) bool) []T {
	return Array[T](items).Filter(checkFn)
}

func First[T any](items []T, checkFn func(t T) bool) *T {
	return Array[T](items).First(checkFn)
}

func NotIn[T comparable](items ...T) func(t T) bool {
	return func(t T) bool {
		for _, item := range items {
			if item == t {
				return false
			}
		}
		return true
	}
}

func Equal[T comparable](value T) func(t T) bool {
	return func(t T) bool {
		return t == value
	}
}

func NotEqual[T comparable](value T) func(t T) bool {
	return func(t T) bool {
		return t != value
	}
}

func MergeAll[T comparable](arrays ...[]T) []T {
	ret := make([]T, 0)
	for _, a := range arrays {
		ret = append(ret, a...)
	}
	return ret
}

func CreateMap(args ...string) map[string]string {
	if len(args)%2 != 0 {
		panic("odd number of args pass to maputil.Create()")
	}
	ret := make(map[string]string)
	for i := 0; i < len(args); i += 2 {
		ret[args[i]] = args[i+1]
	}
	return ret
}

func Map[T1 any, T2 any](v1 []T1, transformer func(T1) T2) []T2 {
	ret := make([]T2, 0)
	for _, v := range v1 {
		ret = append(ret, transformer(v))
	}
	return ret
}

func MergeMaps[K comparable, V any](maps ...map[K]V) map[K]V {
	ret := make(map[K]V)
	for _, aMap := range maps {
		if aMap == nil {
			continue
		}
		for k, v := range aMap {
			ret[k] = v
		}
	}
	return ret
}

func SliceFromMap[K comparable, V any](m map[K]V) []V {
	ret := make([]V, 0)
	for _, v := range m {
		ret = append(ret, v)
	}
	return ret
}

func MapObject(src, dst any) {
	data, err := json.Marshal(src)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		panic(err)
	}
}

func CopyMap[K comparable, V any](m map[K]V) map[K]V {
	ret := make(map[K]V)
	for key, value := range m {
		ret[key] = value
	}
	return ret
}
