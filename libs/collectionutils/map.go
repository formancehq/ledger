package collectionutils

import "fmt"

func Keys[K comparable, V any](m map[K]V) []K {
	ret := make([]K, 0)
	for k := range m {
		ret = append(ret, k)
	}
	return ret
}

func ConvertMap[K comparable, FROM any, TO any](m map[K]FROM, mapper func(v FROM) TO) map[K]TO {
	ret := make(map[K]TO)
	for k, from := range m {
		ret[k] = mapper(from)
	}
	return ret
}

func MergeMaps[K comparable, V any](m1, m2 map[K]V) map[K]V {
	ret := make(map[K]V)
	if m1 != nil {
		for k, v := range m1 {
			ret[k] = v
		}
	}
	if m2 != nil {
		for k, v := range m2 {
			ret[k] = v
		}
	}
	return ret
}

func ToAny[V any](v V) any {
	return v
}

func ToPointer[V any](v V) *V {
	return &v
}

func ToFmtString[V any](v any) string {
	return fmt.Sprint(v)
}
