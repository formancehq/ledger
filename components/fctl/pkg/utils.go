package fctl

func Map[SRC any, DST any](srcs []SRC, mapper func(SRC) DST) []DST {
	ret := make([]DST, 0)
	for _, src := range srcs {
		ret = append(ret, mapper(src))
	}
	return ret
}

func MapKeys[K comparable, V any](m map[K]V) []K {
	ret := make([]K, 0)
	for k := range m {
		ret = append(ret, k)
	}
	return ret
}

func Prepend[V any](array []V, items ...V) []V {
	return append(items, array...)
}
