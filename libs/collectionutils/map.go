package collectionutils

func Keys[K comparable, V any](m map[K]V) []K {
	ret := make([]K, 0)
	for k := range m {
		ret = append(ret, k)
	}
	return ret
}
