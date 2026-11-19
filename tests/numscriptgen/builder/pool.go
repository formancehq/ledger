package builder

type pool[T comparable] struct {
	nextId int
	elems  map[T]int
}

func newPool[T comparable]() pool[T] {
	return pool[T]{
		elems: make(map[T]int),
	}
}
