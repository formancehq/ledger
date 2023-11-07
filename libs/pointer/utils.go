package pointer

func For[T any](t T) *T {
	return &t
}
