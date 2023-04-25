package ledgerstore

func ptr[T any](t T) *T {
	return &t
}
