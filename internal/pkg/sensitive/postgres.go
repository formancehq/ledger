package sensitive

func redactPostgres(raw string) string {
	if result, ok := redactConnectionURL(raw, urlPolicy{
		schemes: []string{"postgres", "postgresql"}, allowEmptyHost: true,
	}, 0); ok {
		return result
	}

	return Marker
}
