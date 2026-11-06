package sensitive

func redactClickHouse(raw string) string {
	// Parse comma-separated hosts as one authority. HTTP transport DSNs also
	// mask bare userinfo; native transport retains a non-secret username.
	if result, ok := redactConnectionURL(raw, urlPolicy{schemes: []string{"clickhouse"}}, 0); ok {
		return result
	}
	if result, ok := redactHTTPURL(raw, 0); ok {
		return result
	}

	return Marker
}
