package sensitive

// HTTP userinfo without a password may itself be a reusable bearer token.
func redactHTTPURL(raw string, depth int) (string, bool) {
	return redactConnectionURL(raw, urlPolicy{
		schemes: []string{"http", "https"}, maskBareUserinfo: true,
	}, depth)
}
