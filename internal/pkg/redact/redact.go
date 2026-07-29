package redact

import "strings"

const (
	SecretSet  = "(set)"
	SecretNone = "(none)"
)

// Secret replaces an opaque secret with a marker that only exposes whether a
// value is configured.
func Secret(value string) string {
	if value == "" {
		return SecretNone
	}

	return SecretSet
}

// DSN replaces the password in a URL-shaped DSN with "****" while preserving
// the username and host for operator diagnostics. Non-URL DSNs and DSNs without
// a password are returned unchanged.
func DSN(dsn string) string {
	schemeEnd := strings.Index(dsn, "://")
	if schemeEnd == -1 {
		return dsn
	}

	rest := dsn[schemeEnd+3:]
	lastAt := strings.LastIndex(rest, "@")
	if lastAt == -1 {
		return dsn
	}

	creds := rest[:lastAt]
	user, _, ok := strings.Cut(creds, ":")
	if !ok {
		return dsn
	}

	return dsn[:schemeEnd+3] + user + ":****" + rest[lastAt:]
}
