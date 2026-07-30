// Package redact provides deterministic, idempotent projections for values
// that may contain reusable credentials.
package redact

import (
	"net/url"
	"strings"
)

const (
	SecretSet  = "(set)"
	SecretNone = "(none)"
	urlMask    = "****"
)

// Secret replaces an opaque secret with a marker that only exposes whether a
// value is configured.
func Secret(value string) string {
	switch value {
	case "":
		return SecretNone
	case SecretSet, SecretNone:
		return value
	default:
		return SecretSet
	}
}

// DSN replaces credentials in a URL-shaped DSN while preserving non-secret
// connection details for operator diagnostics. Userinfo passwords and
// credential-bearing query parameters use "****". Invalid URL-shaped values
// fail closed to SecretSet; non-URL DSNs are returned unchanged.
func DSN(dsn string) string {
	if dsn == "" || !strings.Contains(dsn, "://") {
		return dsn
	}

	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Scheme == "" {
		return SecretSet
	}

	if parsed.User != nil {
		username := parsed.User.Username()
		password, hasPassword := parsed.User.Password()
		if hasPassword {
			if password == "" {
				parsed.User = url.UserPassword(username, "")
			} else {
				parsed.User = url.UserPassword(username, urlMask)
			}
		}
	}

	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return SecretSet
	}
	for key, values := range query {
		if !isCredentialQueryKey(key) {
			continue
		}

		for index, value := range values {
			if value != "" {
				values[index] = urlMask
			}
		}
	}
	parsed.RawQuery = query.Encode()

	return restoreURLMask(parsed.String())
}

// URLUserinfo replaces password or token userinfo in a URL with "****" while
// preserving its scheme, host, path, query and fragment. Invalid URLs fail
// closed to SecretSet.
func URLUserinfo(value string) string {
	if value == "" {
		return ""
	}

	parsed, err := url.Parse(value)
	if err != nil {
		return SecretSet
	}
	if parsed.User == nil {
		return value
	}

	username := parsed.User.Username()
	password, hasPassword := parsed.User.Password()
	switch {
	case hasPassword && password == "":
		parsed.User = url.UserPassword(username, "")
	case hasPassword:
		parsed.User = url.UserPassword(username, urlMask)
	default:
		parsed.User = url.User(urlMask)
	}

	return restoreURLMask(parsed.String())
}

func isCredentialQueryKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
	switch normalized {
	case "password", "passwd", "pass", "token", "access_token", "api_key", "secret", "client_secret":
		return true
	default:
		return false
	}
}

func restoreURLMask(value string) string {
	return strings.ReplaceAll(value, "%2A%2A%2A%2A", urlMask)
}
