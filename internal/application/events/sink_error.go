package events

import (
	"errors"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// sinkErrorSanitizer belongs to the adapter, which knows its connection settings.
// It removes credentials before diagnostics reach logs or persisted SinkError.
// URL echoes lose userinfo and query values, preserving the destination.
// Passwords, tokens and recognized credential query options are also removed
// when echoed separately. Unrelated values invented by a remote server cannot
// be identified from local configuration; adapters must handle such errors at
// their source rather than assuming arbitrary remote text is safe.
type sinkErrorSanitizer struct {
	replacements           []string
	credentialReplacements []string
}

func newSinkErrorSanitizer(connectionURLs []string, credentials ...string) sinkErrorSanitizer {
	secrets := slices.Clone(credentials)
	for _, raw := range connectionURLs {
		// Trim whitespace: the NATS driver accepts leading/trailing spaces around
		// individual server addresses; url.Parse rejects them, so we trim before parsing.
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil {
			// Extract userinfo from malformed URLs (e.g. proxy URLs with invalid ports).
			// The driver may echo the full URL text in errors, so credentials must be
			// registered even when the URL cannot be fully parsed.
			secrets = append(secrets, rawUserinfoCredentials(strings.TrimSpace(raw))...)

			continue
		}
		if parsed.User != nil {
			if password, ok := parsed.User.Password(); ok {
				secrets = append(secrets, password)
			} else if parsed.Scheme == "nats" {
				// Only NATS places a token in the username position without a password.
				// Treating the username as a credential for other schemes (e.g. ClickHouse)
				// over-redacts unrelated diagnostics such as table names.
				secrets = append(secrets, parsed.User.Username())
			}
		}
		values, _ := url.ParseQuery(parsed.RawQuery) // Valid options still identify credentials if another option is malformed.
		for key, items := range values {
			switch strings.ToLower(strings.ReplaceAll(key, "-", "_")) {
			case "password", "passwd", "secret", "token", "api_key", "apikey", "access_token", "access_key", "client_secret", "authorization":
				secrets = append(secrets, items...)
			}
		}
	}
	pairs := make(map[string]string)
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		for _, variant := range []string{secret, url.QueryEscape(secret), url.PathEscape(secret)} {
			pairs[variant] = "[redacted]"
		}
	}
	sanitizer := sinkErrorSanitizer{credentialReplacements: sinkReplacementPairs(pairs)}
	for _, raw := range connectionURLs {
		sanitizer.addURL(pairs, raw)
	}
	sanitizer.replacements = sinkReplacementPairs(pairs)

	return sanitizer
}

// addURL also covers net/http's password-stripped and quoted URL diagnostics.
func (s sinkErrorSanitizer) addURL(pairs map[string]string, raw string) {
	if raw == "" {
		return
	}
	safeString := "[redacted connection URL]"
	parsed, err := url.Parse(raw)
	if err == nil {
		safe := *parsed
		safe.User = nil
		// Credential echoes outside userinfo/query must not bypass replacement when
		// the complete URL is replaced in one pass.
		replacer := strings.NewReplacer(s.credentialReplacements...)
		safe.Scheme = replacer.Replace(safe.Scheme)
		safe.Host = replacer.Replace(safe.Host)
		safe.Opaque = replacer.Replace(safe.Opaque)
		safe.Path = replacer.Replace(safe.Path)
		safe.RawPath = ""
		safe.Fragment = replacer.Replace(safe.Fragment)
		safe.RawFragment = ""
		values, queryErr := url.ParseQuery(parsed.RawQuery)
		safeValues := make(url.Values, len(values))
		for key, items := range values {
			for i := range items {
				items[i] = "[redacted]"
			}
			safeValues[replacer.Replace(key)] = items
		}
		safe.RawQuery = safeValues.Encode()
		if queryErr != nil {
			safe.RawQuery = "[redacted]"
		}
		safeString = safe.String()
		pairs[parsed.String()] = safeString
		pairs[parsed.Redacted()] = safeString
		pairs[strconv.Quote(parsed.Redacted())] = strconv.Quote(safeString)
	}
	pairs[raw] = safeString
	pairs[strconv.Quote(raw)] = strconv.Quote(safeString)
}

// rawQueryValues extracts raw (undecoded) values for a key from a raw query string.
// It returns the raw value bytes, useful when url.ParseQuery would silently drop
// pairs with malformed percent escapes, such as nested proxy URLs from drivers.
func rawQueryValues(rawQuery, key string) []string {
	var results []string
	for part := range strings.SplitSeq(rawQuery, "&") {
		if k, v, ok := strings.Cut(part, "="); ok && k == key && v != "" {
			results = append(results, v)
		}
	}

	return results
}

func sinkReplacementPairs(pairs map[string]string) []string {
	keys := make([]string, 0, len(pairs))
	for key := range pairs {
		if key != "" {
			keys = append(keys, key)
		}
	}
	slices.SortFunc(keys, func(a, b string) int {
		if len(a) != len(b) {
			return len(b) - len(a)
		}

		return strings.Compare(a, b)
	})
	replacements := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		replacements = append(replacements, key, pairs[key])
	}

	return replacements
}

func (s sinkErrorSanitizer) sanitize(err error) error {
	if err == nil {
		return nil
	}
	replacements := s.replacements
	if urlErr, ok := errors.AsType[*url.Error](err); ok {
		// Redirects can change the URL: sanitize the URL reported by the transport,
		// not only the original configured endpoint.
		pairs := make(map[string]string, len(replacements)/2+2)
		for i := 0; i < len(replacements); i += 2 {
			pairs[replacements[i]] = replacements[i+1]
		}
		s.addURL(pairs, urlErr.URL)
		replacements = sinkReplacementPairs(pairs)
	}
	if len(replacements) == 0 {
		return err
	}
	// A single replacement pass never modifies inserted markers or safe URLs.
	message := strings.NewReplacer(replacements...).Replace(err.Error())
	if message == err.Error() {
		return err
	}

	return &sinkDiagnosticError{message: message, cause: err}
}

func (s sinkErrorSanitizer) sanitizeReturned(err *error) { *err = s.sanitize(*err) }

type sinkDiagnosticError struct {
	message string
	cause   error
}

func (e *sinkDiagnosticError) Error() string { return e.message }
func (e *sinkDiagnosticError) Unwrap() error { return e.cause }

// rawUserinfoCredentials extracts the password and, for NATS scheme, the username
// from a URL string that url.Parse refuses to parse (e.g. due to an invalid port).
// It uses conservative string splitting: extract the fragment between "://" and "@host",
// registering only the password or NATS token so other scheme usernames are not over-redacted.
func rawUserinfoCredentials(raw string) []string {
	// Find the authority portion: everything after "://" up to the next "/" or end.
	afterScheme, hasScheme := strings.CutPrefix(raw, strings.SplitN(raw, "://", 2)[0]+"://")
	if !hasScheme {
		return nil
	}
	isNATS := strings.HasPrefix(strings.ToLower(raw), "nats://")
	// Authority ends at the first "/" (path).
	authority, _, _ := strings.Cut(afterScheme, "/")
	// Userinfo is the part before the last "@".
	atIdx := strings.LastIndex(authority, "@")
	if atIdx < 0 {
		return nil
	}
	userinfo := authority[:atIdx]
	// Split into username:password.
	if _, password, ok := strings.Cut(userinfo, ":"); ok {
		if password != "" {
			return []string{password}
		}
	} else if isNATS && userinfo != "" {
		// NATS token in username position, no password.
		return []string{userinfo}
	}

	return nil
}
