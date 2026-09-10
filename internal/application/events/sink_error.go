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
	replacements []string
	credentials  []string
}

func newSinkErrorSanitizer(connectionURLs []string, credentials ...string) sinkErrorSanitizer {
	secrets := slices.Clone(credentials)
	for _, raw := range connectionURLs {
		parsed, err := url.Parse(raw)
		if err != nil {
			continue
		}
		if parsed.User != nil {
			if password, ok := parsed.User.Password(); ok {
				secrets = append(secrets, password)
			} else {
				// NATS supports a token in the username position.
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
	sanitizer := sinkErrorSanitizer{credentials: sinkReplacementPairs(pairs)}
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
		replacer := strings.NewReplacer(s.credentials...)
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

func (s sinkErrorSanitizer) finish(err *error) { *err = s.sanitize(*err) }

type sinkDiagnosticError struct {
	message string
	cause   error
}

func (e *sinkDiagnosticError) Error() string { return e.message }
func (e *sinkDiagnosticError) Unwrap() error { return e.cause }
