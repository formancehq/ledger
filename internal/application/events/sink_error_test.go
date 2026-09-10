package events

import (
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSinkErrorSanitizer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		urls        []string
		credentials []string
		diagnostic  string
	}{
		{name: "HTTP", urls: []string{"https://alice:password-value@example.com/events?api_key=query-secret"}, diagnostic: "Post https://alice:password-value@example.com/events?api_key=query-secret: connection refused"},
		{name: "NATS token", urls: []string{"nats://token-secret@example.com"}, diagnostic: "example.com authentication failed for token-secret: connection refused"},
		{name: "Kafka", credentials: []string{"sasl-secret"}, diagnostic: "broker example.com rejected sasl-secret: connection refused"},
		{name: "ClickHouse", urls: []string{"clickhouse://alice:password-value@example.com/default?password=query-secret"}, diagnostic: "example.com rejected password-value and query-secret: connection refused"},
		{name: "Databricks", credentials: []string{"pat-secret", "oauth-secret"}, diagnostic: "example.com rejected pat-secret and oauth-secret: connection refused"},
		{name: "escaped", credentials: []string{"secret /+value"}, diagnostic: "example.com rejected " + url.QueryEscape("secret /+value") + " and " + url.PathEscape("secret /+value") + ": connection refused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cause := errors.New(tc.diagnostic)
			err := newSinkErrorSanitizer(tc.urls, tc.credentials...).sanitize(fmt.Errorf("delivering event: %w", cause))
			require.ErrorIs(t, err, cause)
			require.Contains(t, err.Error(), "delivering event")
			require.Contains(t, err.Error(), "connection refused")
			require.Contains(t, err.Error(), "example.com")
			for _, secret := range []string{"password-value", "query-secret", "token-secret", "sasl-secret", "pat-secret", "oauth-secret", url.QueryEscape("secret /+value"), url.PathEscape("secret /+value")} {
				require.NotContains(t, err.Error(), secret)
			}
		})
	}
}

func TestSinkErrorSanitizerPreservesUnchangedErrors(t *testing.T) {
	t.Parallel()
	sanitizer := newSinkErrorSanitizer(nil, "credential")
	require.NoError(t, sanitizer.sanitize(nil))
	cause := errors.New("connection refused")
	require.Same(t, cause, sanitizer.sanitize(cause))
}

func TestSinkErrorSanitizerURLStructureAndOverlappingSecrets(t *testing.T) {
	t.Parallel()
	sanitizer := newSinkErrorSanitizer([]string{"https://alice:z@example.com/events?compress=1&api_key=abcdef"}, "abc", "abcdef")
	err := sanitizer.sanitize(errors.New("Post https://alice:z@example.com/events?compress=1&api_key=abcdef: attempt 1 connection refused abc abcdef"))
	require.Equal(t, "Post https://example.com/events?api_key=%5Bredacted%5D&compress=%5Bredacted%5D: attempt 1 connection refused [redacted] [redacted]", err.Error())
}

func TestSinkErrorSanitizerMalformedURL(t *testing.T) {
	t.Parallel()
	raw := "https://alice:password@example.com/%zz?api_key=secret"
	err := newSinkErrorSanitizer([]string{raw}).sanitize(fmt.Errorf("parsing %q: invalid URL escape", raw))
	require.Equal(t, `parsing "[redacted connection URL]": invalid URL escape`, err.Error())
}

func TestSinkErrorSanitizerCredentialsInURLComponents(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{
		"https://alice:token-secret@example.com/token-secret#token-secret",
		"https://token-secret.example.com/events?token-secret=value",
		"https:token-secret",
	} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			err := newSinkErrorSanitizer([]string{raw}, "token-secret").sanitize(fmt.Errorf("Post %s: EOF", raw))
			require.NotContains(t, err.Error(), "token-secret")
			require.Contains(t, err.Error(), "EOF")
		})
	}
}
