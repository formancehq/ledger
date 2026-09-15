package v2

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

type rejectingMirrorTransport struct {
	t *testing.T
}

func (r rejectingMirrorTransport) RoundTrip(*http.Request) (*http.Response, error) {
	r.t.Error("malformed mirror URL reached the network")

	return nil, errors.New("unexpected network request")
}

func TestHTTPSource_MalformedURLDiagnostics(t *testing.T) {
	t.Parallel()
	const password = "AUDIT_MIRROR_PASS_52c91"
	for _, tc := range []struct {
		name, baseURL, diagnostic string
	}{
		{"escaped path", "https://audit-user:" + password + "@localhost/%zz", "invalid URL escape; check percent-encoding"},
		{"control character", "https://audit-user:" + password + "\n@localhost", "control character in URL"},
		// Dropping only url.Error.URL still exposes this invalid port in its cause.
		{"invalid port", "https://audit-user:" + password + "@localhost:" + password, "invalid port; use a numeric port after ':'"},
		{"escaped userinfo", "https://audit-user:" + password + "%zz@localhost", "invalid URL escape; check percent-encoding"},
		{"invalid userinfo", "https://audit-user:" + password + "|@localhost", "invalid userinfo; percent-encode special characters in credentials"},
		{"invalid host", "https://audit-user:" + password + "@local host", "invalid character in host"},
		{"unclosed IPv6", "https://audit-user:" + password + "@[::1", "missing ']' in host; enclose IPv6 addresses in brackets"},
		{"invalid IPv6", "https://audit-user:" + password + "@[" + password + "]", "invalid IP-literal; check the bracketed IPv6 address"},
		{"bracketed IPv4", "https://audit-user:" + password + "@[127.0.0.1]", "invalid IP-literal; check the bracketed IPv6 address"},
		{"missing scheme", "://audit-user:" + password + "@localhost", "missing protocol scheme"},
		{"invalid scheme", "1https://audit-user:" + password + "@localhost", "first path segment cannot contain ':'; check the URL scheme"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := NewHTTPSource(tc.baseURL, "source-ledger", &http.Client{Transport: rejectingMirrorTransport{t: t}})
			t.Cleanup(func() { require.NoError(t, source.Close()) })
			_, headErr := source.GetLatestLogID(context.Background())
			logs, more, batchErr := source.FetchLogs(context.Background(), 0, 100)
			require.Empty(t, logs)
			require.False(t, more)
			for _, err := range []error{headErr, batchErr} {
				require.EqualError(t, err, "parsing URL: "+tc.diagnostic)
				for cause := err; cause != nil; cause = errors.Unwrap(cause) {
					require.NotContains(t, cause.Error(), password)
					require.NotContains(t, cause.Error(), tc.baseURL)
				}
			}
		})
	}
}

// Unknown parser failures must never fall back to formatting their raw cause.
func TestHTTPSource_UnknownURLParseDiagnostic(t *testing.T) {
	t.Parallel()
	const secret = "AUDIT_UNKNOWN_PARSE_SECRET"
	for _, cause := range []error{errors.New(secret), &url.Error{Op: "parse", URL: secret, Err: errors.New(secret)}} {
		err := safeHTTPURLParseError(cause)
		require.EqualError(t, err, "parsing URL: invalid mirror source URL")
		require.Nil(t, errors.Unwrap(err))
	}
}
