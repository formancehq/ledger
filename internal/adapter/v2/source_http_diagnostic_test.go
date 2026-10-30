package v2

import (
	"context"
	"errors"
	"fmt"
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
	for name, baseURL := range map[string]string{
		"escaped path":      "https://audit-user:" + password + "@localhost/%zz",
		"control character": "https://audit-user:" + password + "\n@localhost",
		// The underlying parser error can echo an invalid port, so dropping
		// only url.Error.URL would still disclose sensitive input.
		"invalid port":     "https://audit-user:" + password + "@localhost:" + password,
		"invalid userinfo": "https://audit-user:" + password + "%zz@localhost",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source := NewHTTPSource(baseURL, "source-ledger", &http.Client{Transport: rejectingMirrorTransport{t: t}})
			t.Cleanup(func() { require.NoError(t, source.Close()) })
			_, headErr := source.GetLatestLogID(context.Background())
			logs, more, batchErr := source.FetchLogs(context.Background(), 0, 100)
			require.Empty(t, logs)
			require.False(t, more)
			for _, err := range []error{headErr, batchErr} {
				require.ErrorContains(t, err, "parsing URL: invalid mirror source URL")
				for cause := err; cause != nil; cause = errors.Unwrap(cause) {
					require.NotContains(t, cause.Error(), password)
					require.NotContains(t, cause.Error(), baseURL)
				}
			}
		})
	}
}

// This transport exercises net/http's URL error wrapper without network I/O.
type failingMirrorTransport struct {
	t     *testing.T
	user  *url.Userinfo
	cause error
}

func (r failingMirrorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.t.Helper()
	require.Equal(r.t, r.user, req.URL.User)
	username, password, ok := req.BasicAuth()
	require.True(r.t, ok)
	require.Equal(r.t, r.user.Username(), username)
	expectedPassword, _ := r.user.Password()
	require.Equal(r.t, expectedPassword, password)
	if r.cause != nil {
		return nil, fmt.Errorf("synthetic transport failure: %s: %w", req.URL, r.cause)
	}

	return nil, fmt.Errorf("synthetic transport failure: %s", req.URL)
}

func TestHTTPSource_TransportDiagnostics(t *testing.T) {
	t.Parallel()
	for name, user := range map[string]*url.Userinfo{
		"token":            url.User("MIRROR_TOKEN_CANARY"),
		"escaped token":    url.User("MIRROR/TOKEN+CANARY@"),
		"password":         url.UserPassword("mirror-user", "MIRROR_PASSWORD_CANARY"),
		"escaped password": url.UserPassword("mirror-user", "MIRROR/PASSWORD+CANARY@"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			baseURL := "https://" + user.String() + "@mirror.example"
			source := NewHTTPSource(baseURL, "source-ledger", &http.Client{Transport: failingMirrorTransport{t: t, user: user}})
			t.Cleanup(func() { require.NoError(t, source.Close()) })
			_, headErr := source.GetLatestLogID(context.Background())
			logs, more, batchErr := source.FetchLogs(context.Background(), 0, 100)
			require.Empty(t, logs)
			require.False(t, more)
			require.Equal(t, baseURL, source.baseURL)
			secret := user.Username()
			if password, ok := user.Password(); ok {
				secret = password
			}
			for _, err := range []error{headErr, batchErr} {
				require.ErrorContains(t, err, "synthetic transport failure")
				require.ErrorContains(t, err, "fetching logs")
				for cause := err; cause != nil; cause = errors.Unwrap(cause) {
					for _, diagnostic := range []string{cause.Error(), fmt.Sprintf("%+v", cause), fmt.Sprintf("%#v", cause)} {
						require.NotContains(t, diagnostic, secret)
						require.NotContains(t, diagnostic, url.QueryEscape(secret))
						require.NotContains(t, diagnostic, url.PathEscape(secret))
						require.NotContains(t, diagnostic, user.String())
					}
				}
			}
		})
	}
}

func TestHTTPSource_TransportCancellationIdentity(t *testing.T) {
	t.Parallel()
	for _, cancellation := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cancellation.Error(), func(t *testing.T) {
			t.Parallel()
			user := url.User("MIRROR_CANCELLATION_TOKEN")
			source := NewHTTPSource("https://"+user.String()+"@mirror.example", "source-ledger", &http.Client{Transport: failingMirrorTransport{t: t, user: user, cause: cancellation}})
			t.Cleanup(func() { require.NoError(t, source.Close()) })
			_, err := source.GetLatestLogID(context.Background())
			require.ErrorIs(t, err, cancellation)
			require.ErrorContains(t, err, "synthetic transport failure")
			require.NotContains(t, err.Error(), user.Username())
		})
	}
}
