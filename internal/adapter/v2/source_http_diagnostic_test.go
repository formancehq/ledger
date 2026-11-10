package v2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type rejectingMirrorTransport struct {
	t *testing.T
}

type cancelingMirrorTransport struct {
	http.RoundTripper

	canceled *http.Request
}

func (t *cancelingMirrorTransport) CancelRequest(req *http.Request) {
	t.canceled = req
}

func TestMirrorRedirectTransport_ForwardsCancellation(t *testing.T) {
	t.Parallel()
	transport := &cancelingMirrorTransport{}
	observer := &mirrorRedirectTransport{RoundTripper: transport}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://mirror.example", nil)
	require.NoError(t, err)
	observer.CancelRequest(req)
	require.Same(t, req, transport.canceled)
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
	t              *testing.T
	user           *url.Userinfo
	cause          error
	redirectUser   *url.Userinfo
	alwaysRedirect bool
	calls          *int
	redirectPath   string
}

func (r failingMirrorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.t.Helper()
	if r.calls != nil {
		*r.calls++
	}
	if r.redirectUser != nil && (req.URL.Host == "mirror.example" || r.alwaysRedirect) {
		path := "/final"
		if r.redirectPath != "" {
			path = r.redirectPath
		}

		return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": {"https://" + r.redirectUser.String() + "@redirect.example" + path}}, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
	}
	expectedUser := r.user
	if r.redirectUser != nil {
		expectedUser = r.redirectUser
	}
	require.Equal(r.t, expectedUser, req.URL.User)
	username, password, ok := req.BasicAuth()
	require.True(r.t, ok)
	require.Equal(r.t, expectedUser.Username(), username)
	expectedPassword, _ := expectedUser.Password()
	require.Equal(r.t, expectedPassword, password)
	if r.cause != nil {
		return nil, fmt.Errorf("synthetic transport failure: %s: %w", req.URL, r.cause)
	}

	return nil, fmt.Errorf("synthetic transport failure: %s", req.URL)
}

func TestHTTPSource_MalformedRedirectDiagnostics(t *testing.T) {
	t.Parallel()
	for _, user := range []*url.Userinfo{
		url.User("REDIRECT_SECRET"),
		url.UserPassword("REDIRECT_USER", "REDIRECT/PASSWORD+SECRET@"),
	} {
		t.Run(user.String(), func(t *testing.T) {
			t.Parallel()
			calls := 0
			policyCalls := 0
			client := &http.Client{
				Transport: failingMirrorTransport{t: t, redirectUser: user, redirectPath: "/%zz", calls: &calls},
				CheckRedirect: func(*http.Request, []*http.Request) error {
					policyCalls++

					return nil
				},
			}
			source := NewHTTPSource("https://INITIAL_SOURCE_TOKEN@mirror.example", "source-ledger", client)
			t.Cleanup(func() { require.NoError(t, source.Close()) })
			_, headErr := source.GetLatestLogID(context.Background())
			logs, more, batchErr := source.FetchLogs(context.Background(), 0, 100)
			require.Empty(t, logs)
			require.False(t, more)
			require.Equal(t, 2, calls)
			require.Zero(t, policyCalls, "Location parsing fails before CheckRedirect")
			for _, err := range []error{headErr, batchErr} {
				require.ErrorContains(t, err, "fetching logs: invalid mirror redirect URL")
				secrets := []string{"INITIAL_SOURCE_TOKEN", user.Username(), user.String()}
				if password, ok := user.Password(); ok {
					secrets = append(secrets, password)
				}
				for cause := err; cause != nil; cause = errors.Unwrap(cause) {
					for _, diagnostic := range []string{cause.Error(), fmt.Sprintf("%+v", cause), fmt.Sprintf("%#v", cause)} {
						for _, secret := range secrets {
							require.NotContains(t, diagnostic, secret)
							require.NotContains(t, diagnostic, url.QueryEscape(secret))
							require.NotContains(t, diagnostic, url.PathEscape(secret))
						}
					}
				}
			}
		})
	}
}

func TestHTTPSource_TransportDiagnostics(t *testing.T) {
	t.Parallel()
	for name, user := range map[string]*url.Userinfo{
		"token":              url.User("MIRROR_TOKEN_CANARY"),
		"replacement marker": url.User("[redacted]"),
		"escaped token":      url.User("MIRROR/TOKEN+CANARY@"),
		"password":           url.UserPassword("mirror-user", "MIRROR_PASSWORD_CANARY"),
		"escaped password":   url.UserPassword("mirror-user", "MIRROR/PASSWORD+CANARY@"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, redirect := range []bool{false, true} {
				t.Run(fmt.Sprintf("redirect=%t", redirect), func(t *testing.T) {
					t.Parallel()
					transport := failingMirrorTransport{t: t, user: user}
					if redirect {
						transport.user = url.User("INITIAL_MIRROR_TOKEN")
						transport.redirectUser = user
					}
					baseURL := "https://" + transport.user.String() + "@mirror.example"
					source := NewHTTPSource(baseURL, "source-ledger", &http.Client{Transport: transport})
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
			redirectErr := source.transportDiagnostic(fmt.Errorf("unsafe REDIRECT_SECRET: %w", cancellation), true, user)
			require.ErrorIs(t, redirectErr, cancellation)
			require.ErrorContains(t, redirectErr, "invalid mirror redirect URL")
			require.NotContains(t, redirectErr.Error(), "REDIRECT_SECRET")
		})
	}
}

func TestHTTPSource_RedirectPolicies(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		policy   func(*http.Request, []*http.Request) error
		want     string
		requests int
	}{
		{name: "default limit", want: "stopped after 10 redirects", requests: 10},
		{name: "custom rejection", policy: func(*http.Request, []*http.Request) error { return errors.New("custom redirect rejection") }, want: "custom redirect rejection", requests: 1},
		{name: "last response", policy: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, want: "v2 API returned status 302", requests: 1},
		{name: "cancellation", policy: func(*http.Request, []*http.Request) error { return context.Canceled }, want: context.Canceled.Error(), requests: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			user := url.UserPassword("REDIRECT_POLICY_USER", "REDIRECT_POLICY_PASSWORD")
			client := &http.Client{Transport: failingMirrorTransport{t: t, user: user, redirectUser: user, alwaysRedirect: true, calls: &calls}, CheckRedirect: tc.policy}
			source := NewHTTPSource("https://mirror.example", "source-ledger", client)
			t.Cleanup(func() { require.NoError(t, source.Close()) })
			_, err := source.GetLatestLogID(context.Background())
			require.ErrorContains(t, err, tc.want)
			require.NotContains(t, err.Error(), user.Username())
			password, _ := user.Password()
			require.NotContains(t, err.Error(), password)
			require.Equal(t, tc.requests, calls)
			if tc.name == "cancellation" {
				require.ErrorIs(t, err, context.Canceled)
			}
			if tc.policy == nil {
				require.Nil(t, client.CheckRedirect, "source must not mutate caller's client")
			}
		})
	}
}
