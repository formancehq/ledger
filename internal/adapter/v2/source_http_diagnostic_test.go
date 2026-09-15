package v2

import (
	"context"
	"errors"
	"net/http"
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
