package bootstrap

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// TestBuildAuthConfig_HangingIssuerRespectsTimeout reproduces the issue's
// scenario: a slow / blackholed OIDC issuer. The local fallback used to call
// oidc.Discover with context.Background(), which would hang startup
// indefinitely. With OIDCDiscoveryTimeout set, the call must return well
// inside the test deadline carrying a deadline-exceeded error.
func TestBuildAuthConfig_HangingIssuerRespectsTimeout(t *testing.T) {
	t.Parallel()

	// Hanging server: the handler blocks on a never-firing channel so the
	// underlying TCP socket is accepted but no HTTP response ever arrives.
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)

	timeout := 150 * time.Millisecond
	cfg := Config{
		AuthConfig: AuthFlagConfig{
			Enabled:              true,
			Issuer:               srv.URL,
			OIDCDiscoveryTimeout: timeout,
		},
	}

	start := time.Now()
	_, err := buildAuthConfig(cfg, logging.Testing(), nil)
	elapsed := time.Since(start)

	require.Error(t, err, "discovery must fail against a hanging issuer")
	require.Less(t, elapsed, 2*time.Second,
		"discovery should return inside ~timeout (%s), took %s", timeout, elapsed)
}

func TestTimeoutHTTPClient(t *testing.T) {
	t.Parallel()

	t.Run("zero timeout returns DefaultClient", func(t *testing.T) {
		t.Parallel()
		require.Same(t, http.DefaultClient, TimeoutHTTPClient(0))
	})

	t.Run("positive timeout returns a bounded client", func(t *testing.T) {
		t.Parallel()
		c := TimeoutHTTPClient(500 * time.Millisecond)
		require.NotSame(t, http.DefaultClient, c)
		require.Equal(t, 500*time.Millisecond, c.Timeout)
	})

	t.Run("negative timeout returns DefaultClient", func(t *testing.T) {
		t.Parallel()
		require.Same(t, http.DefaultClient, TimeoutHTTPClient(-1))
	})
}

func TestDiscoveryContext(t *testing.T) {
	t.Parallel()

	t.Run("zero timeout returns background context", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := discoveryContext(0)
		t.Cleanup(cancel)
		_, ok := ctx.Deadline()
		require.False(t, ok, "background context must not carry a deadline")
		require.NoError(t, ctx.Err())
	})

	t.Run("positive timeout sets a deadline", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := discoveryContext(500 * time.Millisecond)
		t.Cleanup(cancel)
		deadline, ok := ctx.Deadline()
		require.True(t, ok, "context must carry a deadline when timeout > 0")
		require.WithinDuration(t, time.Now().Add(500*time.Millisecond), deadline, time.Second)
	})

	t.Run("cancel releases resources", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := discoveryContext(time.Hour)
		cancel()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})
}
