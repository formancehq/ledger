package bootstrap

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestHTTPServerFailureStopsApplication(t *testing.T) {
	t.Parallel()
	listener := mustListenLoopback(t, 0)
	app := fx.New(fx.NopLogger, fx.Invoke(func(lc fx.Lifecycle, shutdowner fx.Shutdowner) {
		lc.Append(httpServerHook(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}), listener, "", logging.Testing(), shutdownRequester(shutdowner)))
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	t.Cleanup(func() { _ = app.Stop(context.Background()) })
	requireHTTPOK(t, listener.Addr().String())
	require.NoError(t, listener.Close())
	select {
	case <-app.Wait():
	case <-ctx.Done():
		t.Fatal("HTTP Serve failure did not request application shutdown")
	}
	require.ErrorIs(t, app.Stop(ctx), net.ErrClosed)
}
