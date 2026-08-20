package bootstrap

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/transport/httpserver"

	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
)

func TestListenerOptionsPassNothingWithoutABinding(t *testing.T) {
	t.Parallel()

	require.Empty(t, listenerOptions(nil), "no binding must leave the server binding its own port")
}

// TestListenerOptionsMakeTheServerAdoptTheBinding checks the wiring end to end
// rather than the option count: the configured port is held by another socket, so
// a server that ignored the binding could not start.
func TestListenerOptionsMakeTheServerAdoptTheBinding(t *testing.T) {
	t.Parallel()

	injected := mustListenLoopback(t, 0)

	blocker := mustListenLoopback(t, 0)
	configuredPort := blocker.Addr().(*net.TCPAddr).Port

	srv, err := grpcadp.NewRaftServer(configuredPort, logging.Testing(), nil, true, "",
		listenerOptions(injected)...)
	require.NoError(t, err)

	require.NoError(t, srv.Listen())
	require.NoError(t, srv.Stop())
}

// TestHTTPListenerOptionServesTheInjectedListener and its fallback twin pin the
// choice go-libs forces: serverport.Listen wants exactly one of listener or
// address, and a listener silently wins over an address, so the two must never be
// passed together.
func TestHTTPListenerOptionServesTheInjectedListener(t *testing.T) {
	t.Parallel()

	listener := mustListenLoopback(t, 0)

	address := listener.Addr().String()
	stop := startHTTPHook(t, httpListenerOption(listener, "127.0.0.1:0"))

	requireHTTPOK(t, address)
	require.NoError(t, stop(context.Background()))
}

func TestHTTPListenerOptionFallsBackToTheConfiguredAddress(t *testing.T) {
	t.Parallel()

	// A port this process owns for the moment, then releases, so the hook can
	// bind the address itself.
	probe := mustListenLoopback(t, 0)
	address := probe.Addr().String()
	require.NoError(t, probe.Close())

	stop := startHTTPHook(t, httpListenerOption(nil, address))

	requireHTTPOK(t, address)
	require.NoError(t, stop(context.Background()))
}

func startHTTPHook(t *testing.T, option httpserver.ServerOptionModifier) func(context.Context) error {
	t.Helper()

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	hook := httpserver.NewHook(handler, option)

	ctx := httpserver.ContextWithServerInfo(logging.TestingContext())
	require.NoError(t, hook.OnStart(ctx))

	stopped := false
	stop := func(stopCtx context.Context) error {
		if stopped {
			return nil
		}

		stopped = true

		return hook.OnStop(stopCtx)
	}

	t.Cleanup(func() { _ = stop(context.Background()) })

	return stop
}

func requireHTTPOK(t *testing.T, address string) {
	t.Helper()

	client := &http.Client{Timeout: 5 * time.Second}

	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		fmt.Sprintf("http://%s/", address), nil)
	require.NoError(t, err)

	response, err := client.Do(request)
	require.NoError(t, err, "the HTTP server must serve %s", address)

	defer func() { _ = response.Body.Close() }()

	require.Equal(t, http.StatusOK, response.StatusCode)
}

func mustListenLoopback(t *testing.T, port int) net.Listener {
	t.Helper()

	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	return listener
}
