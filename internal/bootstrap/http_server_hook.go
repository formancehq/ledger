package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/transport/serverport"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/otlplogs"
)

// httpServerHook owns the HTTP endpoint in both normal and restore mode.
// An unexpected Serve failure requests shutdown immediately and is retained for
// OnStop so the service runner reports an unsuccessful exit.
func httpServerHook(handler http.Handler, listener net.Listener, address string, logger logging.Logger, requestShutdown func() error) fx.Hook {
	port := serverport.NewServer("http", serverport.WithListener(listener), serverport.WithAddress(address))
	requestContext, cancelRequests := context.WithCancel(context.Background())
	var connectionsMu sync.Mutex
	connectionsChanged := sync.NewCond(&connectionsMu)
	connections := map[net.Conn]struct{}{}
	server := &http.Server{
		Handler: handler,
		BaseContext: func(net.Listener) context.Context {
			return requestContext
		},
		ConnState: func(connection net.Conn, state http.ConnState) {
			connectionsMu.Lock()
			defer connectionsMu.Unlock()
			switch state {
			case http.StateNew:
				connections[connection] = struct{}{}
			case http.StateClosed, http.StateHijacked:
				delete(connections, connection)
				connectionsChanged.Broadcast()
			}
		},
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	var (
		wait     func()
		serveErr error
	)

	return fx.Hook{
		OnStart: func(ctx context.Context) error {
			if requestShutdown == nil {
				return errors.New("HTTP server: no shutdown requester wired into the hook")
			}
			if err := port.Listen(ctx); err != nil {
				return fmt.Errorf("HTTP server: %w", err)
			}
			wait = otlplogs.GoWait(func() {
				err := server.Serve(port.Listener)
				if errors.Is(err, http.ErrServerClosed) {
					return
				}
				if err == nil {
					err = errors.New("Serve returned without an error")
				}
				serveErr = fmt.Errorf("HTTP server stopped serving: %w", err)
				logger.Errorf("%v", serveErr)
				if err := requestShutdown(); err != nil {
					logger.Errorf("HTTP server: requesting application shutdown: %v", err)
				}
			}, logger)
			logger.Info("HTTP server started")

			return nil
		},
		OnStop: func(ctx context.Context) error {
			if wait == nil {
				cancelRequests()

				return errors.New("HTTP server: OnStop ran without a completed OnStart")
			}
			err := server.Shutdown(ctx)
			if err != nil {
				// Shutdown deliberately leaves active connections open when its
				// context expires. Cancel their request contexts, close their
				// transports to interrupt blocked I/O, then join every handler.
				cancelRequests()
				err = errors.Join(err, server.Close())
			}
			// Shutdown closes the listener even if request draining times out. Joining
			// Serve publishes serveErr and makes the connection set stable before it
			// is read and joined here.
			wait()
			connectionsMu.Lock()
			for len(connections) > 0 {
				connectionsChanged.Wait()
			}
			connectionsMu.Unlock()
			cancelRequests()

			return errors.Join(err, serveErr)
		},
	}
}
