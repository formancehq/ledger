package bootstrap

import (
	"context"
	"errors"
	"net"

	"go.uber.org/fx"

	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/pkg/network"
)

// The listeners themselves are described by network.Bindings, which runServer
// supplies into the fx graph. HTTP adopts its binding in httpServerHook; this
// file adapts gRPC bindings and releases all injected sockets on stop.

// listenerOptions turns an optional binding into the gRPC server options that
// adopt it. No binding means no option, so the server binds its own port.
func listenerOptions(listener net.Listener) []grpcadp.Option {
	if listener == nil {
		return nil
	}

	return []grpcadp.Option{grpcadp.WithListener(listener)}
}

// releaseBindingsHook returns the lifecycle hook that closes every injected
// listener on stop.
//
// Each server already closes the socket it adopted, but a module may consume
// none of them: restore mode serves HTTP and the service gRPC port and never the
// Raft one. Leaving that socket open would hold a port past the life of the
// application that owns it, so the next node on those addresses could not bind
// it — and normal → restore → normal is exactly the sequence the restore e2e
// specs run. Ownership is therefore total rather than per-adoption: everything
// in Bindings is released here, and a listener a server already closed is not an
// error.
//
// Register it FIRST in a module. fx runs OnStop in reverse registration order,
// so registering first means closing last, once the servers have stopped.
func releaseBindingsHook(bindings network.Bindings) fx.Hook {
	return fx.Hook{
		OnStop: func(_ context.Context) error {
			var errs []error

			for _, listener := range []net.Listener{bindings.HTTP, bindings.Service, bindings.Raft} {
				if listener == nil {
					continue
				}

				if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
					errs = append(errs, err)
				}
			}

			return errors.Join(errs...)
		},
	}
}
