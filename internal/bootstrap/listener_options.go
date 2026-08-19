package bootstrap

import (
	"net"

	"github.com/formancehq/go-libs/v5/pkg/transport/httpserver"

	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
)

// The listeners themselves are described by network.Bindings, which runServer
// supplies into the fx graph. This file holds the two adapters that turn an
// optional binding into the server option that adopts it.

// listenerOptions turns an optional binding into the gRPC server options that
// adopt it. No binding means no option, so the server binds its own port.
func listenerOptions(listener net.Listener) []grpcadp.Option {
	if listener == nil {
		return nil
	}

	return []grpcadp.Option{grpcadp.WithListener(listener)}
}

// httpListenerOption selects how the HTTP server gets its socket: the injected
// listener when there is one, the configured address otherwise. go-libs requires
// exactly one of the two — serverport.Listen errors when both are unset, and a
// listener silently wins over an address — so the choice is made here rather
// than by appending both.
func httpListenerOption(listener net.Listener, addr string) httpserver.ServerOptionModifier {
	if listener == nil {
		return httpserver.WithAddress(addr)
	}

	return httpserver.WithListener(listener)
}
