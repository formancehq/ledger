// Package network carries the node's already-bound listening sockets from
// whoever created them to the lifecycle that serves them.
//
// It is a leaf package on purpose: the run command, the fx wiring and the test
// fixtures all name this type, and none of them may depend on each other.
package network

import "net"

// Bindings carries listeners that are already bound to the addresses this node
// serves. A nil field means "bind it yourself from the configured port", which
// is what production does: the zero value is the production value, so nothing
// outside a test has to think about this type.
//
// Tests inject pre-bound listeners so that a port is never merely probed and
// released before the server binds it. Binding 127.0.0.1:0 and keeping the
// socket open until the application adopts it is the only way to hold a port
// across the gap between "the test knows the number" and "the server listens on
// it"; every scheme that returns a number instead of a socket leaves that gap
// open, and two suites starting together can pick the same number (EN-1784).
//
// Ownership: once a listener reaches the fx graph, the lifecycle owns it and
// closes it on stop — the gRPC servers through their Stop, the HTTP server
// through http.Server.Shutdown, and anything no module consumed (the Raft
// listener in restore mode) through the release hook the modules register for
// exactly that reason. Ownership is total, so a fixture never has to work out
// which listeners an application actually served.
//
// Listeners are therefore one-shot: a node that restarts needs a freshly bound
// generation on the same ports, which is what testserver.NodeLease hands out. An
// application that failed to build never reached its lifecycle and never
// released anything, so its listeners stay with whoever created them.
type Bindings struct {
	// HTTP serves the REST API.
	HTTP net.Listener
	// Service serves the external client-facing gRPC API.
	Service net.Listener
	// Raft serves the internal inter-node gRPC transport. Restore mode never
	// consumes it.
	Raft net.Listener
}
