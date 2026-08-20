package testserver

import (
	"fmt"
	"net"
	"sync"

	"github.com/spf13/cobra"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"

	"github.com/formancehq/ledger/v3/internal/pkg/network"
)

// loopbackHost is the only interface a test node listens on. Nothing outside
// the machine has any business reaching a test node, and every test dials
// 127.0.0.1 or localhost.
const loopbackHost = "127.0.0.1"

// freshBindAttempts bounds the search for a port this process has not already
// claimed. The kernel picks the port, and it only has to avoid the handful this
// process has released (see AllocateDeadAddress), so one attempt is the norm.
const freshBindAttempts = 32

// claimedPorts records every port this process has handed to a test fixture.
//
// Entries are never removed. A port stays claimed after its listener closes —
// a stopped node keeps its Raft address for a restart, and a dead peer address
// must never resolve to a live node — so "claimed" means "spoken for", not
// "currently bound". The kernel guarantees uniqueness among OPEN sockets; this
// set extends that guarantee across the closed ones.
var claimedPorts = &portSet{ports: map[int]struct{}{}}

type portSet struct {
	mu    sync.Mutex
	ports map[int]struct{}
}

// claim records port and reports whether it was free to take.
func (s *portSet) claim(port int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, taken := s.ports[port]; taken {
		return false
	}

	s.ports[port] = struct{}{}

	return true
}

// bindLoopbackZero binds a loopback port of the kernel's choosing.
func bindLoopbackZero() (net.Listener, error) {
	return net.Listen("tcp4", loopbackHost+":0")
}

// bindFresh binds a loopback port the kernel chooses and this process has never
// claimed, and returns the listener still open.
func bindFresh() (net.Listener, error) {
	return claimedPorts.bindFresh(bindLoopbackZero)
}

// bindFresh takes the first port bind offers that this set has not claimed yet.
//
// A candidate that is already claimed is held open until the search ends, so the
// same number cannot come back on the next attempt. bind is a parameter so the
// skip can be tested against a chosen sequence of ports.
func (s *portSet) bindFresh(bind func() (net.Listener, error)) (net.Listener, error) {
	var quarantine []net.Listener

	defer func() {
		for _, listener := range quarantine {
			// Never handed out, and the port stays claimed, so closing it
			// cannot hand the number to a node.
			_ = listener.Close()
		}
	}()

	for range freshBindAttempts {
		listener, err := bind()
		if err != nil {
			return nil, fmt.Errorf("binding a loopback port: %w", err)
		}

		if s.claim(listenerPort(listener)) {
			return listener, nil
		}

		quarantine = append(quarantine, listener)
	}

	return nil, fmt.Errorf("the kernel offered %d already-claimed ports in a row", freshBindAttempts)
}

// bindClaimed binds a port this process already owns. Used to rebind a node's
// advertised port for a restart.
func bindClaimed(port int) (net.Listener, error) {
	listener, err := net.Listen("tcp4", fmt.Sprintf("%s:%d", loopbackHost, port))
	if err != nil {
		return nil, fmt.Errorf("rebinding %s:%d: %w", loopbackHost, port, err)
	}

	return listener, nil
}

// NodePorts holds the advertised ports of one test node.
//
// The fields are unexported and allocated is set only by NodeLease, so a
// hand-written port cannot reach TestNodeConfig: that is a compile error rather
// than something a linter has to catch. Hand-picked ports are what put one
// node's Raft listener on another node's HTTP port in EN-1784.
type NodePorts struct {
	http      int
	grpc      int
	raft      int
	allocated bool
}

// HTTP returns the node's HTTP API port.
func (p NodePorts) HTTP() int { return p.http }

// GRPC returns the node's external service gRPC port.
func (p NodePorts) GRPC() int { return p.grpc }

// Raft returns the node's inter-node Raft transport port.
func (p NodePorts) Raft() int { return p.raft }

// Allocated reports whether these ports came from a NodeLease. The zero value
// is not allocated, which lets callers reject a hand-built NodePorts instead of
// letting a node bind port 0.
func (p NodePorts) Allocated() bool { return p.allocated }

// CommandFactory builds the server run command around a set of bindings.
// cmd/server.NewRunCommandWithBindings satisfies it.
//
// The indirection keeps this package independent of cmd/server: the run command
// lives in package server, whose own tests are in that same package and are
// free to use this one.
type CommandFactory func(network.Bindings) *cobra.Command

// NodeLease owns one test node's three loopback ports and hands the server a
// freshly bound listener generation for every start.
//
// The point is that a port number never travels alone. AllocateNodeLease binds
// the three sockets and keeps them open, so from the moment a spec learns its
// node's ports — while it builds join addresses, gateway peer lists and phantom
// peers, before any server exists — nothing else on the machine can take them.
// The lifecycle adopts those very sockets, so there is no probe-then-bind
// window for a concurrent test binary to slip into (EN-1784).
//
// A generation is one-shot: the lifecycle releases every listener it was given,
// including the ones it never served. NextGeneration therefore rebinds the same
// ports for a restart, which is the one moment the ports are briefly unheld —
// the lease still owns the numbers, so no other fixture in this process competes
// for them, and only an unrelated process could interpose. Removing even that
// window would need a proxy in front of every node, which is more machinery than
// these suites justify.
type NodeLease struct {
	mu       sync.Mutex
	ports    NodePorts
	current  network.Bindings
	consumed bool
}

// AllocateNodeLease binds the three ports one test node needs.
//
// It panics rather than returning an error: every caller is a test fixture that
// cannot proceed without a node, and a bind failure here means the machine is
// out of loopback ports.
//
// Each port is bound independently. The Raft port is deliberately NOT derived
// from the gRPC port: the old raftPort = grpcPort - 1000 rule projected a number
// chosen in the "gRPC" range into the "HTTP" range, which is how gRPC 16200
// became Raft 15200 and collided with a live HTTP listener (EN-1784).
func AllocateNodeLease() *NodeLease {
	httpListener := mustBindFresh("HTTP")
	serviceListener := mustBindFresh("service gRPC")
	raftListener := mustBindFresh("Raft")

	return &NodeLease{
		ports: NodePorts{
			http:      listenerPort(httpListener),
			grpc:      listenerPort(serviceListener),
			raft:      listenerPort(raftListener),
			allocated: true,
		},
		current: network.Bindings{
			HTTP:    httpListener,
			Service: serviceListener,
			Raft:    raftListener,
		},
	}
}

func mustBindFresh(role string) net.Listener {
	listener, err := bindFresh()
	if err != nil {
		panic(fmt.Sprintf("testserver: allocating the %s port: %v", role, err))
	}

	return listener
}

func listenerPort(listener net.Listener) int {
	return listener.Addr().(*net.TCPAddr).Port
}

// Ports returns the addresses this node advertises. They stay the same across
// restarts: a node's peers still hold its old Raft address, so a restart on a
// new port surfaces as a Raft failure rather than as a port mistake.
func (l *NodeLease) Ports() NodePorts {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.ports
}

// NextGeneration hands out the listeners for the next start, rebinding them when
// the previous generation has been consumed.
//
// It panics on a rebind failure for the same reason AllocateNodeLease does, and
// because the caller is a testservice command factory that cannot report an
// error. A failure here means the node's previous generation is still bound:
// stop the node before starting it again.
func (l *NodeLease) NextGeneration() network.Bindings {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.consumed {
		l.current = network.Bindings{
			HTTP:    mustBindClaimed("HTTP", l.ports.http),
			Service: mustBindClaimed("service gRPC", l.ports.grpc),
			Raft:    mustBindClaimed("Raft", l.ports.raft),
		}
	}

	l.consumed = true

	return l.current
}

func mustBindClaimed(role string, p int) net.Listener {
	listener, err := bindClaimed(p)
	if err != nil {
		panic(fmt.Sprintf("testserver: rebinding the %s port for a restart: %v", role, err))
	}

	return listener
}

// NewService returns a test service that runs a node on this lease's ports.
//
// Every Start asks the lease for a generation, so a stopped node can be started
// again through the same lease without the caller thinking about sockets.
func (l *NodeLease) NewService(newCommand CommandFactory, opts ...testservice.Option) *testservice.Service {
	return testservice.New(func() *cobra.Command {
		return newCommand(l.NextGeneration())
	}, opts...)
}

// AllocateDeadAddress returns a loopback address that nothing will listen on.
//
// A peer that is deliberately never started still needs an address, and that
// address must stay dead: pointing a leader's Raft transport at a live listener
// in its own cluster is exactly the confusion EN-1784 was. The port is bound
// long enough to claim it and then released, so connections are refused, and the
// claim keeps every later allocation in this process off that number.
func AllocateDeadAddress() string {
	listener, err := bindFresh()
	if err != nil {
		panic(fmt.Sprintf("testserver: allocating a dead address: %v", err))
	}

	address := listener.Addr().String()

	if err := listener.Close(); err != nil {
		panic(fmt.Sprintf("testserver: releasing the dead address %s: %v", address, err))
	}

	return address
}
