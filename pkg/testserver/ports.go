package testserver

import (
	"fmt"
	"net"
	"sync"
)

const (
	// portAllocatorBase is the first candidate port. It sits below the OS
	// ephemeral range (Linux 32768+, macOS 49152+) so an allocated port cannot
	// be taken by an outbound connection in the window between the probe in
	// allocate and the moment the node actually binds it.
	portAllocatorBase = 15000

	// portAllocatorCeiling bounds the walk. Without a ceiling an exhausted
	// range would spin past 65535 forever, where every net.Listen fails and
	// every failure is skipped.
	portAllocatorCeiling = 30000
)

// portAllocator hands out TCP ports that no other test node in this process
// holds.
//
// State is held on an explicit struct rather than in package globals so the
// tests can build their own allocator and stay hermetic.
type portAllocator struct {
	mu       sync.Mutex
	base     int
	next     int
	ceiling  int
	reserved map[int]struct{}
}

func newPortAllocator(base, ceiling int) *portAllocator {
	return &portAllocator{
		base:     base,
		next:     base,
		ceiling:  ceiling,
		reserved: make(map[int]struct{}),
	}
}

// allocate returns a port that is free right now and that this allocator has
// never returned before.
//
// The bind probe means a stale server left by a killed previous run, or an
// unrelated service on the band, is skipped instead of being handed to a node
// that would then fail to bind. The reserved set means no two nodes in one
// process are ever given the same number — the property that makes EN-1784
// impossible rather than unlikely.
func (a *portAllocator) allocate() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	for a.next <= a.ceiling {
		port := a.next
		a.next++

		if _, taken := a.reserved[port]; taken {
			continue
		}

		listener, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", port))
		if err != nil {
			continue
		}

		_ = listener.Close()

		a.reserved[port] = struct{}{}

		return port
	}

	panic(fmt.Sprintf(
		"testserver: no free TCP port in [%d,%d]: every candidate was busy or already reserved",
		a.base, a.ceiling,
	))
}

var defaultPortAllocator = newPortAllocator(portAllocatorBase, portAllocatorCeiling)

// AllocatePort reserves one TCP port for a fixture that needs a port outside a
// node's triple — currently only the Raft test gateway.
func AllocatePort() int {
	return defaultPortAllocator.allocate()
}

// NodePorts holds the listening ports of one test node.
//
// The fields are unexported and allocated is set only by AllocateNodePorts, so
// a hand-written port cannot reach TestNodeConfig: that is a compile error
// rather than something a linter has to catch. Hand-picked ports are what put
// one node's Raft listener on another node's HTTP port in EN-1784.
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

// Allocated reports whether these ports came from AllocateNodePorts. A
// zero-value NodePorts is not allocated, and DefaultTestInstruments rejects it
// rather than letting a node bind port 0.
func (p NodePorts) Allocated() bool { return p.allocated }

// AllocateNodePorts reserves the three ports a test node needs.
//
// Each port is allocated independently. The Raft port is deliberately NOT
// derived from the gRPC port: the old raftPort = grpcPort - 1000 rule projected
// a number chosen in the "gRPC" range into the "HTTP" range, which is how gRPC
// 16200 became Raft 15200 and collided with a live HTTP listener (EN-1784).
func AllocateNodePorts() NodePorts {
	return NodePorts{
		http:      AllocatePort(),
		grpc:      AllocatePort(),
		raft:      AllocatePort(),
		allocated: true,
	}
}
