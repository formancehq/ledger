package testserver

import (
	"fmt"
	"net"
	"os"
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

	// portAllocatorBuckets and portAllocatorStride carve the band into
	// per-process sub-bands. See processPortBase.
	portAllocatorBuckets = 64
	portAllocatorStride  = 200
)

// portAllocator hands out TCP ports that no other test node in this process
// holds.
//
// State is held on an explicit struct rather than in package globals so the
// tests can build their own allocator and stay hermetic.
type portAllocator struct {
	mu      sync.Mutex
	base    int
	next    int
	ceiling int
}

func newPortAllocator(base, ceiling int) *portAllocator {
	return &portAllocator{
		base:    base,
		next:    base,
		ceiling: ceiling,
	}
}

// allocate returns a port that is free right now and that this allocator has
// never returned before.
//
// The bind probe means a stale server left by a killed previous run, or an
// unrelated service on the band, is skipped instead of being handed to a node
// that would then fail to bind. Monotonic next under the mutex means a number
// is never handed out twice within a process; processPortBase keeps different
// processes apart.
func (a *portAllocator) allocate() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	for a.next <= a.ceiling {
		port := a.next
		a.next++

		listener, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", port))
		if err != nil {
			continue
		}

		if err := listener.Close(); err != nil {
			// Still bound, so handing this port out would defeat the probe.
			continue
		}

		return port
	}

	panic(fmt.Sprintf(
		"testserver: no free TCP port in [%d,%d]: every candidate was busy",
		a.base, a.ceiling,
	))
}

// processPortBase returns this process's starting point inside the band.
//
// Every process that links this package walks the same deterministic sequence,
// and allocate releases each probed port before the node binds it — so two
// processes starting together would be handed the same number. That is not
// hypothetical: `just test-scenarios` (Justfile:100) runs the scenario packages
// without `-p 1`, so up to GOMAXPROCS test binaries start within milliseconds
// of each other. Offsetting by pid gives each process its own sub-band, which
// restores the cross-process disjointness that hand-picked ports used to
// provide.
//
// A process that needs more than portAllocatorStride ports walks on into the
// next bucket rather than failing; the probe still keeps it correct.
func processPortBase() int {
	return portAllocatorBase + (os.Getpid()%portAllocatorBuckets)*portAllocatorStride
}

var defaultPortAllocator = newPortAllocator(processPortBase(), portAllocatorCeiling)

// AllocatePort reserves one TCP port for a test fixture.
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

// Allocated reports whether these ports came from AllocateNodePorts. The zero
// value is not allocated, which lets callers reject a hand-built NodePorts
// instead of letting a node bind port 0.
func (p NodePorts) Allocated() bool { return p.allocated }

// AllocateNodePorts reserves the three ports a test node needs.
//
// Each port is allocated independently. The Raft port is deliberately NOT
// derived from the gRPC port: the old raftPort = grpcPort - 1000 rule projected
// a number chosen in the "gRPC" range into the "HTTP" range, which is how gRPC
// 16200 became Raft 15200 and collided with a live HTTP listener (EN-1784).
//
// Ports are never released or reused. A node that is stopped and restarted must
// be given the SAME NodePorts value rather than a fresh allocation: its peers
// still hold its old Raft address, and a restart on a new port surfaces as a
// Raft failure rather than a port mistake. NodePorts is a value type, so
// carrying the value across the restart is enough.
func AllocateNodePorts() NodePorts {
	return NodePorts{
		http:      AllocatePort(),
		grpc:      AllocatePort(),
		raft:      AllocatePort(),
		allocated: true,
	}
}
