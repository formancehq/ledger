package testserver

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPortAllocatorNeverReturnsTheSamePortTwice(t *testing.T) {
	t.Parallel()

	allocator := newPortAllocator(portAllocatorBase, portAllocatorCeiling)

	seen := make(map[int]struct{}, 200)

	for range 200 {
		port := allocator.allocate()

		_, duplicate := seen[port]
		require.Falsef(t, duplicate, "port %d was handed out twice", port)

		seen[port] = struct{}{}
	}
}

func TestPortAllocatorSkipsAPortThatIsAlreadyBound(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	busy := listener.Addr().(*net.TCPAddr).Port

	// Point a fresh allocator straight at the bound port.
	allocator := newPortAllocator(busy, busy+50)

	require.NotEqual(t, busy, allocator.allocate(),
		"allocator handed out a port that was already bound")
}

func TestPortAllocatorPanicsWhenTheRangeIsExhausted(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	busy := listener.Addr().(*net.TCPAddr).Port

	// A one-port range whose only candidate is bound: allocation cannot succeed
	// and must fail loudly rather than spin.
	allocator := newPortAllocator(busy, busy)

	require.Panics(t, func() { _ = allocator.allocate() })
}

func TestAllocateNodePortsReturnsThreeDistinctAllocatedPorts(t *testing.T) {
	t.Parallel()

	ports := AllocateNodePorts()

	require.True(t, ports.Allocated())
	require.NotEqual(t, ports.HTTP(), ports.GRPC())
	require.NotEqual(t, ports.HTTP(), ports.Raft())
	require.NotEqual(t, ports.GRPC(), ports.Raft())
}

func TestZeroValueNodePortsIsNotAllocated(t *testing.T) {
	t.Parallel()

	require.False(t, NodePorts{}.Allocated())
}
