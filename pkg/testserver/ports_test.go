package testserver

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// testSkipBandBase sits above every band the other tests in this file walk
// (the default allocator tops out at portAllocatorBase +
// portAllocatorBuckets*portAllocatorStride) and below portAllocatorCeiling.
// Ports here are below the OS ephemeral floor, so no unrelated outbound socket
// can take one, and no sibling test competes for them.
const testSkipBandBase = 28000

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

	// Find a free port in the dedicated band and hold it, so the allocator
	// under test meets a genuinely bound first candidate. Deriving this from
	// net.Listen("0.0.0.0:0") instead would put the whole test inside the OS
	// ephemeral range, where an unrelated dial can take the port the allocator
	// just probed and released — the flake this shape avoids.
	source := newPortAllocator(testSkipBandBase, portAllocatorCeiling)
	busy := source.allocate()

	listener, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", busy))
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	allocator := newPortAllocator(busy, busy+50)

	got := allocator.allocate()

	require.Greater(t, got, busy, "allocator must skip the bound port")
	require.LessOrEqual(t, got, busy+50)

	// The point of the probe is that the result is actually usable.
	probe, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", got))
	require.NoError(t, err, "allocator returned a port that cannot be bound")
	_ = probe.Close()
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

	defer func() {
		reason := recover()
		require.NotNil(t, reason, "allocate must panic when the range is exhausted")
		require.Contains(t, fmt.Sprint(reason), "no free TCP port in",
			"panic must be the exhaustion panic, not an unrelated failure")
	}()

	allocator.allocate()
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

func TestProcessPortBaseStaysInsideTheBand(t *testing.T) {
	t.Parallel()

	base := processPortBase()

	require.GreaterOrEqual(t, base, portAllocatorBase)
	require.Less(t, base, portAllocatorBase+portAllocatorBuckets*portAllocatorStride)

	// A process in the highest bucket must still have room to allocate.
	require.Less(t, portAllocatorBase+portAllocatorBuckets*portAllocatorStride, portAllocatorCeiling,
		"buckets x stride must leave headroom below the ceiling")
}
