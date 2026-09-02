package testserver

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
)

func TestPortSetClaimsEachPortOnce(t *testing.T) {
	t.Parallel()

	set := &portSet{ports: map[int]struct{}{}}

	require.True(t, set.claim(40001))
	require.False(t, set.claim(40001), "a claimed port must not be handed out twice")
	require.True(t, set.claim(40002))
}

func TestBindFreshSkipsAPortThisProcessAlreadyClaimed(t *testing.T) {
	t.Parallel()

	// Two real loopback listeners standing in for what the kernel offers. The
	// first one's port is already spoken for — the shape AllocateDeadAddress
	// creates, where a port is claimed while nothing listens on it — so
	// bindFresh must walk past it. A test cannot make the real kernel offer a
	// chosen port, hence the injected bind.
	claimed := mustBindLoopbackZero(t)
	free := mustBindLoopbackZero(t)

	offered := []net.Listener{claimed, free}
	bind := func() (net.Listener, error) {
		listener := offered[0]
		offered = offered[1:]

		return listener, nil
	}

	set := &portSet{ports: map[int]struct{}{}}
	require.True(t, set.claim(listenerPort(claimed)))

	got, err := set.bindFresh(bind)
	require.NoError(t, err)
	require.Equal(t, listenerPort(free), listenerPort(got), "bindFresh must skip the claimed port")
	require.Empty(t, offered, "bindFresh must have asked for a second candidate")
}

func TestBindFreshGivesUpInsteadOfSpinning(t *testing.T) {
	t.Parallel()

	listener := mustBindLoopbackZero(t)

	set := &portSet{ports: map[int]struct{}{}}
	require.True(t, set.claim(listenerPort(listener)))

	// Only ever offers the one claimed port: the search must end with an error
	// rather than loop forever or hand back the port it refused.
	_, err := set.bindFresh(func() (net.Listener, error) {
		return listener, nil
	})
	require.ErrorContains(t, err, "already-claimed ports in a row")
}

func TestBindFreshReportsABindFailure(t *testing.T) {
	t.Parallel()

	set := &portSet{ports: map[int]struct{}{}}

	_, err := set.bindFresh(func() (net.Listener, error) {
		return nil, net.ErrClosed
	})
	require.ErrorIs(t, err, net.ErrClosed)
}

func TestAllocateNodeLeaseHoldsThreeDistinctPortsBound(t *testing.T) {
	t.Parallel()

	lease := AllocateNodeLease()
	ports := lease.Ports()

	require.True(t, ports.Allocated())
	require.NotEqual(t, ports.HTTP(), ports.GRPC())
	require.NotEqual(t, ports.HTTP(), ports.Raft())
	require.NotEqual(t, ports.GRPC(), ports.Raft())

	// The point of a lease over an allocator: the sockets are already bound, so
	// nothing else on the machine — including another test binary — can take
	// these ports between now and the moment the node adopts them.
	for _, port := range []int{ports.HTTP(), ports.GRPC(), ports.Raft()} {
		_, err := net.Listen("tcp4", fmt.Sprintf("%s:%d", loopbackHost, port))
		require.Error(t, err, "port %d must already be bound by the lease", port)
	}
}

func TestNodeLeaseRebindsTheSamePortsForARestart(t *testing.T) {
	t.Parallel()

	lease := AllocateNodeLease()
	ports := lease.Ports()

	first := lease.NextGeneration()
	require.Equal(t, ports.HTTP(), listenerPort(first.HTTP))
	require.Equal(t, ports.GRPC(), listenerPort(first.Service))
	require.Equal(t, ports.Raft(), listenerPort(first.Raft))

	// What the lifecycle does on stop.
	for _, listener := range []net.Listener{first.HTTP, first.Service, first.Raft} {
		require.NoError(t, listener.Close())
	}

	second := lease.NextGeneration()
	t.Cleanup(func() {
		for _, listener := range []net.Listener{second.HTTP, second.Service, second.Raft} {
			_ = listener.Close()
		}
	})

	require.Equal(t, ports, lease.Ports(), "a restart must not move the node's addresses")
	require.Equal(t, ports.HTTP(), listenerPort(second.HTTP))
	require.Equal(t, ports.GRPC(), listenerPort(second.Service))
	require.Equal(t, ports.Raft(), listenerPort(second.Raft))
	require.NotSame(t, first.HTTP, second.HTTP, "a consumed generation must not be handed out again")
}

func TestNodeLeasePanicsWhenTheNodeWasNotStopped(t *testing.T) {
	t.Parallel()

	lease := AllocateNodeLease()

	generation := lease.NextGeneration()
	t.Cleanup(func() {
		for _, listener := range []net.Listener{generation.HTTP, generation.Service, generation.Raft} {
			_ = listener.Close()
		}
	})

	// Rebinding while the previous generation still holds the port cannot
	// succeed. Saying so beats handing back a half-bound generation.
	defer func() {
		reason := recover()
		require.NotNil(t, reason, "rebinding a still-bound port must panic")
		require.Contains(t, fmt.Sprint(reason), "rebinding",
			"the panic must name the restart rebind, not something unrelated")
	}()

	_ = lease.NextGeneration()
}

func TestAllocateDeadAddressLeavesNothingListening(t *testing.T) {
	t.Parallel()

	address := AllocateDeadAddress()

	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	require.Equal(t, loopbackHost, host)

	conn, err := net.Dial("tcp4", address)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("a dead address must refuse connections, but %s accepted one", address)
	}

	// Released at the OS level, but still spoken for here: a later allocation
	// must not hand this number to a live node.
	var number int
	_, err = fmt.Sscanf(port, "%d", &number)
	require.NoError(t, err)
	require.False(t, claimedPorts.claim(number), "a dead address must stay claimed")
}

func TestZeroValueNodePortsIsNotAllocated(t *testing.T) {
	t.Parallel()

	require.False(t, NodePorts{}.Allocated())
}

func TestDefaultTestInstrumentsRejectsUnallocatedPorts(t *testing.T) {
	t.Parallel()

	// A zero-value NodePorts would configure --http-port 0 and let the OS pick
	// a port the test can never learn. Fail loudly instead.
	require.Panics(t, func() {
		_ = DefaultTestInstruments(TestNodeConfig{
			NodeID:    1,
			ClusterID: "test-cluster",
		})
	})
}

func TestHealthDiskThresholdsConfigureBothTestVolumes(t *testing.T) {
	t.Parallel()

	cfg := &testservice.RunConfiguration{}
	require.NoError(t, withHealthDiskThresholds(1, 0.99).Instrument(context.Background(), cfg))
	require.Equal(t, []string{
		"--health-wal-threshold", "1",
		"--health-data-threshold", "1",
		"--health-wal-resume-threshold", "0.99",
		"--health-data-resume-threshold", "0.99",
	}, cfg.GetArgs())
}

func mustBindLoopbackZero(t *testing.T) net.Listener {
	t.Helper()

	listener, err := bindLoopbackZero()
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	return listener
}
