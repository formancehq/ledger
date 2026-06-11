package node

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// newRaceTestPeerConn builds a peerConnection with the minimum surface
// pushMessages and stop touch. We don't run loop() — instead we close
// loopDone up-front so stop returns immediately, mimicking the
// "RemovePeer after loop already exited" path that triggered #315.
func newRaceTestPeerConn(t *testing.T) *peerConnection {
	t.Helper()

	stopCtx, stopCancel := context.WithCancel(context.Background())
	m := noop.NewMeterProvider().Meter("test")

	var (
		loadH metric.Int64Histogram
		fullC metric.Float64Counter
		err   error
	)

	loadH, err = m.Int64Histogram("noop.load")
	require.NoError(t, err)
	fullC, err = m.Float64Counter("noop.full")
	require.NoError(t, err)

	loopDone := make(chan struct{})
	close(loopDone) // loop is already gone — stop() will return immediately

	return &peerConnection{
		highPriorityCh:         make(chan []raftpb.Message, 4),
		mediumPriorityCh:       make(chan []raftpb.Message, 4),
		lowPriorityCh:          make(chan []raftpb.Message, 4),
		stopCtx:                stopCtx,
		stopCancel:             stopCancel,
		loopDone:               loopDone,
		logger:                 logging.Testing(),
		sendQueueLoadHistogram: [3]metric.Int64Histogram{loadH, loadH, loadH},
		sendQueueFullCounter:   [3]metric.Float64Counter{fullC, fullC, fullC},
	}
}

// TestPeerConnection_PushMessagesAfterStopDoesNotPanic pins the fix
// for #315. Before this PR, conn.stop() closed the per-peer priority
// channels via closeQueues(). A pushMessages call racing with
// RemovePeer (the Start goroutine had already snapshotted the conn
// pointer before the map delete) panics with "send on closed channel"
// — the panic happens inside the select-with-default and is therefore
// NOT suppressed. The whole node crashes during a membership change,
// which is exactly the worst time for an outage.
//
// The fix drops closeQueues entirely. The channels are now GC'd with
// the peerConnection struct, and a late pushMessages either writes
// successfully (buffer space) or falls through the default (drop).
func TestPeerConnection_PushMessagesAfterStopDoesNotPanic(t *testing.T) {
	t.Parallel()

	conn := newRaceTestPeerConn(t)

	require.NoError(t, conn.stop(context.Background()))

	// All three priority paths must survive the late send.
	require.NotPanics(t, func() {
		_ = conn.pushMessages(0, []raftpb.Message{{}})
	}, "high-priority pushMessages after stop must not panic (#315)")

	require.NotPanics(t, func() {
		_ = conn.pushMessages(1, []raftpb.Message{{}})
	}, "medium-priority pushMessages after stop must not panic (#315)")

	require.NotPanics(t, func() {
		_ = conn.pushMessages(2, []raftpb.Message{{}})
	}, "low-priority pushMessages after stop must not panic (#315)")
}

// TestPeerConnection_PushMessagesRacedAgainstStopDoesNotPanic
// stresses the same property under concurrent pushMessages activity,
// which is the actual production shape (the transport Start goroutine
// keeps draining pendingSendQueue while RemovePeer is in flight).
func TestPeerConnection_PushMessagesRacedAgainstStopDoesNotPanic(t *testing.T) {
	t.Parallel()

	conn := newRaceTestPeerConn(t)

	var wg sync.WaitGroup

	stopProducers := make(chan struct{})

	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stopProducers:
					return
				default:
					_ = conn.pushMessages(0, []raftpb.Message{{}})
					_ = conn.pushMessages(1, []raftpb.Message{{}})
					_ = conn.pushMessages(2, []raftpb.Message{{}})
				}
			}
		})
	}

	require.NoError(t, conn.stop(context.Background()))

	// Let the producers run a bit more after stop returned — this is the
	// window the bug used to crash in.
	for i := range 1000 {
		_ = conn.pushMessages(i%3, []raftpb.Message{{}})
	}

	close(stopProducers)
	wg.Wait()
}
