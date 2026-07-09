package node

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// captureUnreachable wires a channel-backed sink for the pushUnreachable
// callback on a peerConnection so tests can observe which peer IDs Raft
// would be notified about on drop.
func captureUnreachable() (fn func(peerID uint64) bool, got func() []uint64) {
	var (
		mu      sync.Mutex
		peerIDs []uint64
	)

	fn = func(peerID uint64) bool {
		mu.Lock()
		defer mu.Unlock()
		peerIDs = append(peerIDs, peerID)

		return true
	}
	got = func() []uint64 {
		mu.Lock()
		defer mu.Unlock()
		out := make([]uint64, len(peerIDs))
		copy(out, peerIDs)

		return out
	}

	return fn, got
}

// newTestPeerConn builds a peerConnection whose priority channels have
// zero capacity, so any pushMessages call immediately hits the default
// branch. pushUnreachable is captured via the injected callback.
func newTestPeerConn(t *testing.T, peerID uint64, unreachable func(uint64) bool) *peerConnection {
	t.Helper()

	m := noop.NewMeterProvider().Meter("test")

	loadH, err := m.Int64Histogram("noop.load")
	require.NoError(t, err)
	fullC, err := m.Float64Counter("noop.full")
	require.NoError(t, err)

	return &peerConnection{
		highPriorityCh:         make(chan []*raftpb.Message), // cap 0 → default branch every send
		mediumPriorityCh:       make(chan []*raftpb.Message),
		lowPriorityCh:          make(chan []*raftpb.Message),
		logger:                 logging.Testing(),
		peerID:                 peerID,
		pushUnreachable:        unreachable,
		sendQueueLoadHistogram: [3]metric.Int64Histogram{loadH, loadH, loadH},
		sendQueueFullCounter:   [3]metric.Float64Counter{fullC, fullC, fullC},
	}
}

// TestPeerConnection_PushMessages_EmitsUnreachableOnDrop pins the fix
// for EN-1043: when a batch cannot be enqueued (channel full), the
// peer's Unreachable signal MUST fire so Raft transitions the peer's
// Progress from Replicate to Probe. The prior behavior logged a drop
// and returned silently, leaving Raft in optimistic replication.
func TestPeerConnection_PushMessages_EmitsUnreachableOnDrop(t *testing.T) {
	t.Parallel()

	notify, got := captureUnreachable()
	conn := newTestPeerConn(t, 42, notify)

	ok := conn.pushMessages(0, []*raftpb.Message{{}})
	require.False(t, ok, "pushMessages must report drop on full channel")
	require.Equal(t, []uint64{42}, got(), "Unreachable must be signalled with the peer ID on drop")
}

// TestPeerConnection_PushMessages_DedupsUnreachableWithinBurst pins the
// dedup contract: back-to-back drops for the same peer must NOT spam
// the shared unreachableCh (capacity 100 in production). Only the
// first drop in a burst fires; subsequent drops are absorbed until a
// successful sendMessages resets the flag.
func TestPeerConnection_PushMessages_DedupsUnreachableWithinBurst(t *testing.T) {
	t.Parallel()

	notify, got := captureUnreachable()
	conn := newTestPeerConn(t, 7, notify)

	for range 100 {
		require.False(t, conn.pushMessages(0, []*raftpb.Message{{}}))
	}
	require.Equal(t, []uint64{7}, got(), "burst of 100 drops must emit Unreachable exactly once")
}

// TestPeerConnection_PushMessages_RollsBackFlagOnEmitFailure pins
// the NumaryBot finding on PR #1519: when unreachableCh itself is
// full and pushUnreachable returns false, the dedup flag MUST be
// rolled back so a subsequent drop can retry the emit. Otherwise a
// transient unreachableCh overflow silences the peer's Unreachable
// signal until sendMessages succeeds — which for a stuck peer never
// happens, defeating the whole fix.
func TestPeerConnection_PushMessages_RollsBackFlagOnEmitFailure(t *testing.T) {
	t.Parallel()

	// Toggleable emit: first call fails (unreachableCh "full"), second succeeds.
	var (
		mu    sync.Mutex
		calls []uint64
		fail  = true
	)
	conn := newTestPeerConn(t, 9, func(peerID uint64) bool {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, peerID)

		return !fail
	})

	// First drop: emit fails, flag must roll back to false.
	require.False(t, conn.pushMessages(0, []*raftpb.Message{{}}))
	require.False(t, conn.unreachableReported.Load(),
		"failed emit must not leave the dedup flag set — otherwise subsequent drops are silently suppressed")

	// Flip the emit sink to succeed. Second drop must retry and set the flag.
	mu.Lock()
	fail = false
	mu.Unlock()
	require.False(t, conn.pushMessages(0, []*raftpb.Message{{}}))
	require.True(t, conn.unreachableReported.Load(),
		"successful emit after retry must set the dedup flag")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []uint64{9, 9}, calls, "both drops must have attempted an emit")
}

// TestPeerConnection_PushMessages_ReEmitsAfterSuccessfulSend covers
// the recovery half of the dedup contract. Once the peer has accepted
// a write (simulated by clearing the flag as sendMessages does), the
// next drop must re-signal Unreachable so Raft can re-throttle if the
// channel refills.
func TestPeerConnection_PushMessages_ReEmitsAfterSuccessfulSend(t *testing.T) {
	t.Parallel()

	notify, got := captureUnreachable()
	conn := newTestPeerConn(t, 3, notify)

	require.False(t, conn.pushMessages(0, []*raftpb.Message{{}}))
	require.Equal(t, []uint64{3}, got())

	// Second drop within the same burst is deduped.
	require.False(t, conn.pushMessages(0, []*raftpb.Message{{}}))
	require.Equal(t, []uint64{3}, got())

	// Simulate what sendMessages does on a successful stream.Send.
	conn.unreachableReported.Store(false)

	require.False(t, conn.pushMessages(0, []*raftpb.Message{{}}))
	require.Equal(t, []uint64{3, 3}, got(), "drop after successful send must re-signal Unreachable")
}

// newTestTransport builds a DefaultTransport with only the fields Send
// touches, plus a small unreachableCh we can drain to observe emissions.
// Peers map is empty on purpose — Send only enqueues to pendingSendQueue,
// it never consults t.peers directly, so peer registration isn't needed
// to exercise the drop path.
func newTestTransport(t *testing.T, pendingCap int) *DefaultTransport {
	t.Helper()

	meter := noop.NewMeterProvider().Meter("test")

	loadH, err := meter.Int64Histogram("noop.load")
	require.NoError(t, err)
	fullC, err := meter.Float64Counter("noop.full")
	require.NoError(t, err)

	return &DefaultTransport{
		logger:                   logging.Testing(),
		pendingSendQueue:         make(chan []*raftpb.Message, pendingCap),
		unreachableCh:            make(chan uint64, 16),
		pendingSendLoadHistogram: loadH,
		pendingSendFullCounter:   fullC,
		unreachableLoadHistogram: loadH,
		unreachableFullCounter:   fullC,
	}
}

// TestDefaultTransport_Send_EmitsUnreachablePerUniquePeer pins the
// systemic drop path: when pendingSendQueue is full, the batch is
// dropped and Raft must be told which peers were affected. Emit
// exactly once per unique msg.To in the dropped batch.
func TestDefaultTransport_Send_EmitsUnreachablePerUniquePeer(t *testing.T) {
	t.Parallel()

	// cap 0 → any Send hits the default branch.
	tr := newTestTransport(t, 0)

	tr.Send([]*raftpb.Message{
		{To: proto.Uint64(2)},
		{To: proto.Uint64(3)},
		{To: proto.Uint64(2)}, // duplicate — must not fire Unreachable again
		{To: proto.Uint64(5)},
	})

	// Drain unreachableCh.
	got := make(map[uint64]int)
	for {
		select {
		case peerID := <-tr.unreachableCh:
			got[peerID]++

			continue
		default:
		}

		break
	}

	require.Equal(t, map[uint64]int{2: 1, 3: 1, 5: 1}, got,
		"Send must emit Unreachable exactly once per unique msg.To in the dropped batch")
}

// TestDefaultTransport_Send_NoUnreachableOnHappyPath ensures the fix
// does not fire Unreachable when the batch enqueues cleanly.
func TestDefaultTransport_Send_NoUnreachableOnHappyPath(t *testing.T) {
	t.Parallel()

	tr := newTestTransport(t, 4)

	tr.Send([]*raftpb.Message{{To: proto.Uint64(2)}, {To: proto.Uint64(3)}})

	require.Len(t, tr.unreachableCh, 0, "happy path must not push to unreachableCh")
}
