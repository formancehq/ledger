package node

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func newSyncDonorTestNode(t *testing.T, setup *testApplierSetup, voters []uint64, lead uint64) *Node {
	t.Helper()

	n, err := NewNode(NodeConfig{
		NodeID: 1, AdvertiseAddr: "node-1:7000", ServiceAdvertiseAddr: "node-1:8000",
		InstanceID: []byte("0000000000000001"), TickInterval: time.Hour,
		ProcessingTickInterval: time.Hour, MaintenanceInterval: time.Hour,
	}, NewMockTransport(gomock.NewController(t)), setup.applier, logging.Testing(),
		noop.NewMeterProvider().Meter("sync-donor"),
		setup.wal, setup.fsm, setup.applier.recovery, setup.applier.synchronizer,
		newTestMembership(t), setup.responseSink)
	require.NoError(t, err)

	n.confState.Store(&raftpb.ConfState{Voters: voters})
	n.lastSoftState.Store(&raft.SoftState{Lead: lead})

	return n
}

// TestSyncDonor pins the donor rotation: the known leader first, then every
// other voter in turn, and never the node itself — a node that has just won
// an election while out of sync has no leader but itself.
func TestSyncDonor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		voters []uint64
		lead   uint64
		want   []uint64
	}{
		{"leader first then the other voters", []uint64{1, 2, 3}, 2, []uint64{2, 3, 2, 3}},
		{"self as leader is skipped", []uint64{1, 2, 3}, 1, []uint64{2, 3, 2}},
		{"no leader rotates the voters", []uint64{1, 2, 3}, 0, []uint64{2, 3, 2}},
		{"learner self rotates every voter", []uint64{2, 3, 4}, 3, []uint64{3, 3, 4, 2}},
		{"sole voter has no donor", []uint64{1}, 0, []uint64{0, 0}},
		{"sole voter is never its own donor", []uint64{1}, 1, []uint64{0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n := newSyncDonorTestNode(t, newTestApplierSetup(t), tt.voters, tt.lead)

			got := make([]uint64, 0, len(tt.want))
			for attempt := range uint64(len(tt.want)) {
				got = append(got, n.syncDonor(attempt))
			}

			require.Equal(t, tt.want, got)
		})
	}
}

// TestRetryReadySnapshotSyncRotatesDonors drives the out-of-sync retry from
// a node that leads its own term: the request must target another voter,
// a Ready inside syncRetryInterval must not enqueue a second request, and
// the next attempt must move on to the remaining voter.
func TestRetryReadySnapshotSyncRotatesDonors(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	n := newSyncDonorTestNode(t, setup, []uint64{1, 2, 3}, 1)
	setup.applier.setOutOfSync()

	n.retryReadySnapshotSync(setup.stop, raft.Ready{})
	require.Equal(t, uint64(2), (<-setup.applier.ch).syncLeader)

	n.retryReadySnapshotSync(setup.stop, raft.Ready{})
	select {
	case work := <-setup.applier.ch:
		t.Fatalf("second attempt enqueued within syncRetryInterval: donor %d", work.syncLeader)
	default:
	}

	n.lastSyncAttempt = time.Time{}
	n.retryReadySnapshotSync(setup.stop, raft.Ready{})
	require.Equal(t, uint64(3), (<-setup.applier.ch).syncLeader)
}

// TestRetryReadySnapshotSyncResetsEpisodeOnNormal checks that a Ready seen in
// statusNormal starts the next episode over from the leader.
func TestRetryReadySnapshotSyncResetsEpisodeOnNormal(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	n := newSyncDonorTestNode(t, setup, []uint64{1, 2, 3}, 3)
	n.syncDonorAttempts = 5

	n.retryReadySnapshotSync(setup.stop, raft.Ready{})
	require.Zero(t, n.syncDonorAttempts)

	setup.applier.setOutOfSync()
	n.retryReadySnapshotSync(setup.stop, raft.Ready{})
	require.Equal(t, uint64(3), (<-setup.applier.ch).syncLeader)
}
