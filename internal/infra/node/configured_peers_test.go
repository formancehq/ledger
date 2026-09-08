package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

func newConfiguredPeersTestNode(t *testing.T) *Node {
	t.Helper()

	w, err := wal.New(t.TempDir(), logging.Testing(), noop.Meter{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	require.NoError(t, w.CreateSnapshot(0, &raftpb.ConfState{
		Voters: []uint64{1}, Learners: []uint64{2},
	}, nil))

	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         w,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          NewLoggerAdapter(logging.Testing()),
	})
	require.NoError(t, err)
	require.NoError(t, rawNode.Campaign())
	require.Equal(t, raft.StateLeader, rawNode.Status().RaftState)

	m := newTestMembership(t)
	require.NoError(t, m.Set(1, "self:7777", "self:8888", []byte("self-instance-id")))
	require.NoError(t, m.Set(2, "old:7777", "old:8888", []byte("peer-instance-id")))
	// A cached row alone is not evidence of a configured member.
	require.NoError(t, m.Set(3, "extra:7777", "extra:8888", []byte("extra-instanceid")))

	return &Node{
		rawNode:          rawNode,
		membership:       m,
		clusterCommandCh: make(chan *clusterCommand),
	}
}

func TestGetConfiguredPeers_SnapshotSurvivesMembershipChange(t *testing.T) {
	t.Parallel()

	for _, replace := range []bool{false, true} {
		name := "removed"
		if replace {
			name = "replaced"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			n := newConfiguredPeersTestNode(t)
			type result struct {
				peers []Peer
				err   error
			}
			results := make(chan result, 1)
			go func() {
				peers, err := n.GetConfiguredPeers(t.Context())
				results <- result{peers: peers, err: err}
			}()

			// Run the command as orchestrate would, but hold its response until
			// the next configuration change. The caller must still receive the
			// complete old peer row, never a missing row or the new incarnation.
			cmd := <-n.clusterCommandCh
			err := cmd.fn()
			require.NoError(t, err)
			n.rawNode.ApplyConfChange(&raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{
				Type: new(raftpb.ConfChangeRemoveNode), NodeId: new(uint64(2)),
			}}})
			n.membership.Remove(2)
			if replace {
				n.rawNode.ApplyConfChange(&raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{
					Type: new(raftpb.ConfChangeAddLearnerNode), NodeId: new(uint64(2)),
				}}})
				require.NoError(t, n.membership.Set(2, "new:7777", "new:8888", []byte("next-instance-id")))
			}
			cmd.errCh <- err

			got := <-results
			require.NoError(t, got.err)
			require.ElementsMatch(t, []Peer{
				{ID: 1, Address: "self:7777", ServiceAddress: "self:8888", InstanceID: []byte("self-instance-id")},
				{ID: 2, Address: "old:7777", ServiceAddress: "old:8888", InstanceID: []byte("peer-instance-id")},
			}, got.peers)
		})
	}
}

func TestGetConfiguredPeers_RejectsMissingConfiguredIdentity(t *testing.T) {
	t.Parallel()

	n := newConfiguredPeersTestNode(t)
	n.membership.Remove(2)
	results := make(chan error, 1)
	go func() {
		_, err := n.GetConfiguredPeers(t.Context())
		results <- err
	}()
	cmd := <-n.clusterCommandCh
	cmd.errCh <- cmd.fn()
	require.ErrorContains(t, <-results, "invariant: cluster member 2 has no membership row")
}
