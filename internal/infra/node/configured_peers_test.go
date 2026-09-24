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
	// Raft delivers its self-vote only after the election HardState is
	// durable. Drain the election/no-op Ready cycles before using the leader.
	for rawNode.HasReady() {
		ready := rawNode.Ready()
		require.NoError(t, w.Append(ready.HardState, ready.Entries))
		rawNode.Advance(ready)
	}
	require.Equal(t, raft.StateLeader, rawNode.Status().RaftState)

	m := newTestMembership(t)
	require.NoError(t, m.Set(1, "self:7777", "self:8888", []byte("self-instance-id")))
	require.NoError(t, m.Set(2, "old:7777", "old:8888", []byte("peer-instance-id")))
	// A cached row alone is not evidence of a configured member.
	require.NoError(t, m.Set(3, "extra:7777", "extra:8888", []byte("extra-instanceid")))

	return &Node{
		rawNode:          rawNode,
		wal:              w,
		membership:       m,
		clusterCommandCh: make(chan *clusterCommand),
	}
}

func TestGetConfiguredPeers_FollowerDoesNotPublishLeaderView(t *testing.T) {
	t.Parallel()

	n := newConfiguredPeersTestNode(t)
	storage := raft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2}},
	}}))
	var err error
	n.rawNode, err = raft.NewRawNode(&raft.Config{
		ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: storage,
		MaxInflightMsgs: 256, Logger: NewLoggerAdapter(logging.Testing()),
	})
	require.NoError(t, err)
	require.Equal(t, raft.StateFollower, n.rawNode.Status().RaftState)
	results := make(chan []Peer, 1)
	errors := make(chan error, 1)
	go func() {
		peers, err := n.GetConfiguredPeers(t.Context())
		results <- peers
		errors <- err
	}()
	cmd := <-n.clusterCommandCh
	cmd.errCh <- cmd.fn()
	require.NoError(t, <-errors)
	require.Empty(t, <-results, "local cached peers must not masquerade as leader discovery")
	require.NotEmpty(t, n.membership.PeerAddresses())
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


func TestGetConfiguredPeers_TopologySnapshotIsAtomicWithConcurrentRehydrate(t *testing.T) {
	t.Parallel()

	// Regression test for the concurrency window between rawNode.Status() and
	// membership.PeerAddresses() in the pre-fix GetConfiguredPeers. The fix
	// wraps both reads inside membership.WithPeerAddresses (which holds m.mu.RLock
	// for the duration), so a concurrent Rehydrate / OnSnapshotInstalled calling
	// m.mu.Lock cannot interleave and produce a mixed snapshot.
	//
	// This test runs a concurrent Set() that races with cmd.fn(). Either:
	//   • Set() wins: fn() sees the new addresses (new:7777 / next-instance-id).
	//   • fn()  wins: fn() sees the old addresses (old:7777 / peer-instance-id).
	// In both cases the result must be internally consistent: the address and
	// identity for peer 2 must belong to the same incarnation, never a mix.
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

	cmd := <-n.clusterCommandCh

	// Start concurrent Set() immediately — races with cmd.fn().
	// WithPeerAddresses ensures the two views (Status + addresses) are captured
	// under a single RLock, so Set() either runs entirely before or entirely after
	// the snapshot, never in between.
	setDone := make(chan struct{})
	go func() {
		require.NoError(t, n.membership.Set(2, "new:7777", "new:8888", []byte("next-instance-id")))
		close(setDone)
	}()

	err := cmd.fn()
	require.NoError(t, err)
	cmd.errCh <- err

	<-setDone
	got := <-results
	require.NoError(t, got.err)

	// Find peer 2 in the result.
	var peer2 *Peer
	for i := range got.peers {
		if got.peers[i].ID == 2 {
			peer2 = &got.peers[i]
			break
		}
	}
	require.NotNil(t, peer2, "peer 2 must be present in the topology snapshot")

	// The snapshot must be one consistent incarnation: address and identity must match.
	switch peer2.Address {
	case "old:7777":
		require.Equal(t, []byte("peer-instance-id"), peer2.InstanceID,
			"old address must pair with old identity, never a mixed incarnation")
	case "new:7777":
		require.Equal(t, []byte("next-instance-id"), peer2.InstanceID,
			"new address must pair with new identity, never a mixed incarnation")
	default:
		t.Fatalf("unexpected address for peer 2: %q", peer2.Address)
	}
}

