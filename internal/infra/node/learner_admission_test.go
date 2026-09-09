package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
)

func TestAddLearnerRejectsRemovedAndActiveIncarnations(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		boot    bool
		fresh   bool
		pending bool
		removed bool
		want    error
	}{
		{name: "committed removal awaiting apply", pending: true, want: ErrNodeRemoved},
		{name: "durable removed identity", removed: true, want: ErrNodeRemoved},
		{name: "administrative retry of active identity", want: ErrNodeAlreadyInCluster},
		{name: "administrative replacement with stale progress", fresh: true, want: ErrNodeStaleProgress},
		{name: "boot retry with stale progress", boot: true, want: ErrNodeStaleProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := newConfiguredPeersTestNode(t)
			n.logger = logging.Testing()
			identity := []byte("peer-instance-id")
			if tc.fresh {
				identity = []byte("next-instance-id")
			}
			if tc.pending {
				n.pendingRemovals.Store(2, &pendingRemoval{instanceID: identity, committedIndex: 42})
				removed, err := n.membership.IsRemoved(2, identity)
				require.NoError(t, err)
				require.False(t, removed, "pending admission must reject before a tombstone exists")
			}
			if tc.removed {
				require.NoError(t, n.membership.UnregisterAndBlacklist(2, identity, 1))
			}
			ack := msgWithIndex(raftpb.MsgAppResp, 2, 1, 1)
			ack.Term = new(n.rawNode.Status().GetTerm())
			require.NoError(t, n.rawNode.Step(ack))
			before := n.rawNode.Status()
			require.Positive(t, before.Progress[2].Match)
			addresses := n.membership.PeerAddresses()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			results := make(chan error, 1)
			go func() {
				results <- n.addLearner(ctx, 2, "new:7777", "new:8888", identity, tc.boot)
			}()
			select {
			case cmd := <-n.clusterCommandCh:
				cmd.errCh <- cmd.fn()
			case <-ctx.Done():
				t.Fatal("learner admission did not reach orchestrate")
			}
			require.ErrorIs(t, receiveForceRemoveError(t, results), tc.want)
			require.Equal(t, before, n.rawNode.Status())
			require.Equal(t, addresses, n.membership.PeerAddresses(), "rejected address changes must not reach the cache")
		})
	}
}

func TestAddLearnerZeroProgressRefreshProposesCompleteReplacement(t *testing.T) {
	t.Parallel()
	n := newConfiguredPeersTestNode(t)
	n.logger = logging.Testing()
	require.Zero(t, n.rawNode.Status().Progress[2].Match)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	results := make(chan error, 1)
	go func() {
		results <- n.AddLearner(ctx, 2, "new:7777", "new:8888", []byte("next-instance-id"))
	}()
	cmd := <-n.clusterCommandCh
	require.NoError(t, cmd.fn())
	ready := n.rawNode.Ready()
	var changes []*raftpb.Entry
	for _, entry := range ready.Entries {
		if entry.GetType() == raftpb.EntryConfChangeV2 {
			changes = append(changes, entry)
		}
	}
	require.Len(t, changes, 1)
	cc, ok, err := membership.UnmarshalConfChangeV2(changes[0])
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, membership.ValidateConfChangeIdentities(cc))
	require.Len(t, cc.GetChanges(), 1)
	require.Equal(t, raftpb.ConfChangeUpdateNode, cc.GetChanges()[0].GetType())
	require.Equal(t, uint64(2), cc.GetChanges()[0].GetNodeId())
	payload, err := membership.UnmarshalConfChangeContext(cc.GetContext())
	require.NoError(t, err)
	require.Equal(t, "new:7777", payload.RaftAddress)
	require.Equal(t, "new:8888", payload.ServiceAddress)
	require.Equal(t, []byte("next-instance-id"), payload.InstanceID)
	require.NotEmpty(t, payload.ProposalID)
	require.Equal(t, "old:7777", n.membership.PeerAddresses()[2].RaftAddress, "proposal alone cannot publish new routing")

	pending, err := n.takePendingConfChange(cc, changes[0].GetIndex())
	require.NoError(t, err)
	require.NotNil(t, pending)
	pending.future.Resolve(pending.index, nil)
	cmd.errCh <- nil
	require.NoError(t, receiveForceRemoveError(t, results))
}
