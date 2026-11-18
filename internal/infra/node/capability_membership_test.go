package node

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestCapabilityMembershipWaitsForSelfRefreshCommit(t *testing.T) {
	t.Parallel()
	for _, singleMember := range []bool{false, true} {
		t.Run(strconv.FormatBool(singleMember), func(t *testing.T) {
			t.Parallel()
			raftNode := newConfiguredPeersTestNode(t)
			raftNode.logger = logging.Testing()
			raftNode.config = NodeConfig{NodeID: 1, AdvertiseAddr: "self:7777", ServiceAdvertiseAddr: "self:8888", InstanceID: []byte("self-instance-id")}
			setup := newTestApplierSetupWithConfChangeHandler(t, make(LocalResponses, 1024), func(entry *raftpb.Entry, session *dal.WriteSession) error {
				return raftNode.membership.WriteConfChange(entry, session)
			})
			var err error
			raftNode.membership, err = membership.NewMembership(membership.NewPeerStore(setup.store), noopMemTransport{}, noopMemPool{}, 1, "self:7777", "self:8888", []byte("self-instance-id"), logging.Testing(), membership.NoopClusterPolicyProvider)
			require.NoError(t, err)
			raftNode.fsm = setup.fsm
			confState := &raftpb.ConfState{Voters: []uint64{1}}
			if !singleMember {
				confState.Learners = []uint64{2}
			}
			raftNode.confState.Store(confState)
			require.NoError(t, raftNode.membership.Register(2, "peer:7777", "peer:8888", []byte("peer-instance-id"), []uint32{1}))
			require.NoError(t, raftNode.membership.Register(1, "self:7777", "self:8888", []byte("self-instance-id"), []uint32{1}))
			ready, err := capabilities.Ready([]capabilities.CapabilityID{1}, raftNode.CapabilityMembers())
			require.NoError(t, err)
			require.Empty(t, ready, "local self registration cannot arm activation")
			failed, cancel := context.WithCancel(t.Context())
			cancel()
			require.ErrorIs(t, raftNode.RefreshSelfMembership(failed, []uint32{1}), context.Canceled)
			require.Empty(t, raftNode.membership.PeerAddresses()[1].Capabilities)
			results := make(chan error, 1)
			go func() { results <- raftNode.RefreshSelfMembership(t.Context(), []uint32{1}) }()
			command := <-raftNode.clusterCommandCh
			require.NoError(t, command.fn())
			var change *raftpb.Entry
			for _, entry := range raftNode.rawNode.Ready().Entries {
				if entry.GetType() == raftpb.EntryConfChangeV2 {
					change = entry
				}
			}
			require.NotNil(t, change)
			ready, err = capabilities.Ready([]capabilities.CapabilityID{1}, raftNode.CapabilityMembers())
			require.NoError(t, err)
			require.Empty(t, ready, "proposing self refresh is not committed support")
			confChange, ok, err := membership.UnmarshalConfChangeV2(change)
			require.NoError(t, err)
			require.True(t, ok)
			pending, err := raftNode.takePendingConfChange(confChange, change.GetIndex())
			require.NoError(t, err)
			prefix, err := raftNode.wal.Entries(1, change.GetIndex(), ^uint64(0))
			require.NoError(t, err)
			_, err = setup.fsm.ApplyEntries(t.Context(), setup.store, append(prefix, change)...)
			require.NoError(t, err)
			persisted, err := membership.NewPeerStore(setup.store).LoadAll()
			require.NoError(t, err)
			require.Equal(t, []uint32{1}, persisted[1].Capabilities)
			payload, err := membership.UnmarshalConfChangeContext(confChange.GetContext())
			require.NoError(t, err)
			require.NoError(t, raftNode.membership.Set(1, payload.RaftAddress, payload.ServiceAddress, payload.InstanceID, payload.Capabilities))
			pending.future.Resolve(pending.index, nil)
			command.errCh <- nil
			require.NoError(t, receiveForceRemoveError(t, results))
			ready, err = capabilities.Ready([]capabilities.CapabilityID{1}, raftNode.CapabilityMembers())
			require.NoError(t, err)
			require.Equal(t, []capabilities.CapabilityID{1}, ready)
		})
	}
}

func TestCapabilityMembershipFenceHeldUntilPolicyApply(t *testing.T) {
	n := &Node{stopChannel: make(chan struct{}), terminalCh: make(chan struct{})}
	n.confChangeMu.Configure(nil, n.stopChannel, n.terminalCh)
	release, err := n.confChangeMu.AcquireClusterPolicy(context.Background())
	require.NoError(t, err)
	future := futures.New[state.ApplyResult]()
	release(future)
	require.False(t, n.confChangeMu.TryLock(), "a learner must not pass a pending activation")

	future.Resolve(state.ApplyResult{}, nil)
	require.Eventually(t, func() bool {
		if !n.confChangeMu.TryLock() {
			return false
		}
		n.confChangeMu.Unlock()

		return true
	}, time.Second, time.Millisecond)
}

func TestCapabilityMembershipFenceReleasedWithoutProposal(t *testing.T) {
	n := &Node{}
	release, err := n.confChangeMu.AcquireClusterPolicy(context.Background())
	require.NoError(t, err)
	release(nil)
	require.True(t, n.confChangeMu.TryLock())
	n.confChangeMu.Unlock()
}
