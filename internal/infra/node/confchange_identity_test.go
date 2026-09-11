package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
)

func TestFinishReadyRejectsIncompleteRegistrationBeforeRawNodeMutation(t *testing.T) {
	t.Parallel()
	for name, payload := range map[string]*membership.ConfChangeContext{
		"absent":                  nil,
		"correlation only":        {ProposalID: "promotion"},
		"identity only":           {InstanceID: []byte("peer-instance-id")},
		"missing service address": {RaftAddress: "peer:7777", InstanceID: []byte("peer-instance-id")},
		"missing identity":        {RaftAddress: "peer:7777", ServiceAddress: "peer:8888"},
		"short identity":          {RaftAddress: "peer:7777", ServiceAddress: "peer:8888", InstanceID: []byte("short")},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			storage := raft.NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &raftpb.ConfState{Voters: []uint64{1}}}}))
			rn, err := raft.NewRawNode(&raft.Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: storage, MaxInflightMsgs: 256, Logger: NewLoggerAdapter(logging.Testing())})
			require.NoError(t, err)
			n := &Node{rawNode: rn, logger: logging.Testing(), membership: newTestMembership(t)}
			n.confState.Store(&raftpb.ConfState{Voters: []uint64{1}})
			before := rn.Status().Config
			cc := &raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{Type: new(raftpb.ConfChangeAddNode), NodeId: new(uint64(9))}}}
			if payload != nil {
				cc.Context, err = membership.MarshalConfChangeContext(*payload)
				require.NoError(t, err)
			}
			err = n.finishReady(readyResult{confChanges: []committedConfChange{{index: 2, change: cc}}}, make(chan struct{}))
			switch name {
			case "missing identity", "short identity":
				require.ErrorContains(t, err, "invariant: ConfChange for peer 9 has invalid identity")
			case "absent":
				require.ErrorContains(t, err, "invariant: ConfChange registration for peer 9 has no payload")
			default:
				require.ErrorContains(t, err, "invariant: ConfChange registration for peer 9 requires raft and service addresses")
			}
			require.Equal(t, before, rn.Status().Config)
			require.Equal(t, []uint64{1}, n.confState.Load().GetVoters())
			require.Empty(t, n.membership.PeerAddresses())
		})
	}
}

func TestPromotionContextCarriesCompleteRegisteredIdentity(t *testing.T) {
	t.Parallel()
	m := newTestMembership(t)
	n := &Node{membership: m}
	_, err := n.promotionContext(9, "promotion")
	require.ErrorContains(t, err, "learner 9 has no membership row")
	require.NoError(t, m.Set(9, "peer:7777", "peer:8888", []byte("peer-instance-id")))
	for _, proposalID := range []string{"", "promotion"} {
		payload, err := n.promotionContext(9, proposalID)
		require.NoError(t, err)
		decoded, err := membership.UnmarshalConfChangeContext(payload)
		require.NoError(t, err)
		require.Equal(t, proposalID, decoded.ProposalID)
		require.Equal(t, "peer:7777", decoded.RaftAddress)
		require.Equal(t, "peer:8888", decoded.ServiceAddress)
		require.Equal(t, []byte("peer-instance-id"), decoded.InstanceID)
		require.NoError(t, membership.ValidateConfChangeIdentities(&raftpb.ConfChangeV2{Context: payload, Changes: []*raftpb.ConfChangeSingle{{Type: new(raftpb.ConfChangeAddNode), NodeId: new(uint64(9))}}}))
	}
}

func TestPromotionContextRejectsIncompleteAddresses(t *testing.T) {
	t.Parallel()
	for name, addresses := range map[string][2]string{
		"missing raft address":    {"", "peer:8888"},
		"missing service address": {"peer:7777", ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := newTestMembership(t)
			require.NoError(t, m.Set(9, addresses[0], addresses[1], []byte("peer-instance-id")))
			n := &Node{membership: m}
			payload, err := n.promotionContext(9, "promotion")
			require.Nil(t, payload)
			require.EqualError(t, err, "invariant: learner 9 requires raft and service addresses")
		})
	}
}

func TestFinishReadyCommittedRegistrationPublishesCompleteIdentity(t *testing.T) {
	t.Parallel()
	for _, changeType := range []raftpb.ConfChangeType{
		raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode, raftpb.ConfChangeUpdateNode,
	} {
		t.Run(changeType.String(), func(t *testing.T) {
			t.Parallel()
			n := newConfiguredPeersTestNode(t)
			n.logger = logging.Testing()
			target := uint64(9)
			if changeType == raftpb.ConfChangeUpdateNode {
				target = 2
			}
			payload := membership.ConfChangeContext{
				RaftAddress: "new:7777", ServiceAddress: "new:8888", InstanceID: []byte("next-instance-id"),
			}
			encoded, err := membership.MarshalConfChangeContext(payload)
			require.NoError(t, err)
			cc := &raftpb.ConfChangeV2{Context: encoded, Changes: []*raftpb.ConfChangeSingle{{
				Type: new(changeType), NodeId: new(target),
			}}}
			require.NoError(t, n.finishReady(readyResult{confChanges: []committedConfChange{{index: 2, change: cc}}}, make(chan struct{})))
			require.Equal(t, payload, n.membership.PeerAddresses()[target])
			progress, exists := n.rawNode.Status().Progress[target]
			require.True(t, exists)
			require.Equal(t, changeType != raftpb.ConfChangeAddNode, progress.IsLearner)
			_, persisted, err := n.wal.InitialState()
			require.NoError(t, err)
			require.True(t, confStatesEqual(n.confState.Load(), persisted), "published configuration must be persisted in the WAL snapshot")
		})
	}
}

func TestFinishReadyCommittedRemovalProtectsAdmissionBeforeApply(t *testing.T) {
	t.Parallel()
	setup := newTestApplierSetup(t)
	n := newConfiguredPeersTestNode(t)
	n.logger = logging.Testing()
	n.fsm = setup.fsm
	n.runDone = make(chan struct{})
	t.Cleanup(func() { close(n.runDone) })
	identity := []byte("peer-instance-id")
	encoded, err := membership.MarshalConfChangeContext(membership.ConfChangeContext{InstanceID: identity})
	require.NoError(t, err)
	cc := &raftpb.ConfChangeV2{Context: encoded, Changes: []*raftpb.ConfChangeSingle{{
		Type: new(raftpb.ConfChangeRemoveNode), NodeId: new(uint64(2)),
	}}}
	require.NoError(t, n.finishReady(readyResult{confChanges: []committedConfChange{{index: 42, change: cc}}}, make(chan struct{})))
	require.NotContains(t, n.rawNode.Status().Progress, uint64(2))
	require.NotContains(t, n.membership.PeerAddresses(), uint64(2))
	require.True(t, n.isRemovalPending(2, identity), "commit observation must protect admission even without an originating RPC")
	pending, exists := n.pendingRemovals.Load(2)
	require.True(t, exists)
	require.Equal(t, uint64(42), pending.committedIndex)
	require.Less(t, n.fsm.LastPersistedIndex(), uint64(42), "commit observation installs the barrier before the FSM applies the removal")
	_, persisted, err := n.wal.InitialState()
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, persisted.GetVoters())
	require.Empty(t, persisted.GetLearners())
}
