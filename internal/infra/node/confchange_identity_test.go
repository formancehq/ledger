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
			if name == "missing identity" || name == "short identity" {
				require.ErrorContains(t, err, "invariant: ConfChange for peer 9 has invalid identity")
			} else {
				require.ErrorContains(t, err, "invariant: ConfChange registration for peer 9")
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
