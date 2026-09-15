package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
)

// TestFinishReadyCommittedUpdatePublishesPeerRegistration drives the actual
// commit-observer branch that owns cache and transport publication. It guards
// ConfChangeUpdateNode dispatch and proves the address refresh does not change
// the authoritative voter role.
func TestFinishReadyCommittedUpdatePublishesPeerRegistration(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	confState := &raftpb.ConfState{Voters: []uint64{1, 2}}
	require.NoError(t, setup.wal.UpdateSnapshotConfState(confState))

	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         setup.wal,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          NewLoggerAdapter(logging.Testing()),
	})
	require.NoError(t, err)

	m := newTestMembership(t)
	n := &Node{
		logger:       logging.Testing(),
		wal:          setup.wal,
		fsm:          setup.fsm,
		applier:      setup.applier,
		membership:   m,
		rawNode:      rawNode,
		indexTracker: NewIndexTracker(1),
	}
	n.confState.Store(confState)

	instanceID := []byte("new-instance-id-")
	ccContext, err := membership.MarshalConfChangeContext(membership.ConfChangeContext{
		RaftAddress:    "new:7000",
		ServiceAddress: "new:8000",
		InstanceID:     instanceID,
	})
	require.NoError(t, err)
	cc := &raftpb.ConfChangeV2{
		Changes: []*raftpb.ConfChangeSingle{{
			Type:   new(raftpb.ConfChangeUpdateNode),
			NodeId: proto.Uint64(2),
		}},
		Context: ccContext,
	}
	data, err := proto.Marshal(cc)
	require.NoError(t, err)
	entry := &raftpb.Entry{
		Index: proto.Uint64(1),
		Term:  proto.Uint64(1),
		Type:  new(raftpb.EntryConfChangeV2),
		Data:  data,
	}

	require.NoError(t, n.finishReady(readyResult{
		rd: raft.Ready{CommittedEntries: []*raftpb.Entry{entry}},
		confChanges: []committedConfChange{{
			index:  1,
			change: cc,
		}},
	}, setup.stop))

	registration := m.PeerAddresses()[2]
	require.Equal(t, "new:7000", registration.RaftAddress)
	require.Equal(t, "new:8000", registration.ServiceAddress)
	require.Equal(t, instanceID, registration.InstanceID)
	require.Equal(t, []uint64{1, 2}, n.confState.Load().GetVoters(),
		"UpdateNode must preserve the peer voter role")
	require.Empty(t, n.confState.Load().GetLearners())
}
