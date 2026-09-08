package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// A crash after processReady appends the committed removal but before
// finishReady persists ConfState leaves the snapshot describing the old voter
// set. WAL replay removes the peer row before startup membership validation.
func TestNewNodeAcceptsReplayedRemovalBeforeConfStatePersistence(t *testing.T) {
	t.Parallel()

	var m *membership.Membership
	setup := newTestApplierSetupWithConfChangeHandler(t, make(LocalResponses, 1024), func(entry *raftpb.Entry, session *dal.WriteSession) error {
		return m.WriteConfChange(entry, session)
	})
	var err error
	m, err = membership.NewMembership(membership.NewPeerStore(setup.store), noopMemTransport{}, noopMemPool{},
		1, "self:7777", "self:8888", []byte("self-instance-id"), logging.Testing())
	require.NoError(t, err)
	identity := []byte("peer-instance-id")
	require.NoError(t, m.Register(2, "peer:7777", "peer:8888", identity))
	require.NoError(t, setup.wal.UpdateSnapshotConfState(&raftpb.ConfState{Voters: []uint64{1, 2}}))
	context, err := membership.MarshalConfChangeContext(membership.ConfChangeContext{InstanceID: identity})
	require.NoError(t, err)
	data, err := proto.Marshal(&raftpb.ConfChange{Type: new(raftpb.ConfChangeRemoveNode), NodeId: new(uint64(2)), Context: context})
	require.NoError(t, err)
	require.NoError(t, setup.wal.Append(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}, []*raftpb.Entry{{
		Index: new(uint64(1)), Term: new(uint64(1)), Type: new(raftpb.EntryConfChange), Data: data,
	}}))

	n, err := NewNode(NodeConfig{
		NodeID: 1, AdvertiseAddr: "self:7777", ServiceAdvertiseAddr: "self:8888", InstanceID: []byte("self-instance-id"),
	}, nil, setup.applier, logging.Testing(), noop.Meter{}, setup.wal,
		setup.fsm, setup.applier.recovery, setup.applier.synchronizer, m, setup.responseSink)
	require.NoError(t, err)
	require.NotNil(t, n)
	_, present := m.GetInstanceID(2)
	require.False(t, present, "replayed removal must delete the membership row")
	removed, err := m.IsRemoved(2, identity)
	require.NoError(t, err)
	require.True(t, removed, "replayed removal must persist its tombstone")
}
