package membership_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	transportpkg "github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestCommittedPeerRefreshUpdatesRaftTransport covers the two-stage committed
// ConfChange contract: finishReady publishes the registration through
// Membership.Set before asynchronous FSM apply persists it. The operational
// Raft transport must replace its dial address without adding a second peer or
// disturbing the membership row's identity, then Pebble must converge.
func TestCommittedPeerRefreshUpdatesRaftTransport(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	meterProvider := noop.NewMeterProvider()
	store, err := dal.NewStore(t.TempDir(), logger, meterProvider.Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	raftPool := transportpkg.NewConnectionPool(transportpkg.TLSPolicy{}, transportpkg.PoolConfig{})
	raftTransport := node.NewTransport(
		logger,
		raftPool,
		meterProvider,
		1,
		node.TransportConfig{Reception: []int{1, 1, 1}, Send: []int{1, 1, 1}},
		"test-cluster",
		1,
		"self:7000",
		"self:8000",
	)
	t.Cleanup(func() {
		raftTransport.RemovePeer(context.Background(), 2)
		require.NoError(t, raftPool.Close())
	})

	servicePool := transportpkg.NewConnectionPool(transportpkg.TLSPolicy{}, transportpkg.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, servicePool.Close()) })

	peerStore := membership.NewPeerStore(store)
	m, err := membership.NewMembership(
		peerStore,
		raftTransport,
		servicePool,
		1,
		"self:7000",
		"self:8000",
		[]byte("self-instance-id"),
		logger,
	)
	require.NoError(t, err)
	m.Start()

	oldInstanceID := []byte("old-instance-id-")
	require.NoError(t, m.Register(2, "old:7000", "old:8000", oldInstanceID))
	require.Equal(t, "old:7000", raftPool.GetPeerAddress(2))

	newInstanceID := []byte("new-instance-id-")
	ccContext, err := membership.MarshalConfChangeContext(membership.ConfChangeContext{
		RaftAddress:    "new:7000",
		ServiceAddress: "new:8000",
		InstanceID:     newInstanceID,
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

	// Node.finishReady performs this Set as soon as it observes the commit,
	// before submitting the entry to the asynchronous FSM applier.
	require.NoError(t, m.Set(2, "new:7000", "new:8000", newInstanceID))

	require.Equal(t, "new:7000", m.PeerAddresses()[2].RaftAddress)
	require.Equal(t, newInstanceID, m.PeerAddresses()[2].InstanceID)
	require.Equal(t, "new:7000", raftPool.GetPeerAddress(2),
		"the next Raft dial must use the committed address")
	require.Equal(t, "new:8000", servicePool.GetPeerAddress(2),
		"the service pool must converge with the same committed registration")
	persisted, err := peerStore.LoadAll()
	require.NoError(t, err)
	require.Equal(t, "old:7000", persisted[2].RaftAddress,
		"Pebble changes only when the asynchronous FSM batch applies")

	session := store.OpenWriteSession()
	require.NoError(t, m.WriteConfChange(&raftpb.Entry{
		Type: new(raftpb.EntryConfChangeV2),
		Data: data,
	}, session))
	require.NoError(t, session.Commit())

	persisted, err = peerStore.LoadAll()
	require.NoError(t, err)
	require.Equal(t, "new:7000", persisted[2].RaftAddress)
	require.Equal(t, newInstanceID, persisted[2].InstanceID)
	require.Equal(t, []uint64{2}, raftPool.PeerIDs(),
		"an address refresh must preserve the single peer identity")
	require.Equal(t, []uint64{2}, servicePool.PeerIDs())
}
