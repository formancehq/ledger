package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
)

func TestNewNodeMarksFreshMembershipFormatBeforeRegisteringPeers(t *testing.T) {
	t.Parallel()
	setup := newTestApplierSetup(t)
	require.NoError(t, setup.wal.UpdateSnapshotConfState(&raftpb.ConfState{}))
	store := membership.NewPeerStore(setup.store)
	m, err := membership.NewMembershipWithServicePort(store, noopMemTransport{}, noopMemPool{},
		1, "self:7777", "self:8889", 8889, []byte("self-instance-id"), logging.Testing(), membership.NoopClusterPolicyProvider)
	require.NoError(t, err)

	n, err := NewNode(NodeConfig{
		NodeID: 1, Bootstrap: true, AdvertiseAddr: "self:7777",
		ServiceAdvertiseAddr: "self:8889", InstanceID: []byte("self-instance-id"),
		Peers: []Peer{{ID: 2, Address: "peer:7777", ServiceAddress: "peer:8891", InstanceID: []byte("peer-instance-id")}},
	}, nil, setup.applier, logging.Testing(), noop.Meter{}, setup.wal,
		setup.fsm, setup.applier.recovery, setup.applier.synchronizer, m, setup.responseSink)
	require.NoError(t, err)
	require.NotNil(t, n)
	require.NoError(t, m.Rehydrate())
	require.Equal(t, "peer:8891", m.PeerAddresses()[2].ServiceAddress)
	stored, err := store.LoadAll()
	require.NoError(t, err)
	require.Equal(t, "peer:8891", stored[2].ServiceAddress)
	// A fresh node must have written the format marker; a subsequent
	// constructor must preserve the distinct peer endpoint too.
	restarted, err := membership.NewMembershipWithServicePort(store, noopMemTransport{}, noopMemPool{},
		1, "self:7777", "self:8889", 8889, []byte("self-instance-id"), logging.Testing(), membership.NoopClusterPolicyProvider)
	require.NoError(t, err)
	require.Equal(t, "peer:8891", restarted.PeerAddresses()[2].ServiceAddress)
}
