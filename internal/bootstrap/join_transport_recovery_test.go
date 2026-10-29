package bootstrap

import (
	"context"
	"crypto/tls"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	membershipapp "github.com/formancehq/ledger/v3/internal/application/membership"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/spool"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// The joining listener must remain closed through the actual production
// preflight. Committed registration has to retain a retry owner even though
// optional TLS cannot yet establish a TCP connection to the learner.
func TestJoinPreflightRecoversInitiallyUnavailableLearner(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	leaderListener := checkpointFenceListener(t)
	leader := newJoinRecoveryNode(t, 1, leaderListener.Addr().String(), nil)
	leader.start(t, ctx, leaderListener)
	require.NoError(t, leader.node.WaitLeaderReady(ctx))

	// Reserve an address, then close the socket rather than merely delaying
	// Serve: a listening socket would let the optional TLS TCP probe succeed.
	reservation := checkpointFenceListener(t)
	learnerAddr := reservation.Addr().String()
	require.NoError(t, reservation.Close())
	learner := newJoinRecoveryNode(t, 2, learnerAddr, []node.Peer{{
		ID: 1, Address: leader.addr, ServiceAddress: leader.addr, InstanceID: leader.instanceID,
	}})
	_, port, err := net.SplitHostPort(learnerAddr)
	require.NoError(t, err)
	servicePort, err := strconv.Atoi(port)
	require.NoError(t, err)
	cfg := Config{RaftConfig: learner.config, GRPCPort: servicePort, ClusterID: "join-recovery", TLSConfig: TLSConfig{Mode: TLSModeDisabled}}
	require.True(t, shouldRunJoinPreflight(cfg, logging.Testing()))
	require.NoError(t, joinPreflightHook(cfg, logging.Testing()).OnStart(ctx))

	// Preflight success means durable membership, not peer reachability. Read
	// both the real WAL ConfState and the persisted membership projection.
	hardState, confState, err := leader.wal.InitialState()
	require.NoError(t, err)
	require.Contains(t, confState.GetLearners(), uint64(2))
	additionIndex := hardState.GetCommit()
	require.Positive(t, additionIndex)
	before, err := leader.node.GetClusterState(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, before.GetRaftStatus().GetLastPersistedIndex(), additionIndex)
	rows, err := membership.NewPeerStore(leader.store).LoadAll()
	require.NoError(t, err)
	require.Equal(t, learnerAddr, rows[2].RaftAddress)
	require.Equal(t, learner.instanceID, rows[2].InstanceID)
	identity, err := wal.ReadInstanceID(learner.config.WalDir)
	require.NoError(t, err)
	require.Equal(t, learner.instanceID, identity)
	require.True(t, wal.IsClusterJoined(learner.config.WalDir))
	require.Nil(t, leader.pool.GetConnection(2), "the withheld listener must cause the first optional TLS probe to fail")
	t.Logf("learner registration committed at term %d, index %d; durable applied index %d", before.GetRaftStatus().GetTerm(), additionIndex, before.GetRaftStatus().GetLastPersistedIndex())

	listener, err := net.Listen("tcp", learnerAddr)
	require.NoError(t, err)
	learner.start(t, ctx, listener)

	// No registration, restart, or proposal from the test follows listener
	// startup. The leader's retained loop must deliver the committed log and
	// the production promotion ticker must observe the catch-up itself.
	require.Eventually(t, func() bool {
		status, err := leader.node.GetClusterState(ctx)
		if err != nil {
			return false
		}
		for _, peer := range status.GetNodes() {
			if peer.GetId() == 2 {
				return peer.GetSuffrage() == "Voter" && peer.GetProgress().GetMatch() >= additionIndex && learner.node.LastPersistedIndex() >= additionIndex
			}
		}

		return false
	}, 10*time.Second, 10*time.Millisecond, "the learner must catch up and be promoted after its listener opens")
	_, promoted, err := leader.wal.InitialState()
	require.NoError(t, err)
	require.Contains(t, promoted.GetVoters(), uint64(2))
	require.NotContains(t, promoted.GetLearners(), uint64(2))
}

type joinRecoveryNode struct {
	node       *node.Node
	member     *membership.Membership
	transport  *node.DefaultTransport
	pool       *transport.ConnectionPool
	server     *grpc.Server
	wal        *wal.DefaultWAL
	store      *dal.Store
	config     node.NodeConfig
	addr       string
	instanceID []byte
}

func newJoinRecoveryNode(t *testing.T, id uint64, addr string, peers []node.Peer) *joinRecoveryNode {
	t.Helper()
	logger := logging.Testing()
	meters := noop.NewMeterProvider()
	meter := meters.Meter("join-recovery")
	store := newTestStore(t)
	waldir := t.TempDir()
	instanceID, err := wal.EnsureInstanceID(waldir)
	require.NoError(t, err)
	w, err := wal.New(waldir, logger, meter)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	sp, err := spool.NewDefault(spool.DefaultSpoolConfig{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sp.Close()) })
	pool := transport.NewConnectionPool(transport.TLSPolicy{TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}, transport.PoolConfig{})
	servicePool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	t.Cleanup(func() { require.NoError(t, servicePool.Close()) })
	tr := node.NewTransport(logger, pool, meters, id, node.TransportConfig{Reception: []int{32, 32, 32}, Send: []int{32, 32, 32}}, "join-recovery", 1024, addr, addr)
	member, err := membership.NewMembership(membership.NewPeerStore(store), tr, servicePool, id, addr, addr, instanceID, logger)
	require.NoError(t, err)
	cacheStore, err := cache.New(1000, meter)
	require.NoError(t, err)
	registry := state.NewStateRegistry(cacheStore, attributes.New())
	machine, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil), store, dal.NewSentinelFactory(store, false), meters, nil, state.NewSharedState(), signal.NewNotifications(), nil, "join-recovery", 0, member.WriteConfChange)
	require.NoError(t, err)
	t.Cleanup(machine.Close)
	recovery := state.NewRecovery(machine, store)
	require.NoError(t, recovery.RecoverState())
	synchronizer := state.NewSynchronizer(machine, recovery, dal.NewIncomingRestoreFactory(store))
	responses := node.NewLocalResponses()
	applier, err := node.NewApplier(machine, recovery, synchronizer, sp, store, w, logger, meter, 1000, 1000, nil, member, responses)
	require.NoError(t, err)
	cfg := node.NodeConfig{NodeID: id, Peers: peers, Bootstrap: len(peers) == 0, WalDir: waldir, AdvertiseAddr: addr, ServiceAdvertiseAddr: addr, InstanceID: instanceID, TickInterval: 10 * time.Millisecond, ProcessingTickInterval: time.Millisecond, MaintenanceInterval: time.Hour, AutoPromoteThreshold: 1}
	n, err := node.NewNode(cfg, tr, applier, logger, meter, w, machine, recovery, synchronizer, member, responses)
	require.NoError(t, err)
	server := grpc.NewServer()
	tr.RegisterRaftService(server)
	grpcadp.RegisterClusterBootstrapService(server, grpcadp.NewClusterBootstrapServiceServer(n, tr, membershipapp.NewService(n, member, logger, addr, addr), logger, "join-recovery"))
	t.Cleanup(server.Stop)

	return &joinRecoveryNode{node: n, member: member, transport: tr, pool: pool, server: server, wal: w, store: store, config: cfg, addr: addr, instanceID: instanceID}
}

func (fixture *joinRecoveryNode) start(t *testing.T, ctx context.Context, listener net.Listener) {
	t.Helper()
	serveCheckpointFenceRPCListener(t, fixture.server, listener)
	go fixture.transport.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, fixture.transport.Stop(ctx))
	})
	fixture.member.Start()
	hook := nodeHook(fixture.node, logging.Testing())
	require.NoError(t, hook.OnStart(ctx))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, hook.OnStop(ctx))
	})
}
