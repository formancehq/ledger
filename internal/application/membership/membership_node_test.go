package membership

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	inframembership "github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/spool"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// newMembershipServiceNode runs a single voter through its real WAL, FSM, and
// orchestrate loops. A learner can be registered without a remote process:
// only the existing voter contributes to the registration's commit quorum.
func newMembershipServiceNode(t *testing.T) *Service {
	t.Helper()
	require.Equal(t, wal.InstanceIDLen, inframembership.InstanceIDLen, "WAL and membership must agree on persisted identity length")

	logger := logging.Testing()
	meters := noop.NewMeterProvider()
	meter := meters.Meter("membership-service-test")
	w, err := wal.New(t.TempDir(), logger, meter)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	require.NoError(t, w.CreateSnapshot(0, &raftpb.ConfState{Voters: []uint64{1}}, nil))
	sp, err := spool.NewDefault(spool.DefaultSpoolConfig{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sp.Close()) })
	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	raftTransport := node.NewTransport(logger, pool, meters, 1, node.TransportConfig{
		Reception: []int{16, 16, 16}, Send: []int{16, 16, 16},
	}, "membership-service-test", 1024, "self:7777", "self:8888")
	transportDone := make(chan struct{})
	go func() {
		defer close(transportDone)
		raftTransport.Start(context.Background())
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, raftTransport.Stop(ctx))
		select {
		case <-transportDone:
		case <-ctx.Done():
			t.Error("membership transport did not stop")
		}
	})
	m, err := inframembership.NewMembership(inframembership.NewPeerStore(store), raftTransport, pool,
		1, "self:7777", "self:8888", []byte("self-instance-id"), logger)
	require.NoError(t, err)

	c, err := cache.New(1000, nil)
	require.NoError(t, err)
	registry := state.NewStateRegistry(c, attributes.New(), 0)
	fsm, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil),
		store, dal.NewSentinelFactory(store, false), meters, nil, state.NewSharedState(),
		signal.NewNotifications(), nil, "membership-service-test", 0, m.WriteConfChange)
	require.NoError(t, err)
	recovery := state.NewRecovery(fsm, store)
	require.NoError(t, recovery.RecoverState())
	synchronizer := state.NewSynchronizer(fsm, recovery, dal.NewIncomingRestoreFactory(store))
	responses := node.NewLocalResponses()
	applier, err := node.NewApplier(fsm, recovery, synchronizer, sp, store, w, logger, meter,
		0, 1000, nil, func() {}, responses)
	require.NoError(t, err)
	n, err := node.NewNode(node.NodeConfig{
		NodeID: 1, AdvertiseAddr: "self:7777", ServiceAdvertiseAddr: "self:8888",
		// Keep the ConfChange retry window above the 500ms floor so package
		// co-scheduling does not turn a committed registration into a retry.
		InstanceID: []byte("self-instance-id"), TickInterval: 100 * time.Millisecond,
		ElectionTick: 15, HeartbeatTick: 10,
		ProcessingTickInterval: time.Millisecond, MaintenanceInterval: time.Hour,
	}, raftTransport, applier, logger, meter, w, fsm, recovery, synchronizer, m, responses)
	require.NoError(t, err)

	ready := make(chan struct{})
	runDone := make(chan struct{})
	var runErr error
	go func() {
		defer close(runDone)
		runErr = n.Run(context.Background(), ready)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, n.Stop(ctx))
		select {
		case <-runDone:
			require.NoError(t, runErr)
		case <-ctx.Done():
			t.Error("membership node did not stop before its stores closed")
		}
	})
	select {
	case <-ready:
	case <-runDone:
		t.Fatalf("membership node exited before readiness: %v", runErr)
	case <-time.After(5 * time.Second):
		t.Fatal("membership node did not become ready")
	}
	require.Eventually(t, n.IsLeader, 5*time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, n.WaitLeaderReady(ctx))

	return NewService(n, m, logger, "public-self:7777", "public-self:8888")
}

func TestService_LearnerRegistrationCommitsAndPreservesCallerIntent(t *testing.T) {
	t.Parallel()

	for _, bootJoin := range []bool{false, true} {
		name := "administrative"
		if bootJoin {
			name = "boot join"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := newMembershipServiceNode(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			register := s.AddLearner
			if bootJoin {
				register = s.JoinAsLearner
			}
			require.NoError(t, register(ctx, 2, "peer:7777", "peer:8888", []byte("peer-instance-id")))
			peers, err := s.ListPeers(ctx)
			require.NoError(t, err)
			require.Contains(t, peers, Peer{
				ID: 2, RaftAddress: "peer:7777", ServiceAddress: "peer:8888", InstanceID: []byte("peer-instance-id"),
			})

			// The existing voter has replicated its own no-op. An administrative
			// retry is idempotent, while a fresh-WAL boot against that progress
			// must report stale state, even with the same persisted identity.
			before := s.infraMembership.PeerAddresses()
			err = register(ctx, 1, "replacement:7777", "replacement:8888", []byte("self-instance-id"))
			if bootJoin {
				require.ErrorIs(t, err, node.ErrNodeStaleProgress)
			} else {
				require.ErrorIs(t, err, node.ErrNodeAlreadyInCluster)
			}
			require.ErrorContains(t, err, "adding learner:")
			require.Equal(t, before, s.infraMembership.PeerAddresses(), "rejected registration must retain committed addresses")
		})
	}
}

func TestService_ListPeersReturnsCompleteIndependentSnapshot(t *testing.T) {
	t.Parallel()

	s := newMembershipServiceNode(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.AddLearner(ctx, 2, "peer:7777", "peer:8888", []byte("peer-instance-id")))
	// A cached row alone must not leak into the configured member response.
	require.NoError(t, s.infraMembership.Set(3, "extra:7777", "extra:8888", []byte("extra-instanceid")))
	want := []Peer{
		{ID: 1, RaftAddress: "public-self:7777", ServiceAddress: "public-self:8888", InstanceID: []byte("self-instance-id")},
		{ID: 2, RaftAddress: "peer:7777", ServiceAddress: "peer:8888", InstanceID: []byte("peer-instance-id")},
	}
	peers, err := s.ListPeers(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, want, peers)
	for i := range peers {
		peers[i].InstanceID[0] ^= 0xff
	}
	fresh, err := s.ListPeers(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, want, fresh, "caller mutation must not corrupt a later discovery response")
}
