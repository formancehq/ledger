package bootstrap

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/spool"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source ../infra/state/machine.go -destination node_hook_notifier_generated_test.go -package bootstrap Notifier

// observedDrainNode preserves the real lifecycle while exposing the caller's
// cancellation boundary and the context owned by the bootstrap hook.
type observedDrainNode struct {
	*node.Node

	runCtx       context.Context
	runDone      chan struct{}
	stopReturned chan error
}

func (n *observedDrainNode) Run(ctx context.Context, ready chan struct{}) error {
	n.runCtx = ctx
	defer close(n.runDone)

	return n.Node.Run(ctx, ready)
}

func (n *observedDrainNode) Stop(ctx context.Context) error {
	err := n.Node.Stop(ctx)
	n.stopReturned <- err

	return err
}

func TestNodeHookStopCancellationRetainsRealCommitDrain(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	commitEntered := make(chan struct{})
	releaseCommit := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseCommit) })
	notifier := NewMockNotifier(gomock.NewController(t))
	notifier.EXPECT().NotifyConfigChanged().AnyTimes()
	notifier.EXPECT().NotifyLogsCommitted(gomock.Any()).AnyTimes().Do(func(lastSequence uint64) {
		if lastSequence == 0 {
			return // Election no-ops do not contain the business proposal.
		}
		close(commitEntered)
		<-releaseCommit
	})
	n, store := newHookCommitDrainNode(t, notifier)
	hook := nodeHook(n, logging.Testing())
	require.NoError(t, hook.OnStart(ctx))
	// Register after the infrastructure cleanups: failed assertions must release
	// and join the node before its stores, pools, or transport are closed.
	t.Cleanup(func() {
		release()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, n.Node.Stop(stopCtx))
		select {
		case <-n.runDone:
		case <-stopCtx.Done():
			t.Error("real node did not exit before dependency cleanup")
		}
	})
	require.Eventually(t, n.IsLeader, 5*time.Second, time.Millisecond)
	require.NoError(t, n.WaitLeaderReady(ctx))
	const ledger = "hook-shutdown-drain"
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: ledger, Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
	}}, Technical: &raftcmdpb.OrderTechnical{CoverageBits: []byte{1}}}
	command := commands.NewCommand(order)
	command.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterPolicy)
	ledgerKey, _ := attributes.MakeKey(domain.LedgerKey{Name: ledger}.Bytes())
	command.ExecutionPlan = &raftcmdpb.ExecutionPlan{Attributes: []*raftcmdpb.AttributeCoverage{{
		Id: &raftcmdpb.AttributeID{Id: ledgerKey[:]}, AttrCode: uint32(dal.SubAttrLedger),
	}}}
	data, err := command.MarshalVT()
	require.NoError(t, err)
	future, err := n.Propose(ctx, node.NewProposal(command.GetId(), data))
	require.NoError(t, err)
	select {
	case <-commitEntered:
	case <-ctx.Done():
		t.Fatal("real proposal did not reach post-commit notification")
	}
	cancelled, cancelWait := context.WithCancel(ctx)
	cancelWait()
	_, err = future.Wait(cancelled)
	require.ErrorIs(t, err, context.Canceled, "notification barrier must retain the proposal future")
	stopCtx, cancelStop := context.WithCancel(ctx)
	hookDone := make(chan error, 1)
	go func() { hookDone <- hook.OnStop(stopCtx) }()
	cancelStop()
	select {
	case err := <-n.stopReturned:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("real Stop did not respect its cancelled caller")
	}
	_, err = future.Wait(cancelled)
	require.ErrorIs(t, err, context.Canceled, "Stop cancellation must not prematurely resolve the held proposal")
	persistedBeforeRelease, err := query.GetLedgerByName(ctx, store, ledger)
	require.NoError(t, err, "notifier must hold the actual durable business commit")
	require.Equal(t, ledger, persistedBeforeRelease.GetName())
	// Unit regressions establish the scheduling-independent hook ownership
	// rule. This real-I/O fixture additionally observes the held commit across
	// the cancellation boundary before releasing the notifier.
	require.NoError(t, n.runCtx.Err(), "hook must retain the context needed by the blocked real commit")
	select {
	case err := <-hookDone:
		t.Fatalf("hook returned before real commit drain: %v", err)
	default:
	}
	select {
	case <-n.runDone:
		t.Fatal("Run exited while its committer was blocked")
	default:
	}
	release()
	select {
	case err := <-hookDone:
		require.ErrorIs(t, err, context.Canceled, "drain must preserve the original Stop error")
	case <-ctx.Done():
		t.Fatal("hook did not join the released real commit")
	}
	select {
	case <-n.runDone:
	default:
		t.Fatal("hook returned before Run completion")
	}
	require.ErrorIs(t, n.runCtx.Err(), context.Canceled)
	_, err = future.Wait(ctx)
	require.NoError(t, err, "committed proposal must resolve after notifier release")
	persisted, err := query.GetLedgerByName(ctx, store, ledger)
	require.NoError(t, err)
	require.Equal(t, ledger, persisted.GetName())
	require.Positive(t, n.LastPersistedIndex())
}

func newHookCommitDrainNode(t *testing.T, notifier state.Notifier) (*observedDrainNode, *dal.Store) {
	t.Helper()
	logger := logging.Testing()
	meters := noop.NewMeterProvider()
	meter := meters.Meter("hook-commit-drain")
	store := newTestStore(t)
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}, []byte("0123456789abcdef0123456789abcdef")))
	require.NoError(t, batch.Commit())
	w, err := wal.New(t.TempDir(), logger, meter)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	confState := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(0, confState, nil))
	require.NoError(t, w.Append(&raftpb.HardState{Term: proto.Uint64(1)}, nil))
	sp, err := spool.NewDefault(spool.DefaultSpoolConfig{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sp.Close()) })
	cacheStore, err := cache.New(1000, meter)
	require.NoError(t, err)
	registry := state.NewStateRegistry(cacheStore, attributes.New())
	machine, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil), store,
		dal.NewSentinelFactory(store, false), meters, nil, state.NewSharedState(), notifier, nil, "hook-drain", 0,
		func(*raftpb.Entry, *dal.WriteSession) error { return nil })
	require.NoError(t, err)
	t.Cleanup(machine.Close)
	recovery := state.NewRecovery(machine, store)
	require.NoError(t, recovery.RecoverState())
	synchronizer := state.NewSynchronizer(machine, recovery, dal.NewIncomingRestoreFactory(store))
	raftPool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	servicePool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, raftPool.Close()) })
	t.Cleanup(func() { require.NoError(t, servicePool.Close()) })
	raftTransport := node.NewTransport(logger, raftPool, meters, 1,
		node.TransportConfig{Reception: []int{8, 8, 8}, Send: []int{8, 8, 8}}, "hook-drain", 1024, "", "")
	go raftTransport.Start(context.Background())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, raftTransport.Stop(stopCtx))
	})
	instanceID := []byte("0000000000000001")
	member, err := membership.NewMembership(membership.NewPeerStore(store), raftTransport, servicePool,
		1, "node-1:7000", "node-1:8000", instanceID, logger)
	require.NoError(t, err)
	responses := node.NewLocalResponses()
	applier, err := node.NewApplier(machine, recovery, synchronizer, sp, store, w, logger, meter, 1000, 1000, nil, member, responses)
	require.NoError(t, err)
	raftNode, err := node.NewNode(node.NodeConfig{
		NodeID: 1, AdvertiseAddr: "node-1:7000", ServiceAdvertiseAddr: "node-1:8000", InstanceID: instanceID,
		TickInterval: time.Millisecond, ProcessingTickInterval: time.Millisecond, MaintenanceInterval: time.Hour,
	}, raftTransport, applier, logger, meter, w, machine, recovery, synchronizer, member, responses)
	require.NoError(t, err)

	return &observedDrainNode{Node: raftNode, runDone: make(chan struct{}), stopReturned: make(chan error, 1)}, store
}
