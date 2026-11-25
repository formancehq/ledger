package node

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

type checkpointSequenceProvider struct {
	paths []string
	calls int
}

func (p *checkpointSequenceProvider) GetForPeer(id uint64) (state.SnapshotFetcher, error) {
	return p.GetForPeers([]uint64{id})
}

func (p *checkpointSequenceProvider) GetForPeers(_ []uint64) (state.SnapshotFetcher, error) {
	path := p.paths[p.calls]
	p.calls++

	return checkpointDirectoryFetcher{path: path}, nil
}

type checkpointDirectoryFetcher struct{ path string }

func (f checkpointDirectoryFetcher) FetchSnapshot(_ context.Context, targetDir string, _ *state.SyncProgress, _ uint64) (uint64, error) {
	return 0, os.CopyFS(targetDir, os.DirFS(f.path))
}

func testJoinCheckpoint(t *testing.T, withConfig bool) string {
	t.Helper()
	store, err := dal.NewStore(t.TempDir(), logging.Testing(), noop.NewMeterProvider().Meter("donor"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	seedApplierAuditKey(t, store)
	batch := store.OpenWriteSession()
	require.NoError(t, state.SetAppliedIndex(batch, 1))
	if withConfig {
		require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterConfig},
			&commonpb.PersistedClusterState{Config: &commonpb.ClusterConfig{RotationThreshold: 1000}, CacheEpoch: 1}))
	}
	require.NoError(t, batch.Commit())
	id, err := store.CreateSnapshot()
	require.NoError(t, err)
	path, err := store.GetCheckpointPath(id)
	require.NoError(t, err)

	return path
}

func waitJoinSyncResult(t *testing.T, applier *Applier) gatingResult {
	t.Helper()
	select {
	case result := <-applier.gatingTerminated:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint synchronization did not finish")
		return gatingResult{}
	}
}

func TestFreshJoinRejectsPreConfigCheckpointThenReplaysAfterRetry(t *testing.T) {
	t.Parallel()
	setup := newTestApplierSetup(t)
	provider := &checkpointSequenceProvider{paths: []string{
		testJoinCheckpoint(t, false), testJoinCheckpoint(t, true),
	}}
	setup.applier.snapshotFetcherProvider = provider
	setup.applier.requireJoinConfig = true
	entry, _ := makeCreateLedgerEntry(t, 2, "after-join-config")
	require.NoError(t, setup.spool.AppendCommittedEntries(t.Context(), entry))

	setup.applier.startSyncSnapshot(t.Context(), 2)
	first := waitJoinSyncResult(t, setup.applier)
	require.True(t, first.syncFailed, "a checkpoint without ClusterConfig must remain gated")
	require.False(t, first.taskFailed)
	require.Equal(t, 1, provider.calls)
	require.Equal(t, uint64(1), setup.fsm.LastAppliedIndex())
	require.Nil(t, setup.fsm.State.LastClusterConfig)
	require.False(t, listLedgerContains(setup.store, "after-join-config"), "failed sync must not replay the spool")

	setup.applier.startSyncSnapshot(t.Context(), 2)
	second := waitJoinSyncResult(t, setup.applier)
	require.False(t, second.syncFailed)
	require.False(t, second.taskFailed)
	require.Equal(t, 2, provider.calls)
	require.Equal(t, uint64(2), setup.fsm.LastAppliedIndex())
	require.NotNil(t, setup.fsm.State.LastClusterConfig)
	require.True(t, listLedgerContains(setup.store, "after-join-config"), "successful retry must replay the spool")
}

func seedRecoveredClusterConfig(t *testing.T, setup *testApplierSetup, epoch uint64) {
	t.Helper()
	batch := setup.store.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterConfig},
		&commonpb.PersistedClusterState{Config: &commonpb.ClusterConfig{RotationThreshold: 1000}, CacheEpoch: epoch}))
	require.NoError(t, batch.Commit())
	require.NoError(t, setup.applier.recovery.RecoverState())
}

// A fresh learner with an index-0 WAL snapshot may be sent the leader's
// retained log. It must request a checkpoint instead of applying that log
// against its node-local cache threshold.
func TestNewNodeFreshLearnerRequiresCheckpointBeforeRetainedLogReplay(t *testing.T) {
	t.Parallel()
	setup := newTestApplierSetup(t)
	require.NoError(t, setup.wal.UpdateSnapshotConfState(&raftpb.ConfState{
		Voters: []uint64{2}, Learners: []uint64{1},
	}))

	proposal := commands.NewCommand()
	proposal.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterConfig)
	proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{
		ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: 1000},
	}}}
	data, err := proposal.MarshalVT()
	require.NoError(t, err)
	require.NoError(t, setup.wal.Append(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}, []*raftpb.Entry{{
		Index: new(uint64(1)), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data,
	}}))

	n, err := NewNode(NodeConfig{
		NodeID: 1, AdvertiseAddr: "node-1:7000", ServiceAdvertiseAddr: "node-1:8000",
		InstanceID: []byte("0000000000000001"), TickInterval: time.Hour,
		ProcessingTickInterval: time.Hour, MaintenanceInterval: time.Hour,
	}, NewMockTransport(gomock.NewController(t)), setup.applier, logging.Testing(),
		noop.NewMeterProvider().Meter("fresh-join"), setup.wal, setup.fsm,
		setup.applier.recovery, setup.applier.synchronizer, newTestMembership(t), setup.responseSink)
	require.NoError(t, err)
	require.EqualValues(t, statusOutOfSync, n.applier.Status())
	require.Equal(t, uint64(0), setup.fsm.LastAppliedIndex(), "retained log must wait for donor checkpoint")
	n.lastSoftState.Store(&raft.SoftState{Lead: 2})
	n.retryReadySnapshotSync(setup.stop, raft.Ready{})
	require.Equal(t, uint64(2), (<-setup.applier.ch).syncLeader, "fresh learner must request the donor checkpoint")
}

func TestNewNodeFirstJoinRequiresCheckpoint(t *testing.T) {
	t.Parallel()
	setup := newTestApplierSetup(t)
	newWAL, err := wal.New(t.TempDir(), logging.Testing(), noop.NewMeterProvider().Meter("first-join-wal"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, newWAL.Close()) })
	setup.applier.wal = newWAL

	n, err := NewNode(NodeConfig{
		NodeID: 1, DataDir: t.TempDir(), AdvertiseAddr: "node-1:7000", ServiceAdvertiseAddr: "node-1:8000",
		InstanceID: []byte("0000000000000001"), TickInterval: time.Hour,
		ProcessingTickInterval: time.Hour, MaintenanceInterval: time.Hour,
		Peers: []Peer{{ID: 2, Address: "node-2:7000", ServiceAddress: "node-2:8000", InstanceID: []byte("0000000000000002")}},
	}, NewMockTransport(gomock.NewController(t)), setup.applier, logging.Testing(),
		noop.NewMeterProvider().Meter("first-join"), newWAL, setup.fsm,
		setup.applier.recovery, setup.applier.synchronizer, newTestMembership(t), setup.responseSink)
	require.NoError(t, err)
	require.EqualValues(t, statusOutOfSync, n.applier.Status())
}

// A promotion may be committed to the WAL before the learner installs its
// first checkpoint. The voter role must not make that empty FSM replay the log.
func TestNewNodePromotedBeforeCheckpointStillRequiresSync(t *testing.T) {
	t.Parallel()
	setup := newTestApplierSetup(t)
	require.NoError(t, setup.wal.UpdateSnapshotConfState(&raftpb.ConfState{Voters: []uint64{1, 2}}))

	n, err := NewNode(NodeConfig{
		NodeID: 1, AdvertiseAddr: "node-1:7000", ServiceAdvertiseAddr: "node-1:8000",
		InstanceID: []byte("0000000000000001"), TickInterval: time.Hour,
		ProcessingTickInterval: time.Hour, MaintenanceInterval: time.Hour,
	}, NewMockTransport(gomock.NewController(t)), setup.applier, logging.Testing(),
		noop.NewMeterProvider().Meter("promoted-before-sync"), setup.wal, setup.fsm,
		setup.applier.recovery, setup.applier.synchronizer, newTestMembership(t), setup.responseSink)
	require.NoError(t, err)
	require.EqualValues(t, statusOutOfSync, n.applier.Status())
	require.Zero(t, setup.fsm.LastAppliedIndex())
}

// A donor checkpoint can be durably installed while the member remains a
// learner. Its persisted config and epoch make another transfer unnecessary.
func TestNewNodeSynchronizedLearnerRestartsWithoutCheckpoint(t *testing.T) {
	t.Parallel()
	setup := newTestApplierSetup(t)
	require.NoError(t, setup.wal.UpdateSnapshotConfState(&raftpb.ConfState{
		Voters: []uint64{2}, Learners: []uint64{1},
	}))
	seedRecoveredClusterConfig(t, setup, 2)

	n, err := NewNode(NodeConfig{
		NodeID: 1, AdvertiseAddr: "node-1:7000", ServiceAdvertiseAddr: "node-1:8000",
		InstanceID: []byte("0000000000000001"), TickInterval: time.Hour,
		ProcessingTickInterval: time.Hour, MaintenanceInterval: time.Hour,
	}, NewMockTransport(gomock.NewController(t)), setup.applier, logging.Testing(),
		noop.NewMeterProvider().Meter("synchronized-learner"), setup.wal, setup.fsm,
		setup.applier.recovery, setup.applier.synchronizer, newTestMembership(t), setup.responseSink)
	require.NoError(t, err)
	require.EqualValues(t, statusNormal, n.applier.Status())
	require.Equal(t, uint64(2), setup.fsm.Registry.Cache.Epoch())
}
