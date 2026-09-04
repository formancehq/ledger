package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

type forceRemoveFailureStage int

const (
	forceRemoveNoFailure forceRemoveFailureStage = iota
	forceRemoveBeforeSnapshotPersistence
	forceRemoveAfterSnapshotFilePersistence
)

type forceRemoveWAL struct {
	wal.WAL

	mu           sync.Mutex
	failureStage forceRemoveFailureStage
	updateErr    error
	updates      int
	started      chan struct{}
	startOnce    sync.Once
	release      chan struct{}
	releaseOnce  sync.Once
}

func (w *forceRemoveWAL) UpdateSnapshotConfState(cs *raftpb.ConfState) error {
	w.mu.Lock()
	w.updates++
	w.mu.Unlock()

	w.startOnce.Do(func() { close(w.started) })
	if w.release != nil {
		<-w.release
	}

	switch w.failureStage {
	case forceRemoveNoFailure:
		return w.WAL.UpdateSnapshotConfState(cs)
	case forceRemoveBeforeSnapshotPersistence:
		return w.updateErr
	case forceRemoveAfterSnapshotFilePersistence:
		// Closing the real etcd WAL makes its SaveSnapshot call fail while
		// leaving Snapshotter.Save operational. This drives the production
		// UpdateSnapshotConfState sequence through its atomic snapshot-file
		// replacement and fails at the following WAL-record write.
		if err := w.Close(); err != nil {
			return fmt.Errorf("closing WAL for failure injection: %w", err)
		}

		err := w.WAL.UpdateSnapshotConfState(cs)
		if err == nil {
			return errors.New("invariant: closed WAL accepted snapshot record")
		}

		return fmt.Errorf("%w after snapshot-file persistence: %w", w.updateErr, err)
	default:
		return errors.New("invariant: unknown force-remove failure stage")
	}
}

func (w *forceRemoveWAL) updateCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.updates
}

type forceRemoveHarness struct {
	n               *Node
	rawNode         *raft.RawNode
	durableWAL      wal.WAL
	injectedWAL     *forceRemoveWAL
	membership      *membership.Membership
	walDir          string
	baselineCommit  uint64
	stagedIndex     uint64
	stop            chan struct{}
	orchestrateDone chan error
	stopped         bool
}

func newForceRemoveTransport(t *testing.T) Transport {
	t.Helper()

	transport := NewMockTransport(gomock.NewController(t))
	transport.EXPECT().Unreachable().AnyTimes().Return((<-chan uint64)(nil))
	transport.EXPECT().RecvHighPriority().AnyTimes().Return((<-chan []*raftpb.Message)(nil))
	transport.EXPECT().RecvMediumPriority().AnyTimes().Return((<-chan []*raftpb.Message)(nil))
	transport.EXPECT().RecvLowPriority().AnyTimes().Return((<-chan []*raftpb.Message)(nil))
	transport.EXPECT().Send(gomock.Any()).AnyTimes()

	return transport
}

func newForceRemoveHarness(
	t *testing.T,
	voters []uint64,
	failureStage forceRemoveFailureStage,
	updateErr error,
	blockUpdate bool,
) *forceRemoveHarness {
	t.Helper()

	setup := newTestApplierSetup(t)
	initial := &raftpb.ConfState{Voters: voters}
	require.NoError(t, setup.wal.UpdateSnapshotConfState(initial))

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

	// Elect node 1 using the real voter tracker. A candidate needs quorum
	// including its own vote, so supply exactly quorum-1 peer votes.
	require.NoError(t, rawNode.Campaign())
	persistReadyAndAdvance(t, rawNode, setup.wal)
	quorum := len(voters)/2 + 1
	for i := 1; i < quorum; i++ {
		require.NoError(t, rawNode.Step(&raftpb.Message{
			Type: new(raftpb.MsgVoteResp),
			From: new(voters[i]),
			To:   proto.Uint64(1),
			Term: new(rawNode.Status().GetTerm()),
		}))
		persistReadyAndAdvance(t, rawNode, setup.wal)
	}
	require.Equal(t, raft.StateLeader, rawNode.Status().RaftState)

	// Commit the leader's no-op so later tests have a stable durable baseline.
	leaderIndex := rawNode.Status().Progress[1].Match
	for i := 1; i < quorum; i++ {
		require.NoError(t, rawNode.Step(&raftpb.Message{
			Type:  new(raftpb.MsgAppResp),
			From:  new(voters[i]),
			To:    proto.Uint64(1),
			Term:  new(rawNode.Status().GetTerm()),
			Index: new(leaderIndex),
		}))
		persistReadyAndAdvance(t, rawNode, setup.wal)
	}
	require.Equal(t, leaderIndex, rawNode.Status().GetCommit())
	baselineCommit := rawNode.Status().GetCommit()
	var stagedIndex uint64
	if len(voters) == 4 {
		// Stage an entry with only A and B acknowledging it. That is not a
		// quorum of four, but becomes a quorum after D is force-removed.
		require.NoError(t, rawNode.Propose([]byte("committable only after quorum shrinks")))
		persistReadyAndAdvance(t, rawNode, setup.wal)
		stagedIndex = rawNode.Status().Progress[1].Match
		require.Greater(t, stagedIndex, baselineCommit)
		require.NoError(t, rawNode.Step(&raftpb.Message{
			Type:  new(raftpb.MsgAppResp),
			From:  proto.Uint64(2),
			To:    proto.Uint64(1),
			Term:  new(rawNode.Status().GetTerm()),
			Index: new(stagedIndex),
		}))
		require.Equal(t, baselineCommit, rawNode.Status().GetCommit(),
			"two acknowledgements are not a quorum of four")
	}

	m := newTestMembership(t)
	for _, id := range voters {
		require.NoError(t, m.Register(
			id,
			fmt.Sprintf("node-%d:7000", id),
			fmt.Sprintf("node-%d:8000", id),
			fmt.Appendf(nil, "%016d", id),
		))
	}

	injectedWAL := &forceRemoveWAL{
		WAL:          setup.wal,
		failureStage: failureStage,
		updateErr:    updateErr,
		started:      make(chan struct{}),
	}
	if blockUpdate {
		injectedWAL.release = make(chan struct{})
	}

	n := &Node{
		rawNode:          rawNode,
		logger:           logging.Testing(),
		wal:              injectedWAL,
		fsm:              setup.fsm,
		transport:        newForceRemoveTransport(t),
		config:           NodeConfig{NodeID: 1, TickInterval: time.Hour},
		proposeCh:        make(chan *Proposal, 16),
		clusterCommandCh: make(chan *clusterCommand, 16),
		readyTerminated:  make(chan readyResult),
		localResponseCh:  make(LocalResponses),
		applier:          setup.applier,
		membership:       m,
		indexTracker:     NewIndexTracker(initialIndex(setup.wal)),
		terminalCh:       make(chan struct{}),
	}
	n.confState.Store(initial)

	h := &forceRemoveHarness{
		n:               n,
		rawNode:         rawNode,
		durableWAL:      setup.wal,
		injectedWAL:     injectedWAL,
		membership:      m,
		walDir:          setup.walDir,
		baselineCommit:  baselineCommit,
		stagedIndex:     stagedIndex,
		stop:            make(chan struct{}),
		orchestrateDone: make(chan error, 1),
	}
	go func() {
		h.orchestrateDone <- n.orchestrate(context.Background(), h.stop)
	}()

	t.Cleanup(func() {
		h.releaseUpdate()
		if !h.stopped {
			require.NoError(t, h.stopAndWait(t))
		}
	})

	return h
}

func persistReadyAndAdvance(t *testing.T, rawNode *raft.RawNode, w wal.WAL) {
	t.Helper()

	for rawNode.HasReady() {
		ready := rawNode.Ready()
		require.NoError(t, w.Append(ready.HardState, ready.Entries))
		rawNode.Advance(ready)
	}
}

func (h *forceRemoveHarness) stopAndWait(t *testing.T) error {
	t.Helper()

	if !h.stopped {
		close(h.stop)
		h.stopped = true
	}

	select {
	case err := <-h.orchestrateDone:
		return err
	case <-time.After(time.Second):
		t.Fatal("orchestrate did not stop")

		return nil
	}
}

func (h *forceRemoveHarness) waitForTerminal(t *testing.T) error {
	t.Helper()

	select {
	case err := <-h.orchestrateDone:
		h.stopped = true

		return err
	case <-time.After(time.Second):
		t.Fatal("orchestrate continued after terminal force-remove failure")

		return nil
	}
}

func (h *forceRemoveHarness) waitForUpdate(t *testing.T) {
	t.Helper()

	select {
	case <-h.injectedWAL.started:
	case <-time.After(time.Second):
		t.Fatal("ForceRemoveNode did not reach UpdateSnapshotConfState")
	}
}

func (h *forceRemoveHarness) releaseUpdate() {
	if h.injectedWAL.release != nil {
		h.injectedWAL.releaseOnce.Do(func() { close(h.injectedWAL.release) })
	}
}

func receiveForceRemoveError(t *testing.T, ch <-chan error) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-time.After(time.Second):
		t.Fatal("operation did not receive terminal force-remove error")

		return nil
	}
}

func durableState(t *testing.T, w wal.WAL) (*raftpb.HardState, []uint64) {
	t.Helper()

	hardState, confState, err := w.InitialState()
	require.NoError(t, err)

	return hardState, confState.GetVoters()
}

func (h *forceRemoveHarness) restartedState(t *testing.T) (*raftpb.HardState, map[uint64]struct{}) {
	t.Helper()

	_ = h.durableWAL.Close()
	reopened, err := wal.New(
		h.walDir,
		logging.Testing(),
		noop.NewMeterProvider().Meter("force-remove-restart"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	hardState, _, err := reopened.InitialState()
	require.NoError(t, err)
	restarted, err := raft.NewRawNode(&raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         reopened,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          NewLoggerAdapter(logging.Testing()),
	})
	require.NoError(t, err)

	return hardState, restarted.Status().Config.Voters.IDs()
}

func TestForceRemoveNodeDurabilityFailureContract(t *testing.T) {
	t.Run("validation failure happens before live mutation", func(t *testing.T) {
		t.Parallel()

		h := newForceRemoveHarness(t, []uint64{1, 2, 3}, forceRemoveNoFailure, nil, false)

		err := h.n.ForceRemoveNode(context.Background(), 99)
		require.ErrorIs(t, err, ErrNodeNotInCluster)
		require.Zero(t, h.injectedWAL.updateCount())
		require.Contains(t, h.rawNode.Status().Progress, uint64(3))
		require.Equal(t, []uint64{1, 2, 3}, h.n.confState.Load().GetVoters())
		_, voters := durableState(t, h.durableWAL)
		require.Equal(t, []uint64{1, 2, 3}, voters)

		_, err = h.n.GetClusterState(context.Background())
		require.NoError(t, err, "a pre-mutation validation error must not stop the node")
	})

	t.Run("failure before snapshot persistence fail-stops and restarts old membership", func(t *testing.T) {
		t.Parallel()

		injected := errors.New("injected before snapshot persistence")
		h := newForceRemoveHarness(
			t, []uint64{1, 2, 3}, forceRemoveBeforeSnapshotPersistence, injected, false,
		)

		err := h.n.ForceRemoveNode(context.Background(), 3)
		require.ErrorIs(t, err, injected)
		require.Equal(t, 1, h.injectedWAL.updateCount())
		require.NotContains(t, h.rawNode.Status().Progress, uint64(3))
		require.Equal(t, []uint64{1, 2}, h.n.confState.Load().GetVoters())
		_, voters := durableState(t, h.durableWAL)
		require.Equal(t, []uint64{1, 2, 3}, voters)
		require.Contains(t, h.membership.PeerAddresses(), uint64(3))
		removed, lookupErr := h.membership.IsRemoved(3, []byte("0000000000000003"))
		require.NoError(t, lookupErr)
		require.False(t, removed, "peer cleanup must not write the tombstone after persistence fails")

		runErr := h.waitForTerminal(t)
		require.ErrorIs(t, runErr, injected)
		_, err = h.n.GetClusterState(context.Background())
		require.ErrorIs(t, err, injected)
		_, err = h.n.Propose(context.Background(), NewProposal(100, []byte("must not be admitted")))
		require.ErrorIs(t, err, injected)

		_, restartedVoters := h.restartedState(t)
		require.Equal(t, map[uint64]struct{}{1: {}, 2: {}, 3: {}}, restartedVoters)
	})

	t.Run("failure after snapshot file persistence fail-stops and restarts new membership", func(t *testing.T) {
		t.Parallel()

		injected := errors.New("injected WAL snapshot-record failure")
		h := newForceRemoveHarness(
			t, []uint64{1, 2, 3}, forceRemoveAfterSnapshotFilePersistence, injected, false,
		)

		err := h.n.ForceRemoveNode(context.Background(), 3)
		require.ErrorIs(t, err, injected)
		require.NotContains(t, h.rawNode.Status().Progress, uint64(3))
		require.Equal(t, []uint64{1, 2}, h.n.confState.Load().GetVoters())
		require.Contains(t, h.membership.PeerAddresses(), uint64(3),
			"peer cleanup must not run after a persistence error")
		removed, lookupErr := h.membership.IsRemoved(3, []byte("0000000000000003"))
		require.NoError(t, lookupErr)
		require.False(t, removed, "peer cleanup must not write the tombstone after persistence fails")
		require.ErrorIs(t, h.waitForTerminal(t), injected)

		_, restartedVoters := h.restartedState(t)
		require.Equal(t, map[uint64]struct{}{1: {}, 2: {}}, restartedVoters,
			"the atomically replaced snapshot file is the restart authority for its existing term/index")
	})

	t.Run("concurrent admissions receive terminal failure", func(t *testing.T) {
		t.Parallel()

		injected := errors.New("blocked snapshot persistence failure")
		h := newForceRemoveHarness(
			t, []uint64{1, 2, 3}, forceRemoveBeforeSnapshotPersistence, injected, true,
		)

		forceCtx, cancelForce := context.WithCancel(context.Background())
		defer cancelForce()
		forceErr := make(chan error, 1)
		go func() { forceErr <- h.n.ForceRemoveNode(forceCtx, 3) }()
		h.waitForUpdate(t)

		commandErr := make(chan error, 1)
		go func() {
			_, err := h.n.GetClusterState(context.Background())
			commandErr <- err
		}()
		require.Eventually(t, func() bool { return len(h.n.clusterCommandCh) == 1 }, time.Second, time.Millisecond)

		proposal := NewProposal(101, []byte("queued before terminal publication"))
		trackerBefore := h.n.indexTracker.Next()
		h.n.indexTracker.Lock()
		fsmFuture, err := h.n.Propose(context.Background(), proposal)
		h.n.indexTracker.Unlock()
		require.NoError(t, err)
		require.NotNil(t, fsmFuture)
		require.Len(t, h.n.proposeCh, 1)

		cancelForce()
		select {
		case err := <-forceErr:
			t.Fatalf("force-remove returned before its persistence outcome: %v", err)
		default:
		}

		h.releaseUpdate()
		require.ErrorIs(t, receiveForceRemoveError(t, forceErr), injected)
		require.ErrorIs(t, receiveForceRemoveError(t, commandErr), injected)
		require.ErrorIs(t, h.waitForTerminal(t), injected)

		waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
		defer cancelWait()
		_, err = fsmFuture.Wait(waitCtx)
		require.ErrorIs(t, err, injected)
		_, err = proposal.Wait(waitCtx)
		require.ErrorIs(t, err, injected)
		require.Equal(t, trackerBefore, h.n.indexTracker.Next())
	})

	t.Run("reduced live quorum cannot durably commit staged work after failure", func(t *testing.T) {
		t.Parallel()

		injected := errors.New("injected four-to-three persistence failure")
		h := newForceRemoveHarness(
			t, []uint64{1, 2, 3, 4}, forceRemoveBeforeSnapshotPersistence, injected, false,
		)

		baselineHardState, _ := durableState(t, h.durableWAL)
		baselineApplied := h.n.fsm.LastAppliedIndex()
		require.Equal(t, h.baselineCommit, baselineHardState.GetCommit())
		require.Greater(t, h.stagedIndex, h.baselineCommit)
		require.Equal(t, h.baselineCommit, h.rawNode.Status().GetCommit())

		err := h.n.ForceRemoveNode(context.Background(), 4)
		require.ErrorIs(t, err, injected)
		require.Equal(t, map[uint64]struct{}{1: {}, 2: {}, 3: {}}, h.rawNode.Status().Config.Voters.IDs())
		require.Equal(t, h.stagedIndex, h.rawNode.Status().GetCommit(),
			"the irreversible four-to-three transition makes the staged entry live-committed")
		require.ErrorIs(t, h.waitForTerminal(t), injected)
		require.Equal(t, baselineApplied, h.n.fsm.LastAppliedIndex(),
			"terminal exit must prevent business application")

		durableHardState, durableVoters := durableState(t, h.durableWAL)
		require.Equal(t, baselineHardState.GetCommit(), durableHardState.GetCommit(),
			"the live-only commit must not reach durable HardState")
		require.Equal(t, []uint64{1, 2, 3, 4}, durableVoters)

		restartedHardState, restartedVoters := h.restartedState(t)
		require.Equal(t, baselineHardState.GetCommit(), restartedHardState.GetCommit())
		require.Equal(t, map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}}, restartedVoters)
	})

	t.Run("successful persistence remains live durable and restartable", func(t *testing.T) {
		t.Parallel()

		h := newForceRemoveHarness(t, []uint64{1, 2, 3}, forceRemoveNoFailure, nil, false)

		require.NoError(t, h.n.ForceRemoveNode(context.Background(), 3))
		require.Equal(t, 1, h.injectedWAL.updateCount())
		require.NotContains(t, h.rawNode.Status().Progress, uint64(3))
		require.Equal(t, []uint64{1, 2}, h.n.confState.Load().GetVoters())
		_, voters := durableState(t, h.durableWAL)
		require.Equal(t, []uint64{1, 2}, voters)
		require.NotContains(t, h.membership.PeerAddresses(), uint64(3))
		removed, err := h.membership.IsRemoved(3, []byte("0000000000000003"))
		require.NoError(t, err)
		require.True(t, removed)

		err = h.n.ForceRemoveNode(context.Background(), 3)
		require.ErrorIs(t, err, ErrNodeNotInCluster)
		require.Equal(t, 1, h.injectedWAL.updateCount())
		_, err = h.n.GetClusterState(context.Background())
		require.NoError(t, err)

		_, restartedVoters := h.restartedState(t)
		require.Equal(t, map[uint64]struct{}{1: {}, 2: {}}, restartedVoters)
	})
}
