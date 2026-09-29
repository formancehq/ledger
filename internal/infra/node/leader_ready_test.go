package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestLeaderReadyGenerationSuccessfulWait(t *testing.T) {
	t.Parallel()

	generation := newLeaderReadyGeneration(context.Background())
	emitted := make(chan struct{})

	go func() {
		require.NoError(t, generation.run(
			func(context.Context) error { return nil },
			func() { close(emitted) },
		))
	}()

	<-generation.done
	requireClosed(t, emitted)
	requireClosed(t, generation.ready)
}

func TestLeaderReadyGenerationCancellationDoesNotAuthorizeReadiness(t *testing.T) {
	t.Parallel()

	generation := newLeaderReadyGeneration(context.Background())
	emitted := make(chan struct{})

	go func() {
		err := generation.run(
			func(ctx context.Context) error {
				<-ctx.Done()

				return ctx.Err()
			},
			func() { close(emitted) },
		)
		require.ErrorIs(t, err, context.Canceled)
	}()

	generation.cancelAndWait()
	requireOpen(t, emitted)
	requireOpen(t, generation.ready)
}

func TestLeaderReadyGenerationCancellationAfterWaitDoesNotAuthorizeReadiness(t *testing.T) {
	t.Parallel()

	generation := newLeaderReadyGeneration(context.Background())
	waitReturned := make(chan struct{})
	emitted := make(chan struct{})
	runErr := make(chan error, 1)

	// Hold the publication lock so the wait can succeed while readiness
	// publication remains pending, matching the EN-1870 race window.
	generation.mu.Lock()
	go func() {
		runErr <- generation.run(
			func(context.Context) error {
				close(waitReturned)

				return nil
			},
			func() { close(emitted) },
		)
	}()

	<-waitReturned
	stopped := make(chan struct{})
	go func() {
		generation.cancelAndWait()
		close(stopped)
	}()

	<-generation.ctx.Done()
	generation.mu.Unlock()
	<-stopped

	require.ErrorIs(t, <-runErr, context.Canceled)
	requireOpen(t, emitted)
	requireOpen(t, generation.ready)
}

func TestLeaderReadyGenerationCancellationJoinsRunningCallback(t *testing.T) {
	t.Parallel()

	generation := newLeaderReadyGeneration(context.Background())
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		require.NoError(t, generation.run(
			func(context.Context) error { return nil },
			func() {
				close(callbackStarted)
				<-releaseCallback
			},
		))
	}()

	<-callbackStarted
	go func() {
		generation.cancelAndWait()
		close(stopped)
	}()

	requireOpen(t, stopped)
	close(releaseCallback)
	<-stopped
	requireClosed(t, generation.ready)
}

func TestLeaderReadyGenerationReplacementIsIndependent(t *testing.T) {
	t.Parallel()

	stale := newLeaderReadyGeneration(context.Background())
	current := newLeaderReadyGeneration(context.Background())

	go func() {
		_ = stale.run(
			func(ctx context.Context) error {
				<-ctx.Done()

				return ctx.Err()
			},
			func() { t.Error("cancelled generation emitted readiness") },
		)
	}()

	stale.cancelAndWait()
	require.NoError(t, current.run(
		func(context.Context) error { return nil },
		func() {},
	))

	requireOpen(t, stale.ready)
	requireClosed(t, stale.lost)
	requireClosed(t, current.ready)
}

func TestWaitLeaderReadyReturnsWhenPendingGenerationLosesLeadership(t *testing.T) {
	t.Parallel()

	generation := newLeaderReadyGeneration(context.Background())
	n := &Node{}
	n.leaderReady.Store(generation)

	go func() {
		_ = generation.run(
			func(ctx context.Context) error {
				<-ctx.Done()

				return ctx.Err()
			},
			func() { t.Error("cancelled generation emitted readiness") },
		)
	}()

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- n.WaitLeaderReady(context.Background())
	}()

	generation.cancelAndWait()
	require.ErrorIs(t, <-waitErr, ErrNotLeader)
}

func TestProcessReadyLeadershipLossFencesPendingReadinessPublication(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	n, err := NewNode(NodeConfig{
		NodeID: 1, AdvertiseAddr: "node-1:7000", ServiceAdvertiseAddr: "node-1:8000",
		InstanceID: []byte("0000000000000001"), TickInterval: time.Hour,
		ProcessingTickInterval: time.Hour, MaintenanceInterval: time.Hour,
	}, newForceRemoveTransport(t), setup.applier, logging.Testing(),
		noop.NewMeterProvider().Meter("leader-ready-loss"), setup.wal, setup.fsm,
		setup.applier.recovery, setup.applier.synchronizer, newTestMembership(t), setup.responseSink)
	require.NoError(t, err)

	n.lastSoftState.Store(&raft.SoftState{Lead: 1, RaftState: raft.StateLeader})
	generation := newLeaderReadyGeneration(context.Background())
	n.leaderReady.Store(generation)

	readyEvent := make(chan struct{})
	lostEvent := make(chan struct{})
	n.SetObserver(NewObserver(func(event any) {
		switch event := event.(type) {
		case LeaderReadyEvent:
			close(readyEvent)
		case LeadershipChangeEvent:
			if !event.IsLeader {
				close(lostEvent)
			}
		}
	}))

	waitReturned := make(chan struct{})
	generation.mu.Lock()
	go func() {
		_ = generation.run(
			func(context.Context) error {
				close(waitReturned)

				return nil
			},
			func() { n.observer.Emit(LeaderReadyEvent{}) },
		)
	}()
	<-waitReturned

	processed := make(chan error, 1)
	go func() {
		_, processErr := n.processReady(context.Background(), make(chan struct{}), raft.Ready{
			SoftState: &raft.SoftState{Lead: 2, RaftState: raft.StateFollower},
		})
		processed <- processErr
	}()

	<-generation.ctx.Done()
	generation.mu.Unlock()
	require.NoError(t, <-processed)
	requireClosed(t, lostEvent)
	requireOpen(t, readyEvent)
	requireOpen(t, generation.ready)
}

func requireClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	default:
		t.Fatal(errors.New("channel is open"))
	}
}

func requireOpen(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
		t.Fatal(errors.New("channel is closed"))
	default:
	}
}
