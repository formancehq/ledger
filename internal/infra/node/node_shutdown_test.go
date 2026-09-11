package node

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

// startShutdownNode pauses the real Run implementation after task startup,
// before it can receive a shutdown request. Cleanup joins it before closing
// the fixture's WAL, spool, and store, including on the unfixed failure path.
func startShutdownNode(t *testing.T, ctx context.Context, setup *testApplierSetup) (*Node, <-chan error, func()) {
	t.Helper()
	n, err := NewNode(NodeConfig{
		NodeID: 1, AdvertiseAddr: "node-1:7000", ServiceAdvertiseAddr: "node-1:8000",
		InstanceID: []byte("0000000000000001"), TickInterval: time.Hour,
		ProcessingTickInterval: time.Hour, MaintenanceInterval: time.Hour,
	}, newForceRemoveTransport(t), setup.applier, logging.Testing(),
		noop.NewMeterProvider().Meter("node-shutdown"), setup.wal, setup.fsm,
		setup.applier.recovery, setup.applier.synchronizer, newTestMembership(t), setup.responseSink)
	require.NoError(t, err)
	started := make(chan struct{})
	release := make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- n.run(ctx, func() {
			close(started)
			<-release
		})
	}()
	t.Cleanup(func() {
		resume()
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, n.Stop(stopCtx))
		select {
		case <-done:
		case <-stopCtx.Done():
			t.Error("Node.Run was not joined before fixture cleanup")
		}
	})
	select {
	case <-started:
	case err := <-result:
		t.Fatalf("Node.Run failed before readiness: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Node.Run did not start its tasks")
	}
	// Drain traverses the real decoder/applier and proves their startup without
	// a scheduling delay. The committer starts before this barrier is handled.
	setup.applier.Drain(setup.stop)

	return n, result, resume
}

func TestNodeStopExpiredContextStillTerminatesRun(t *testing.T) {
	t.Parallel()
	for _, deadline := range []bool{false, true} {
		name := "cancelled"
		if deadline {
			name = "expired deadline"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runCtx, cancelRun := context.WithCancel(context.Background())
			defer cancelRun()
			n, result, resume := startShutdownNode(t, runCtx, newTestApplierSetup(t))
			stopCtx, cancelStop := context.WithCancel(context.Background())
			if deadline {
				cancelStop()
				stopCtx, cancelStop = context.WithDeadline(context.Background(), time.Unix(0, 0))
			} else {
				cancelStop()
			}
			defer cancelStop()
			require.ErrorIs(t, n.Stop(stopCtx), stopCtx.Err())
			// Reproduce bootstrap's fallback too: it cannot replace an explicit stop
			// request when the applier/committer are idle.
			cancelRun()
			resume()
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("Stop abandoned shutdown: Node.Run and its idle applier/committer remain live")
			}
			select {
			case <-n.runDone:
			default:
				t.Fatal("shutdown returned before Run lifecycle completion")
			}
		})
	}
}

func TestNodeIdleTasksRequireExplicitStopAfterCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		runCtx, cancelRun := context.WithCancel(context.Background())
		defer cancelRun()
		n, result, resume := startShutdownNode(t, runCtx, newTestApplierSetup(t))
		resume()
		cancelRun()
		// Let the real decoder observe cancellation and all remaining tasks
		// settle. Unlike a scheduling delay, Wait establishes that they cannot
		// make progress until another event (the explicit stop below) arrives.
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("context cancellation unexpectedly terminated Node.Run: %v", err)
		default:
		}
		select {
		case <-n.tasks.terminated:
			t.Fatal("context cancellation unexpectedly terminated the task pool")
		default:
		}
		require.NoError(t, n.Stop(context.Background()))
		require.NoError(t, <-result)
		select {
		case <-n.tasks.terminated:
		default:
			t.Fatal("Stop returned without joining the real idle tasks")
		}
	})
}

func TestNodeStopWaitsForCommitDrain(t *testing.T) {
	t.Parallel()
	commitEntered := make(chan struct{})
	releaseCommit := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseCommit) })
	notifier := NewMockNotifier(gomock.NewController(t))
	notifier.EXPECT().NotifyLogsCommitted(gomock.Any()).Do(func(uint64) {
		close(commitEntered)
		<-releaseCommit
	})
	setup := newTestApplierSetupWithNotifier(t, make(LocalResponses, 1024), notifier)
	runCtx := t.Context()
	n, result, resume := startShutdownNode(t, runCtx, setup)
	t.Cleanup(release)
	entry, id := makeCreateLedgerEntry(t, 1, "shutdown-drain")
	future := futures.New[state.ApplyResult]()
	setup.applier.StoreFuture(id, 1, future)
	setup.applier.Submit([]*raftpb.Entry{entry}, setup.confState, nil, setup.stop)
	select {
	case <-commitEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("committer did not enter post-commit notification")
	}
	// The real commit is still in flight; futures and the committer completion
	// signal are published only after this notification returns.
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStop()
	stopped := make(chan error, 1)
	go func() { stopped <- n.Stop(stopCtx) }()
	select {
	case <-n.stopChannel:
	case <-stopCtx.Done():
		t.Fatal("Stop did not publish shutdown")
	}
	resume()
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned before commit drain: %v", err)
	default:
	}
	require.NoError(t, runCtx.Err(), "graceful Stop must preserve the commit context")
	release()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-stopCtx.Done():
		t.Fatal("Stop did not join the completed commit")
	}
	require.NoError(t, <-result)
	_, err := future.Wait(stopCtx)
	require.NoError(t, err, "graceful drain must resolve the committed proposal")
	require.True(t, listLedgerContains(setup.store, "shutdown-drain"))
	require.Equal(t, uint64(1), setup.fsm.LastAppliedIndex())
	select {
	case <-n.tasks.terminated:
	default:
		t.Fatal("Stop returned without joining the task pool")
	}
}

func TestNodeStopConcurrentCallersShareShutdown(t *testing.T) {
	t.Parallel()
	n, result, resume := startShutdownNode(t, context.Background(), newTestApplierSetup(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	const callers = 8
	results := make(chan error, callers)
	for range callers {
		go func() { results <- n.Stop(ctx) }()
	}
	for range callers {
		require.ErrorIs(t, <-results, context.Canceled)
	}
	resume()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Stop calls did not terminate Run")
	}
	require.NoError(t, n.Stop(ctx), "already-exited Run needs no transfer or waiting")
}

func TestNodeStopCancellationDuringLeadershipTransferStillRequestsShutdown(t *testing.T) {
	t.Parallel()
	// A minimal node holds the orchestrate command so cancellation occurs
	// inside the transfer preflight, rather than before Stop starts. The
	// command callback deliberately remains unexecuted, as with a stalled
	// orchestrate loop; no rawNode access is needed on this failure path.
	n := &Node{
		logger: logging.Testing(), clusterCommandCh: make(chan *clusterCommand),
		runDone: make(chan struct{}), stopChannel: make(chan struct{}),
	}
	n.lastSoftState.Store(&raft.SoftState{RaftState: raft.StateLeader, Lead: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- n.Stop(ctx) }()
	select {
	case <-n.clusterCommandCh:
	case <-time.After(time.Second):
		t.Fatal("Stop did not enter leadership-transfer preflight")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Stop did not respect transfer cancellation")
	}
	select {
	case <-n.stopChannel:
	default:
		t.Fatal("leadership transfer consumed the caller context and abandoned shutdown")
	}
}

func TestNodeStopWaitsForRunCompletionOrCallerCancellation(t *testing.T) {
	t.Parallel()
	for _, complete := range []bool{false, true} {
		name := "cancel while draining"
		if complete {
			name = "complete drain"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				n := &Node{logger: logging.Testing(), runDone: make(chan struct{}), stopChannel: make(chan struct{})}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- n.Stop(ctx) }()
				synctest.Wait()
				select {
				case <-n.stopChannel:
				default:
					t.Fatal("Stop did not publish its request before waiting")
				}
				select {
				case err := <-result:
					t.Fatalf("Stop returned before Run completed: %v", err)
				default:
				}
				if complete {
					close(n.runDone)
				} else {
					cancel()
				}
				synctest.Wait()
				if complete {
					require.NoError(t, <-result)
				} else {
					require.ErrorIs(t, <-result, context.Canceled)
				}
			})
		})
	}
}
