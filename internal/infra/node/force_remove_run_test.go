package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestForceRemoveNodeDurabilityFailureStopsRun(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	// A sole voter can elect itself through the real Run loops. Removing its
	// learner exercises the terminal lifecycle independently of quorum sizing.
	require.NoError(t, setup.wal.UpdateSnapshotConfState(&raftpb.ConfState{
		Voters:   []uint64{1},
		Learners: []uint64{2},
	}))
	injected := errors.New("force-remove snapshot persistence failed")
	w := &forceRemoveWAL{
		WAL:          setup.wal,
		walDir:       setup.walDir,
		failureStage: forceRemoveBeforeSnapshotPersistence,
		updateErr:    injected,
		started:      make(chan struct{}),
	}
	n, err := NewNode(
		NodeConfig{
			NodeID:                 1,
			AdvertiseAddr:          "node-1:7000",
			ServiceAdvertiseAddr:   "node-1:8000",
			InstanceID:             []byte("0000000000000001"),
			TickInterval:           time.Millisecond,
			ProcessingTickInterval: time.Millisecond,
			MaintenanceInterval:    time.Hour,
		},
		newForceRemoveTransport(t), setup.applier, logging.Testing(),
		noop.NewMeterProvider().Meter("force-remove-run"), w, setup.fsm,
		setup.applier.recovery, setup.applier.synchronizer,
		newTestMembership(t), setup.responseSink,
	)
	require.NoError(t, err)

	ready := make(chan struct{})
	runErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runErr <- n.Run(context.Background(), ready)
	}()
	t.Cleanup(func() {
		// Also stop and join Run when an earlier assertion fails, before the
		// shared fixture closes the WAL, spool, and Pebble store.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, n.Stop(ctx))
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Node.Run did not finish shutdown")
		}
	})

	select {
	case <-ready:
	case err := <-runErr:
		t.Fatalf("Node.Run exited before readiness: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Node.Run did not become ready")
	}
	require.Eventually(t, n.IsLeader, 5*time.Second, time.Millisecond)

	forceErr := make(chan error, 1)
	go func() {
		forceErr <- n.ForceRemoveNode(context.Background(), 2)
	}()
	require.ErrorIs(t, receiveForceRemoveError(t, forceErr), injected)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Node.Run continued after terminal force-remove failure")
	}
	err = <-runErr
	require.ErrorIs(t, err, injected)
	require.ErrorContains(t, err, "task pool error: persisting confstate after force-remove")
	require.Equal(t, 1, w.updateCount())
	select {
	case <-n.runDone:
	default:
		t.Fatal("Node.Run did not publish lifecycle completion")
	}
}
