package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestWaitForRemovalAppliedKeepsAdmissionBarrierAfterCallerStopsWaiting(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	runDone := make(chan struct{})
	t.Cleanup(func() { close(runDone) })

	n := &Node{
		fsm:        setup.fsm,
		membership: newTestMembership(t),
		logger:     logging.Testing(),
		runDone:    runDone,
	}

	instanceID := []byte("0123456789abcdef")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := n.waitForRemovalApplied(ctx, 3, instanceID, 42)
	var committedErr *RemoveNodeCommittedError
	require.ErrorAs(t, err, &committedErr)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, uint64(3), committedErr.NodeID)
	require.Equal(t, uint64(42), committedErr.CommittedIndex)
	require.Equal(
		t,
		"node 3 removal committed at raft index 42; durable FSM application is still pending",
		committedErr.Error(),
	)

	pending, ok := n.pendingRemovals.Load(3)
	require.True(t, ok)
	require.Equal(t, instanceID, pending.instanceID)
	require.Equal(t, uint64(42), pending.committedIndex)
	require.True(t, n.isRemovalPending(3, instanceID),
		"the removed live pod must remain blocked until the tombstone's FSM index is durable")

	otherInstance := []byte("fedcba9876543210")
	require.False(t, n.isRemovalPending(3, otherInstance),
		"a fresh pod identity at the reused ordinal is not covered by the old removal barrier")
	require.False(t, errors.Is(err, ErrNodeNotInCluster))
}

func TestTrackCommittedRemovalClearsBarrierAfterDurableApplyWithoutCaller(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	runDone := make(chan struct{})
	t.Cleanup(func() { close(runDone) })
	m := newTestMembership(t)
	instanceID := []byte("0123456789abcdef")
	require.NoError(t, m.UnregisterAndBlacklist(3, instanceID, 1))

	n := &Node{
		fsm:        setup.fsm,
		membership: m,
		logger:     logging.Testing(),
		runDone:    runDone,
	}

	_, err := n.trackCommittedRemoval(3, instanceID, 1)
	require.NoError(t, err)
	require.True(t, n.isRemovalPending(3, instanceID))

	entry, _ := makeCreateLedgerEntry(t, 1, "removal-barrier-cleanup")
	setup.applyEntry(t, context.Background(), entry)

	require.Eventually(t, func() bool {
		_, pending := n.pendingRemovals.Load(3)

		return !pending
	}, time.Second, 10*time.Millisecond,
		"the node-local barrier must be cleared after the committed index is durable")
}

func TestVerifyAndClearPendingRemovalRetainsBarrierWithoutTombstone(t *testing.T) {
	t.Parallel()

	n := &Node{
		membership: newTestMembership(t),
	}
	pending := &pendingRemoval{
		instanceID:     []byte("0123456789abcdef"),
		committedIndex: 1,
	}
	n.pendingRemovals.Store(3, pending)

	cleared, err := n.verifyAndClearPendingRemoval(3, pending)
	require.ErrorContains(t, err, "invariant")
	require.False(t, cleared)
	require.True(t, n.isRemovalPending(3, pending.instanceID),
		"a missing tombstone must retain the admission barrier")
}

func TestWaitForRemovalAppliedWithoutInstanceIDStillWaitsForFSM(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	n := &Node{
		fsm:        setup.fsm,
		membership: newTestMembership(t),
		logger:     logging.Testing(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := n.waitForRemovalApplied(ctx, 3, nil, 42)
	var committedErr *RemoveNodeCommittedError
	require.ErrorAs(t, err, &committedErr)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, uint64(3), committedErr.NodeID)
	require.Equal(t, uint64(42), committedErr.CommittedIndex)

	_, pending := n.pendingRemovals.Load(3)
	require.False(t, pending, "a member without an instance ID needs no admission barrier")
}

func TestWaitForRemovalAppliedVerifiesTombstoneAfterDurableApply(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	m := newTestMembership(t)
	instanceID := []byte("0123456789abcdef")
	require.NoError(t, m.UnregisterAndBlacklist(3, instanceID, 1))

	n := &Node{
		fsm:        setup.fsm,
		membership: m,
		logger:     logging.Testing(),
	}

	require.NoError(t, n.waitForRemovalApplied(context.Background(), 3, instanceID, 0))

	_, pending := n.pendingRemovals.Load(3)
	require.False(t, pending, "the admission barrier is cleared after durable tombstone verification")
}

func TestWaitForRemovalAppliedRejectsMissingTombstoneAfterDurableApply(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	n := &Node{
		fsm:        setup.fsm,
		membership: newTestMembership(t),
		logger:     logging.Testing(),
	}

	err := n.waitForRemovalApplied(
		context.Background(),
		3,
		[]byte("0123456789abcdef"),
		0,
	)
	var committedErr *RemoveNodeCommittedError
	require.ErrorAs(t, err, &committedErr)
	require.Equal(t, uint64(0), committedErr.CommittedIndex)
	require.ErrorContains(t, committedErr.Cause, "missing its removed-member tombstone")
	require.True(t, n.isRemovalPending(3, []byte("0123456789abcdef")),
		"an invariant failure must retain the admission barrier")
}

func TestTrackCommittedRemovalReusesExactBarrierAndRejectsConflicts(t *testing.T) {
	t.Parallel()

	instanceID := []byte("0123456789abcdef")
	pending := &pendingRemoval{
		instanceID:     instanceID,
		committedIndex: 42,
	}
	n := &Node{}
	n.pendingRemovals.Store(3, pending)

	actual, err := n.trackCommittedRemoval(3, instanceID, 42)
	require.NoError(t, err)
	require.Same(t, pending, actual)

	for name, conflict := range map[string]struct {
		instanceID     []byte
		committedIndex uint64
	}{
		"instance identity": {
			instanceID:     []byte("fedcba9876543210"),
			committedIndex: 42,
		},
		"committed index": {
			instanceID:     instanceID,
			committedIndex: 43,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			actual, err := n.trackCommittedRemoval(3, conflict.instanceID, conflict.committedIndex)
			require.Nil(t, actual)
			require.ErrorContains(t, err, "conflicts with pending removal at index 42")
		})
	}
}
