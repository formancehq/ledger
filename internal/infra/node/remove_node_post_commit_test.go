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

	pending, ok := n.pendingRemovals.Load(pendingRemovalKey{3, 42})
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
		_, pending := n.pendingRemovals.Load(pendingRemovalKey{3, 1})

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
	n.pendingRemovals.Store(pendingRemovalKey{3, 1}, pending)

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

	barrierCount := 0
	n.pendingRemovals.Range(func(_ pendingRemovalKey, _ *pendingRemoval) bool {
		barrierCount++

		return true
	})
	require.Zero(t, barrierCount, "a member without an instance ID needs no admission barrier")
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

	_, pending := n.pendingRemovals.Load(pendingRemovalKey{3, 0})
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

func TestTrackCommittedRemovalReusesExactBarrierAndRejectsConflictingIdentity(t *testing.T) {
	t.Parallel()

	instanceID := []byte("0123456789abcdef")
	pending := &pendingRemoval{
		instanceID:     instanceID,
		committedIndex: 42,
	}
	n := &Node{}
	n.pendingRemovals.Store(pendingRemovalKey{3, 42}, pending)

	actual, err := n.trackCommittedRemoval(3, instanceID, 42)
	require.NoError(t, err)
	require.Same(t, pending, actual)

	actual, err = n.trackCommittedRemoval(3, []byte("fedcba9876543210"), 42)
	require.Nil(t, actual)
	require.ErrorContains(t, err, "conflicting instance IDs")
}

func TestTrackCommittedRemovalKeepsIndependentBarriersForRepeatedRemoval(t *testing.T) {
	t.Parallel()

	for name, newID := range map[string][]byte{
		"same incarnation": []byte("0123456789abcdef"),
		"new incarnation":  []byte("fedcba9876543210"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			setup := newTestApplierSetup(t)
			runDone := make(chan struct{})
			t.Cleanup(func() { close(runDone) })
			m := newTestMembership(t)
			oldID := []byte("0123456789abcdef")
			require.NoError(t, m.UnregisterAndBlacklist(4, oldID, 1))
			require.NoError(t, m.UnregisterAndBlacklist(4, newID, 2))
			n := &Node{fsm: setup.fsm, membership: m, logger: logging.Testing(), runDone: runDone}

			oldRemoval, err := n.trackCommittedRemoval(4, oldID, 29)
			require.NoError(t, err)
			newRemoval, err := n.trackCommittedRemoval(4, newID, 38)
			require.NoError(t, err)
			require.NotSame(t, oldRemoval, newRemoval)
			require.True(t, n.isRemovalPending(4, oldID))
			require.True(t, n.isRemovalPending(4, newID))

			cleared, err := n.verifyAndClearPendingRemoval(4, oldRemoval)
			require.NoError(t, err)
			require.True(t, cleared)
			_, oldPending := n.pendingRemovals.Load(pendingRemovalKey{4, 29})
			require.False(t, oldPending)
			_, newPending := n.pendingRemovals.Load(pendingRemovalKey{4, 38})
			require.True(t, newPending, "old cleanup must not release the later removal's barrier")
			require.True(t, n.isRemovalPending(4, newID))

			cleared, err = n.verifyAndClearPendingRemoval(4, newRemoval)
			require.NoError(t, err)
			require.True(t, cleared)
			require.False(t, n.isRemovalPending(4, newID))
		})
	}
}
