package node

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestWaitForRemovalAppliedKeepsAdmissionBarrierAfterCallerStopsWaiting(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	n := &Node{
		fsm:        setup.fsm,
		membership: newTestMembership(t),
		logger:     logging.Testing(),
	}

	instanceID := []byte("0123456789abcdef")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := n.waitForRemovalApplied(ctx, 3, instanceID, 42)
	var committedErr *RemoveNodeCommittedError
	require.ErrorAs(t, err, &committedErr)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, uint64(3), committedErr.NodeID)
	require.Equal(t, uint64(42), committedErr.AppliedIndex)

	pending, ok := n.pendingRemovals.Load(3)
	require.True(t, ok)
	require.Equal(t, instanceID, pending.instanceID)
	require.Equal(t, uint64(42), pending.appliedIndex)
	require.True(t, n.isRemovalPending(3, instanceID),
		"the removed live pod must remain blocked until the tombstone's FSM index is durable")

	otherInstance := []byte("fedcba9876543210")
	require.False(t, n.isRemovalPending(3, otherInstance),
		"a fresh pod identity at the reused ordinal is not covered by the old removal barrier")
	require.False(t, errors.Is(err, ErrNodeNotInCluster))
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
	require.Equal(t, uint64(42), committedErr.AppliedIndex)

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
