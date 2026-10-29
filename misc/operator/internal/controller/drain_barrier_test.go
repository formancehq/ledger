package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type testDrainCluster struct {
	decision drainDecision
	current  drainDecision
	status   map[uint64]drainNodeStatus
	failed   uint64
}

func (c *testDrainCluster) BeginDrain(_ context.Context, epoch string) (drainDecision, error) {
	if c.decision.Epoch != epoch {
		return drainDecision{}, errors.New("wrong epoch")
	}

	return c.decision, nil
}
func (c *testDrainCluster) CurrentDrain(context.Context) (drainDecision, error) {
	return c.current, nil
}
func (c *testDrainCluster) NodeStatus(_ context.Context, id uint64) (drainNodeStatus, error) {
	if id == c.failed {
		return drainNodeStatus{}, errors.New("unreachable")
	}

	return c.status[id], nil
}

func TestEnsureDrainRequiresEveryOriginalMemberDurableApply(t *testing.T) {
	t.Parallel()
	c := &testDrainCluster{
		decision: drainDecision{Epoch: "e1", Index: 42, Members: []uint64{1, 2, 3}},
		current:  drainDecision{Epoch: "e1", Index: 42, Members: []uint64{3, 2, 1}},
		status: map[uint64]drainNodeStatus{
			1: {NodeID: 1, DrainEpoch: "e1", DrainIndex: 42, LastPersistedIndex: 42, SyncStatus: "normal"},
			2: {NodeID: 2, DrainEpoch: "e1", DrainIndex: 42, LastPersistedIndex: 41, SyncStatus: "normal"},
			3: {NodeID: 3, DrainEpoch: "e1", DrainIndex: 42, LastPersistedIndex: 42, SyncStatus: "normal"},
		},
	}
	ready, err := ensureDrain(t.Context(), c, "e1")
	require.NoError(t, err)
	require.False(t, ready)
	c.status[2] = drainNodeStatus{NodeID: 2, DrainEpoch: "e1", DrainIndex: 42, LastPersistedIndex: 42, SyncStatus: "normal"}
	ready, err = ensureDrain(t.Context(), c, "e1")
	require.NoError(t, err)
	require.True(t, ready)
	c.failed = 3
	ready, err = ensureDrain(t.Context(), c, "e1")
	require.False(t, ready)
	require.ErrorContains(t, err, "node 3")
}

func TestEnsureDrainRejectsDriftAndUnsyncedMember(t *testing.T) {
	t.Parallel()
	c := &testDrainCluster{
		decision: drainDecision{Epoch: "e1", Index: 7, Members: []uint64{1}},
		current:  drainDecision{Epoch: "e1", Index: 7, Members: []uint64{1}},
		status: map[uint64]drainNodeStatus{
			1: {NodeID: 1, DrainEpoch: "e1", DrainIndex: 7, LastPersistedIndex: 7, SyncStatus: "syncing"},
		},
	}
	ready, err := ensureDrain(t.Context(), c, "e1")
	require.NoError(t, err)
	require.False(t, ready)
	c.status[1] = drainNodeStatus{NodeID: 1, DrainEpoch: "other", DrainIndex: 7, LastPersistedIndex: 7, SyncStatus: "normal"}
	_, err = ensureDrain(t.Context(), c, "e1")
	require.ErrorContains(t, err, "expected drain marker")
	c.current.Members = []uint64{1, 2}
	_, err = ensureDrain(t.Context(), c, "e1")
	require.ErrorContains(t, err, "membership changed")
}
