package node

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newHistoricalRemovalLifecycleSetup(t *testing.T) (*Node, *testApplierSetup) {
	t.Helper()

	var m *membership.Membership
	setup := newTestApplierSetupWithConfChangeHandler(t, make(LocalResponses, 1024), func(entry *raftpb.Entry, session *dal.WriteSession) error {
		return m.WriteConfChange(entry, session)
	})
	var err error
	m, err = membership.NewMembership(membership.NewPeerStore(setup.store), noopMemTransport{}, noopMemPool{},
		1, "self:7777", "self:8888", []byte("self-instance-id"), logging.Testing(), membership.NoopClusterPolicyProvider)
	require.NoError(t, err)
	n := &Node{fsm: setup.fsm, membership: m, logger: logging.Testing(), runDone: make(chan struct{})}
	t.Cleanup(func() { close(n.runDone) })

	return n, setup
}

func historicalRemovalLifecycleEntry(t *testing.T, index uint64, kind raftpb.ConfChangeType, identity []byte) *raftpb.Entry {
	t.Helper()

	payload := membership.ConfChangeContext{InstanceID: identity}
	if kind == raftpb.ConfChangeAddLearnerNode {
		payload.RaftAddress = "peer:7777"
		payload.ServiceAddress = "peer:8888"
	}
	encoded, err := membership.MarshalConfChangeContext(payload)
	require.NoError(t, err)
	data, err := proto.Marshal(&raftpb.ConfChangeV2{
		Context: encoded,
		Changes: []*raftpb.ConfChangeSingle{{Type: new(kind), NodeId: new(uint64(2))}},
	})
	require.NoError(t, err)

	return &raftpb.Entry{Index: new(index), Term: new(uint64(1)), Type: new(raftpb.EntryConfChangeV2), Data: data}
}

func TestHistoricalRemovalLifecycleCanceledCallersKeepIndependentEvents(t *testing.T) {
	t.Parallel()

	n, _ := newHistoricalRemovalLifecycleSetup(t)
	oldIdentity := []byte("old-instance-id-")
	newIdentity := []byte("new-instance-id-")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for _, event := range []struct {
		index    uint64
		identity []byte
	}{
		{1, oldIdentity},
		{3, newIdentity},
		{5, oldIdentity},
	} {
		err := n.waitForRemovalApplied(ctx, 2, event.identity, event.index)
		var committedErr *RemoveNodeCommittedError
		require.ErrorAs(t, err, &committedErr)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, event.index, committedErr.CommittedIndex)
		pending, ok := n.pendingRemovals.load(2, event.index)
		require.True(t, ok)
		require.Equal(t, event.identity, pending.instanceID)
		require.True(t, n.isRemovalPending(2, oldIdentity))
	}
	require.True(t, n.isRemovalPending(2, newIdentity))
	require.False(t, n.isRemovalPending(2, []byte("fresh-identity--")))

	pending, ok := n.pendingRemovals.load(2, 1)
	require.True(t, ok)
	duplicate, err := n.trackCommittedRemoval(2, oldIdentity, 1)
	require.NoError(t, err)
	require.Same(t, pending, duplicate, "duplicate commit observation must reuse its lifecycle barrier")
	oldIdentity[0] = 'X'
	require.Equal(t, []byte("old-instance-id-"), pending.instanceID, "a caller must not mutate the tracked identity")
	require.True(t, n.isRemovalPending(2, []byte("old-instance-id-")))
}

func TestHistoricalRemovalLifecycleFailedVerificationRetainsEveryEvent(t *testing.T) {
	t.Parallel()

	n, _ := newHistoricalRemovalLifecycleSetup(t)
	oldIdentity := []byte("old-instance-id-")
	newIdentity := []byte("new-instance-id-")
	oldPending, err := n.trackCommittedRemoval(2, oldIdentity, 1)
	require.NoError(t, err)
	newPending, err := n.trackCommittedRemoval(2, newIdentity, 3)
	require.NoError(t, err)

	for _, pending := range []*pendingRemoval{oldPending, newPending} {
		cleared, err := n.verifyAndClearPendingRemoval(2, pending)
		require.ErrorContains(t, err, "invariant: applied removal is missing its removed-member tombstone")
		require.False(t, cleared)
		require.True(t, n.isRemovalPending(2, oldIdentity))
		require.True(t, n.isRemovalPending(2, newIdentity))
	}
}

func TestHistoricalRemovalLifecycleDurableCleanupCannotClearLaterEvent(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		n, setup := newHistoricalRemovalLifecycleSetup(t)
		oldIdentity := []byte("old-instance-id-")
		newIdentity := []byte("new-instance-id-")
		require.NoError(t, n.membership.Register(2, "peer:7777", "peer:8888", oldIdentity))
		oldPending, err := n.trackCommittedRemoval(2, oldIdentity, 1)
		require.NoError(t, err)
		newPending, err := n.trackCommittedRemoval(2, newIdentity, 3)
		require.NoError(t, err)
		_, err = n.trackCommittedRemoval(2, oldIdentity, 5)
		require.NoError(t, err)

		applierDone := make(chan error, 1)
		go func() { applierDone <- setup.applier.Run(t.Context(), setup.stop) }()
		t.Cleanup(func() {
			close(setup.stop)
			require.NoError(t, <-applierDone)
		})
		apply := func(entry *raftpb.Entry) {
			setup.applier.Submit([]*raftpb.Entry{entry}, nil, nil, setup.stop)
			setup.applier.Drain(setup.stop)
		}
		apply(historicalRemovalLifecycleEntry(t, 1, raftpb.ConfChangeRemoveNode, oldIdentity))
		// Every background cleanup waiter has now either finished or parked on
		// an index that is not yet durable.
		synctest.Wait()
		_, pending := n.pendingRemovals.load(2, 1)
		require.False(t, pending)
		require.True(t, n.isRemovalPending(2, oldIdentity), "the later removal of the same identity must retain its own guard")
		require.True(t, n.isRemovalPending(2, newIdentity), "durable application of removal 1 must not release removal 3")
		stored, ok := n.pendingRemovals.load(2, 3)
		require.True(t, ok)
		require.Same(t, newPending, stored)

		// A delayed foreground verification can still hold the first event after
		// its background cleanup. It must not release a later event at this ordinal.
		cleared, err := n.verifyAndClearPendingRemoval(2, oldPending)
		require.NoError(t, err)
		require.False(t, cleared)
		require.True(t, n.isRemovalPending(2, newIdentity))

		apply(historicalRemovalLifecycleEntry(t, 2, raftpb.ConfChangeAddLearnerNode, newIdentity))
		apply(historicalRemovalLifecycleEntry(t, 3, raftpb.ConfChangeRemoveNode, newIdentity))
		synctest.Wait()
		require.False(t, n.isRemovalPending(2, newIdentity))
		require.True(t, n.isRemovalPending(2, oldIdentity))
		apply(historicalRemovalLifecycleEntry(t, 4, raftpb.ConfChangeAddLearnerNode, oldIdentity))
		apply(historicalRemovalLifecycleEntry(t, 5, raftpb.ConfChangeRemoveNode, oldIdentity))
		synctest.Wait()
		require.False(t, n.isRemovalPending(2, oldIdentity))
		for _, identity := range [][]byte{oldIdentity, newIdentity} {
			removed, err := n.membership.IsRemoved(2, identity)
			require.NoError(t, err)
			require.True(t, removed)
		}
	})
}

func TestNewNodeReplaysHistoricalRemovalCycles(t *testing.T) {
	t.Parallel()

	fixture, setup := newHistoricalRemovalLifecycleSetup(t)
	oldIdentity := []byte("old-instance-id-")
	newIdentity := []byte("new-instance-id-")
	require.NoError(t, fixture.membership.Register(2, "peer:7777", "peer:8888", oldIdentity))
	// Retain the pre-crash ConfState: all committed changes exist only in the
	// WAL, so NewNode must recover their durable membership effects by replay.
	require.NoError(t, setup.wal.UpdateSnapshotConfState(&raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}))
	entries := []*raftpb.Entry{
		historicalRemovalLifecycleEntry(t, 1, raftpb.ConfChangeRemoveNode, oldIdentity),
		historicalRemovalLifecycleEntry(t, 2, raftpb.ConfChangeAddLearnerNode, newIdentity),
		historicalRemovalLifecycleEntry(t, 3, raftpb.ConfChangeRemoveNode, newIdentity),
	}
	require.NoError(t, setup.wal.Append(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(3))}, entries))

	n, err := NewNode(NodeConfig{
		NodeID: 1, AdvertiseAddr: "self:7777", ServiceAdvertiseAddr: "self:8888", InstanceID: []byte("self-instance-id"),
	}, nil, setup.applier, logging.Testing(), noop.Meter{}, setup.wal,
		setup.fsm, setup.applier.recovery, setup.applier.synchronizer, fixture.membership, setup.responseSink)
	require.NoError(t, err)
	require.NotNil(t, n)
	require.Equal(t, uint64(3), setup.fsm.LastPersistedIndex())
	require.NotContains(t, n.membership.PeerAddresses(), uint64(2))
	for _, identity := range [][]byte{oldIdentity, newIdentity} {
		removed, err := n.membership.IsRemoved(2, identity)
		require.NoError(t, err)
		require.True(t, removed, "WAL replay must preserve both historical tombstones")
	}
}
