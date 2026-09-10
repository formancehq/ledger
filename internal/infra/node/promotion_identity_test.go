package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
)

// requireCompletePromotion inspects the actual entry emitted by RawNode, so
// each production caller must propagate the payload through ProposeConfChange.
func requireCompletePromotion(t *testing.T, n *Node) (*raftpb.Entry, *raftpb.ConfChangeV2) {
	t.Helper()

	ready := n.rawNode.Ready()
	var promotions []*raftpb.Entry
	for _, entry := range ready.Entries {
		if entry.GetType() == raftpb.EntryConfChangeV2 {
			promotions = append(promotions, entry)
		}
	}
	require.Len(t, promotions, 1, "the production trigger must propose one promotion")
	entry := promotions[0]
	cc, ok, err := membership.UnmarshalConfChangeV2(entry)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, cc.GetChanges(), 1)
	require.Equal(t, raftpb.ConfChangeAddNode, cc.GetChanges()[0].GetType())
	require.Equal(t, uint64(2), cc.GetChanges()[0].GetNodeId())
	require.NoError(t, membership.ValidateConfChangeIdentities(cc))
	payload, err := membership.UnmarshalConfChangeContext(cc.GetContext())
	require.NoError(t, err)
	require.Equal(t, "old:7777", payload.RaftAddress)
	require.Equal(t, "old:8888", payload.ServiceAddress)
	require.Equal(t, []byte("peer-instance-id"), payload.InstanceID)

	return entry, cc
}

func TestPromoteLearner_ProposesCompleteRegisteredIdentity(t *testing.T) {
	t.Parallel()

	n := newConfiguredPeersTestNode(t)
	n.logger = logging.Testing()
	results := make(chan error, 1)
	go func() {
		results <- n.PromoteLearner(t.Context(), 2)
	}()

	cmd := <-n.clusterCommandCh
	err := cmd.fn()
	require.NoError(t, err)
	entry, cc := requireCompletePromotion(t, n)
	payload, err := membership.UnmarshalConfChangeContext(cc.GetContext())
	require.NoError(t, err)
	require.NotEmpty(t, payload.ProposalID, "manual promotion must retain waiter correlation")

	// Acknowledge the captured entry through the same correlation mechanism
	// as finishReady, then release the command response and its caller.
	pending, err := n.takePendingConfChange(cc, entry.GetIndex())
	require.NoError(t, err)
	require.NotNil(t, pending)
	pending.future.Resolve(pending.index, nil)
	cmd.errCh <- nil
	require.NoError(t, <-results)
}

func TestCheckAndPromoteLearners_ProposesCompleteRegisteredIdentity(t *testing.T) {
	t.Parallel()

	n := newConfiguredPeersTestNode(t)
	n.logger = logging.Testing()
	n.lastAutoPromote = make(map[uint64]time.Time)

	// A real replication acknowledgement makes the learner active and caught
	// up to the leader's initial no-op entry, satisfying the automatic gate.
	ack := msgWithIndex(raftpb.MsgAppResp, 2, 1, 1)
	ack.Term = new(n.rawNode.Status().GetTerm())
	require.NoError(t, n.rawNode.Step(ack))
	progress := n.rawNode.Status().Progress[2]
	require.True(t, progress.IsLearner)
	require.True(t, progress.RecentActive)
	require.Equal(t, uint64(1), progress.Match)

	require.NoError(t, n.checkAndPromoteLearners())
	_, cc := requireCompletePromotion(t, n)
	payload, err := membership.UnmarshalConfChangeContext(cc.GetContext())
	require.NoError(t, err)
	require.Empty(t, payload.ProposalID, "automatic promotion has no synchronous waiter")
	_, attempted := n.lastAutoPromote[2]
	require.True(t, attempted)
}

func TestCheckAndPromoteLearnersMissingRowStopsRun(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	require.NoError(t, setup.wal.UpdateSnapshotConfState(&raftpb.ConfState{
		Voters: []uint64{1}, Learners: []uint64{2},
	}))
	m := newTestMembership(t)
	require.NoError(t, m.Register(2, "node-2:7000", "node-2:8000", []byte("0000000000000002")))
	n, err := NewNode(
		NodeConfig{
			NodeID:                 1,
			AdvertiseAddr:          "node-1:7000",
			ServiceAdvertiseAddr:   "node-1:8000",
			InstanceID:             []byte("0000000000000001"),
			TickInterval:           time.Millisecond,
			ProcessingTickInterval: time.Millisecond,
			MaintenanceInterval:    time.Hour,
			AutoPromoteThreshold:   1,
		},
		newForceRemoveTransport(t), setup.applier, logging.Testing(),
		noop.NewMeterProvider().Meter("promotion-invariant-run"), setup.wal, setup.fsm,
		setup.applier.recovery, setup.applier.synchronizer, m, setup.responseSink,
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
		// Join the real Run tasks before the fixture closes their storage,
		// including when an assertion above the expected failure aborts.
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

	// Prepare the impossible state and a real replication acknowledgement
	// inside orchestrate. The subsequent production ticker, rather than the
	// test calling checkAndPromoteLearners, must surface the invariant to Run.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		armed := false
		require.NoError(collect, n.execClusterCommand(ctx, true, func() error {
			status := n.rawNode.Status()
			if status.Progress[1].Match == 0 {
				// The self acknowledgement follows durable append of the
				// leader's no-op; election alone does not establish it.
				return nil
			}
			n.membership.Remove(2)
			ack := msgWithIndex(raftpb.MsgAppResp, 2, 1, status.Progress[1].Match)
			ack.Term = new(status.GetTerm())
			armed = true

			return n.rawNode.Step(ack)
		}))

		require.True(collect, armed)
	}, 5*time.Second, time.Millisecond)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Node.Run continued after automatic promotion invariant failure")
	}
	require.ErrorContains(t, <-runErr, "task pool error: invariant: learner 2 has no membership row")
	require.True(t, n.rawNode.Status().Progress[2].IsLearner, "failed promotion must preserve the learner")
	require.NotContains(t, n.lastAutoPromote, uint64(2))
	select {
	case <-n.runDone:
	default:
		t.Fatal("Node.Run did not publish lifecycle completion")
	}
}
