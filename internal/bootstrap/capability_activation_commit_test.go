package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestCapabilityActivationDirectThroughRaftAfterDrain(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	supported := []capabilities.CapabilityID{capabilities.StrictAccountTypes}
	members := []capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Reachable: true, Capabilities: supported}}
	var store *dal.Store
	gate := func(_ context.Context, proposal *node.Proposal) error {
		policy, err := query.ReadClusterPolicy(store)
		if err != nil {
			return err
		}

		return validateProposalCapabilityAdmissionWithSupported(policy, proposal, members, supported)
	}
	raftNode, store := newHookCommitDrainNode(t, signal.NewNotifications(), node.WithProposalAdmission(gate))
	ready := make(chan struct{})
	runDone := make(chan error, 1)
	go func() { runDone <- raftNode.Run(ctx, ready) }()
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, raftNode.Node.Stop(stopCtx))
		select {
		case err := <-runDone:
			require.NoError(t, err)
		case <-stopCtx.Done():
			t.Error("activation node did not stop")
		}
	})
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("activation node did not start")
	}
	require.Eventually(t, raftNode.IsLeader, 5*time.Second, time.Millisecond)
	require.NoError(t, raftNode.WaitLeaderReady(ctx))
	_, err := raftNode.BeginDrain(ctx, "direct-activation", []uint64{1}, 0)
	require.NoError(t, err)
	_, err = raftNode.EndDrain(ctx, "direct-activation", 0)
	require.NoError(t, err)
	business := commands.NewCommand(&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: "after-drain", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
	}}})
	proposal, err := node.NewProposal(business.GetId(), business)
	require.NoError(t, err)
	_, err = raftNode.Propose(ctx, proposal)
	require.ErrorIs(t, err, capabilities.ErrActivationPending)
	proposeControl := func(command *raftcmdpb.Proposal) error {
		release, err := raftNode.MembershipCoordinator().AcquireClusterPolicy(ctx)
		if err != nil {
			return err
		}
		command.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterPolicy)
		control, err := node.NewProposal(command.GetId(), command)
		if err != nil {
			release(nil)

			return err
		}
		raftNode.IndexTracker().Lock()
		control.SetPredictedIndex(raftNode.IndexTracker().Next())
		future, err := raftNode.Propose(ctx, control)
		raftNode.IndexTracker().Unlock()
		release(future)
		if err != nil {
			return err
		}
		result, err := future.Wait(ctx)
		if err != nil {
			return err
		}

		return result.Error
	}
	attempts := 0
	lostResponse := errors.New("activation committed but response lost")
	admit := func(_ context.Context, activation *commonpb.CapabilityActivation) (*domain.ApplyResult, error) {
		attempts++
		require.True(t, activation.GetActive())
		cfg := validBaseConfig()
		cfg.ClusterPolicyRevision = 7
		cfg.QueryCheckpointLimit = 42
		ordinary := &fakeAdmission{}
		reconcileClusterPolicy(ctx, ordinary, store, cfg, raftNode.IsLeader, logging.Testing())
		require.NotNil(t, ordinary.lastPolicy)
		configuration := commands.NewCommand()
		configuration.Payload = &raftcmdpb.Proposal_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{
			Policy: ordinary.lastPolicy, ConfigurationOnly: true,
		}}
		require.NoError(t, proposeControl(configuration))
		require.ErrorIs(t, gate(ctx, proposal), capabilities.ErrActivationPending)
		command := commands.NewCommand()
		command.Payload = &raftcmdpb.Proposal_CapabilityActivation{CapabilityActivation: activation}
		require.NoError(t, proposeControl(command))

		return nil, lostResponse
	}
	run := func() error {
		return activateReadyCapabilities(ctx, admit, store, raftNode.IsLeader, func() []capabilities.Member { return members }, supported)
	}
	require.ErrorIs(t, run(), lostResponse)
	require.NoError(t, run())
	require.Equal(t, 1, attempts)
	policy, err := query.ReadClusterPolicy(store)
	require.NoError(t, err)
	require.Equal(t, uint64(7), policy.GetRevision())
	require.Equal(t, uint64(42), policy.GetQueryCheckpointLimit())
	require.Equal(t, []uint32{uint32(capabilities.StrictAccountTypes)}, capabilities.ActiveIDs(policy))
	require.NoError(t, gate(ctx, proposal))
}
