package state

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func drainProposal(id uint64, epoch string) *raftcmdpb.Proposal {
	p := makeProposal(id)
	p.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_DrainMarker{DrainMarker: &raftcmdpb.DrainMarker{
			Epoch: epoch, Members: []uint64{1, 2, 3},
		}},
	}}

	return p
}

func drainReleaseProposal(id uint64, epoch string) *raftcmdpb.Proposal {
	p := makeProposal(id)
	p.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_DrainRelease{DrainRelease: &raftcmdpb.DrainRelease{Epoch: epoch}},
	}}

	return p
}

func TestDrainAllowsEmptyMembershipBarrier(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	_, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, drainProposal(1, "epoch-one")))
	require.NoError(t, err)
	result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, makeProposal(2)))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	require.Equal(t, uint64(1), machine.DrainDecision().GetIndex())
}

func TestDrainMarkerClearedForCrossClusterRestore(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	_, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, drainProposal(1, "epoch-one")))
	require.NoError(t, err)
	decision, err := query.ReadDrainState(store)
	require.NoError(t, err)
	require.NotNil(t, decision)
	require.NoError(t, attributes.PrepareForBackup(store))
	decision, err = query.ReadDrainState(store)
	require.NoError(t, err)
	require.Nil(t, decision)
}

func TestDrainMarkerOrdersWritesAndSurvivesRecovery(t *testing.T) {
	t.Parallel()
	for replica := range 3 {
		t.Run(fmt.Sprintf("replica-%d", replica), func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachine(t)
			installTestClusterPolicy(machine)
			pre, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, createLedgerOrder("before-drain"))))
			require.NoError(t, err)
			require.NoError(t, pre.Results[0].Error)

			barrier, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, drainProposal(2, "epoch-one")))
			require.NoError(t, err)
			require.NoError(t, barrier.Results[0].Error)
			require.GreaterOrEqual(t, machine.LastPersistedIndex(), uint64(2))
			decision, err := query.ReadDrainState(store)
			require.NoError(t, err)
			require.Equal(t, &raftcmdpb.DrainState{Epoch: "epoch-one", Index: 2, Members: []uint64{1, 2, 3}}, decision)

			blocked, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 3, makeProposal(3, createLedgerOrder("after-drain"))))
			require.NoError(t, err)
			require.ErrorIs(t, blocked.Results[0].Error, domain.ErrMaintenanceMode)
			technical := makeProposal(4)
			technical.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
				Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: 12}},
			}}
			blockedTechnical, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 4, technical))
			require.NoError(t, err)
			require.ErrorIs(t, blockedTechnical.Results[0].Error, domain.ErrMaintenanceMode)
			beforeAudit := machine.State.NextAuditSequenceID

			machine = recoverMachineOnStore(t, store)
			installTestClusterPolicy(machine)
			epoch, index := machine.Drain()
			require.Equal(t, "epoch-one", epoch)
			require.Equal(t, uint64(2), index)
			require.Equal(t, []uint64{1, 2, 3}, machine.DrainDecision().GetMembers())
			blocked, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 5, makeProposal(5, createLedgerOrder("after-restart"))))
			require.NoError(t, err)
			require.ErrorIs(t, blocked.Results[0].Error, domain.ErrMaintenanceMode)

			duplicate, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 6, drainProposal(6, "epoch-one")))
			require.NoError(t, err)
			require.NoError(t, duplicate.Results[0].Error)
			stored, err := query.ReadDrainState(store)
			require.NoError(t, err)
			require.Equal(t, decision, stored)
			require.Equal(t, beforeAudit, machine.State.NextAuditSequenceID)

			released, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 7, drainReleaseProposal(7, "epoch-one")))
			require.NoError(t, err)
			require.NoError(t, released.Results[0].Error)
			stored, err = query.ReadDrainState(store)
			require.NoError(t, err)
			require.Equal(t, uint64(7), stored.GetReleasedIndex())
			epoch, index = machine.Drain()
			require.Empty(t, epoch)
			require.Zero(t, index)
			require.Equal(t, uint64(7), machine.DrainReleaseIndex())
			resumed, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 8, makeProposal(8, createLedgerOrder("after-release"))))
			require.NoError(t, err)
			require.NoError(t, resumed.Results[0].Error)

			machine = recoverMachineOnStore(t, store)
			installTestClusterPolicy(machine)
			epoch, index = machine.Drain()
			require.Empty(t, epoch)
			require.Zero(t, index)
			require.Equal(t, uint64(7), machine.DrainReleaseIndex())
			newDrain, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 9, drainProposal(9, "epoch-two")))
			require.NoError(t, err)
			require.NoError(t, newDrain.Results[0].Error)
			stored, err = query.ReadDrainState(store)
			require.NoError(t, err)
			require.Equal(t, "epoch-two", stored.GetEpoch())
			require.Zero(t, stored.GetReleasedIndex())
			require.Equal(t, []string{"epoch-one"}, stored.GetPriorEpochs())
			staleRelease, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 10, drainReleaseProposal(10, "epoch-one")))
			require.NoError(t, err)
			require.NoError(t, staleRelease.Results[0].Error)
			epoch, index = machine.Drain()
			require.Equal(t, "epoch-two", epoch)
			require.Equal(t, uint64(9), index)
			released, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 11, drainReleaseProposal(11, "epoch-two")))
			require.NoError(t, err)
			require.NoError(t, released.Results[0].Error)
			staleMarker, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 12, drainProposal(12, "epoch-one")))
			require.NoError(t, err)
			require.NoError(t, staleMarker.Results[0].Error)
			stored, err = query.ReadDrainState(store)
			require.NoError(t, err)
			require.Equal(t, "epoch-two", stored.GetEpoch())
			require.Equal(t, uint64(11), stored.GetReleasedIndex())
			machine = recoverMachineOnStore(t, store)
			staleMarker, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 13, drainProposal(13, "epoch-one")))
			require.NoError(t, err)
			require.NoError(t, staleMarker.Results[0].Error)
			require.Equal(t, "epoch-two", machine.DrainDecision().GetEpoch())
			require.Equal(t, uint64(11), machine.DrainDecision().GetReleasedIndex())
		})
	}
}
