package state

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// The rolling-upgrade barrier must prevent even a previously admitted proposal
// from reaching address validation while old and new voters coexist.
func TestAccountMetadataMaintenanceBarrierSurvivesRecovery(t *testing.T) {
	t.Parallel()
	for replica := range 3 {
		t.Run(fmt.Sprintf("replica-%d", replica), func(t *testing.T) {
			t.Parallel()
			machine, store, attrs := newTestMachine(t)
			const ledger = "maintenance-metadata"
			maintenance := func(enabled bool) *raftcmdpb.Order {
				return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
					SystemScoped: &raftcmdpb.SystemScopedOrder{
						Payload: &raftcmdpb.SystemScopedOrder_SetMaintenanceMode{
							SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{Enabled: enabled},
						},
					},
				}}
			}
			for i, order := range []*raftcmdpb.Order{createLedgerOrder(ledger), maintenance(true)} {
				index := uint64(i + 1)
				result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, index, makeProposal(index, order)))
				require.NoError(t, err)
				require.NoError(t, result.Results[0].Error)
			}
			require.True(t, machine.sharedState.MaintenanceMode())
			require.GreaterOrEqual(t, machine.LastPersistedIndex(), uint64(2))
			before := readBusinessProjections(t, store, attrs)
			readAuditAndMetadata := func() map[string]any {
				handle, err := store.NewReadHandle()
				require.NoError(t, err)
				defer func() { require.NoError(t, handle.Close()) }()
				audit, err := query.ReadLastAuditEntry(handle)
				require.NoError(t, err)
				metadata, err := attrs.Metadata.ComputeAllForPrefix(handle, nil)
				require.NoError(t, err)

				return map[string]any{"audit": audit, "metadata": metadata}
			}
			beforeAuditAndMetadata := readAuditAndMetadata()

			for _, index := range []uint64{3, 4} {
				if index == 4 {
					machine = recoverMachineOnStore(t, store)
					require.NoError(t, NewRecovery(machine, store).RestoreCacheFromStore())
					installTestClusterPolicy(machine)
					require.True(t, machine.sharedState.MaintenanceMode(), "restart must retain the rollout gate")
				}
				order := createTransactionOrder(ledger, true, newPosting("world", "alice", "EUR", 10))
				order.GetLedgerScoped().GetApply().GetCreateTransaction().AccountMetadata = map[string]*commonpb.MetadataMap{
					"":     {Values: map[string]*commonpb.MetadataValue{"note": commonpb.NewStringValue("empty address")}},
					"a b":  {Values: map[string]*commonpb.MetadataValue{"note": commonpb.NewStringValue("invalid character")}},
					"a::b": {Values: map[string]*commonpb.MetadataValue{"note": commonpb.NewStringValue("empty segment")}},
				}
				proposal := makeProposal(index, order)
				proposal.Idempotency = &commonpb.Idempotency{Key: "blocked-during-rollout"}
				result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, index, proposal))
				require.NoError(t, err)
				require.ErrorIs(t, result.Results[0].Error, domain.ErrMaintenanceMode,
					"the FSM must reject before selecting any invalid address")
				require.Empty(t, result.Results[0].Logs)
				require.Equal(t, before, readBusinessProjections(t, store, attrs))
				require.Equal(t, beforeAuditAndMetadata, readAuditAndMetadata(),
					"maintenance rejection must preserve audit and account metadata")
				_, frozen := machine.Registry.Idempotency.Get("blocked-during-rollout")
				require.False(t, frozen, "maintenance rejection must not freeze an idempotency outcome")
			}

			result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 5, makeProposal(5, maintenance(false))))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)
			require.False(t, machine.sharedState.MaintenanceMode())
			proposal := makeProposal(6, createTransactionOrder(ledger, true, newPosting("world", "alice", "EUR", 10)))
			proposal.Idempotency = &commonpb.Idempotency{Key: "blocked-during-rollout"}
			result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 6, proposal))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)
			require.Len(t, result.Results[0].Logs, 1)
			require.False(t, result.Results[0].Replayed)
			require.NotEqual(t, before, readBusinessProjections(t, store, attrs))
		})
	}
}
