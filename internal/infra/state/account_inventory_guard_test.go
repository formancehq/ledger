package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestAccountInventoryGuardRejectsSameCountKeySwapAndRecreation(t *testing.T) {
	fsm, _, _ := newTestMachine(t)
	account := domain.AccountKey{LedgerName: "ledger", Account: "orders:1"}
	ledger := &commonpb.LedgerInfo{Name: "ledger"}
	digest, err := accounttype.LedgerTypeDigest(ledger)
	require.NoError(t, err)
	_, _, err = fsm.Registry.Ledgers.KeyStore().Put(domain.LedgerKey{Name: "ledger"}.Bytes(), ledger)
	require.NoError(t, err)
	initial := &raftcmdpb.AccountInventoryState{BirthIndex: 3, InventoryRevision: 5, VolumeCount: 1}
	_, _, err = fsm.Registry.AccountInventories.KeyStore().Put(account.Bytes(), initial)
	require.NoError(t, err)
	proposal := &raftcmdpb.Proposal{ExecutionPlan: &raftcmdpb.ExecutionPlan{
		AccountInventoryGeneration: fsm.Registry.Cache.CurrentGeneration(),
		AccountInventories:         []*raftcmdpb.AccountInventoryObservation{{AccountKey: account.Bytes(), Expected: initial.CloneVT(), AccountTypesDigest: digest}},
	}}
	require.NoError(t, fsm.checkStaleProposal(0, proposal))

	// A different key can replace the old key while the total remains one.
	_, _, err = fsm.Registry.AccountInventories.KeyStore().Put(account.Bytes(), &raftcmdpb.AccountInventoryState{BirthIndex: 3, InventoryRevision: 6, VolumeCount: 1})
	require.NoError(t, err)
	require.ErrorIs(t, fsm.checkStaleProposal(0, proposal), domain.ErrStaleProposal)

	// A purged and recreated account can reuse the same key and counts.
	_, _, err = fsm.Registry.AccountInventories.KeyStore().Put(account.Bytes(), &raftcmdpb.AccountInventoryState{BirthIndex: 7, InventoryRevision: 7, VolumeCount: 1})
	require.NoError(t, err)
	require.ErrorIs(t, fsm.checkStaleProposal(0, proposal), domain.ErrStaleProposal)
}

func TestAccountInventoryGuardRejectsTypeChangeAndTwoRotations(t *testing.T) {
	fsm, _, _ := newTestMachine(t)
	account := domain.AccountKey{LedgerName: "ledger", Account: "orders:1"}
	digest, err := accounttype.LedgerTypeDigest(&commonpb.LedgerInfo{Name: "ledger"})
	require.NoError(t, err)
	proposal := &raftcmdpb.Proposal{ExecutionPlan: &raftcmdpb.ExecutionPlan{
		AccountInventoryGeneration: fsm.Registry.Cache.CurrentGeneration(),
		AccountInventories:         []*raftcmdpb.AccountInventoryObservation{{AccountKey: account.Bytes(), AccountTypesDigest: digest}},
	}}
	_, _, err = fsm.Registry.Ledgers.KeyStore().Put(domain.LedgerKey{Name: "ledger"}.Bytes(), &commonpb.LedgerInfo{Name: "ledger", AccountTypes: map[string]*commonpb.AccountType{"ephemeral": {Name: "ephemeral", Pattern: "orders:{id}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL}}})
	require.NoError(t, err)
	require.ErrorIs(t, fsm.checkStaleProposal(0, proposal), domain.ErrStaleProposal)
	fsm.Registry.Cache.SetCurrentGeneration(proposal.GetExecutionPlan().GetAccountInventoryGeneration() + 2)
	require.ErrorIs(t, fsm.checkStaleProposal(0, proposal), domain.ErrStaleProposal)
}
