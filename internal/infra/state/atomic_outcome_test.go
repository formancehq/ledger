package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// readBusinessProjections opens an independent persisted read after apply. It
// deliberately excludes audit failures and technical Raft progress, which may
// advance when a proposal rejects or replays.
func readBusinessProjections(t *testing.T, store *dal.Store, attrs *attributes.Attributes) map[string]any {
	t.Helper()
	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()

	volumes, err := attrs.Volume.ComputeAllForPrefix(handle, nil)
	require.NoError(t, err)
	transactions, err := attrs.Transaction.ComputeAllForPrefix(handle, nil)
	require.NoError(t, err)
	references, err := attrs.References.ComputeAllForPrefix(handle, nil)
	require.NoError(t, err)
	boundaries, err := attrs.Boundary.ComputeAllForPrefix(handle, nil)
	require.NoError(t, err)
	lastLog, err := query.ReadLastSequence(handle)
	require.NoError(t, err)

	return map[string]any{
		"volumes": volumes, "transactions": transactions, "references": references,
		"boundaries": boundaries, "lastLog": lastLog,
	}
}

func TestAtomicProposal_StagedTransferDoesNotSurviveLaterRejection(t *testing.T) {
	t.Parallel()
	machine, store, attrs := newTestMachine(t)
	ctx := t.Context()
	const ledger = "atomic-transfer"

	result, err := machine.ApplyEntries(ctx, store, makeEntry(t, 1, makeProposal(1,
		createLedgerOrder(ledger),
		createTransactionOrder(ledger, true, newPosting("world", "alice", "EUR", 100)),
	)))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	before := readBusinessProjections(t, store, attrs)
	boundary, err := attrs.Boundary.Get(store, domain.LedgerKey{Name: ledger}.Bytes())
	require.NoError(t, err)
	nextID := boundary.GetNextTransactionId()

	transfer := createTransactionOrder(ledger, false, newPosting("alice", "bob", "EUR", 40))
	transfer.GetLedgerScoped().GetApply().GetCreateTransaction().Reference = "rolled-back-reference"
	referenceKey := domain.TransactionReferenceKey{LedgerName: ledger, Reference: "rolled-back-reference"}
	referenceID, _ := attributes.MakeKey(referenceKey.Bytes())
	proposal := func(id uint64, orders ...*raftcmdpb.Order) *raftcmdpb.Proposal {
		p := makeProposal(id, orders...)
		p.ExecutionPlan.Attributes = append(p.ExecutionPlan.Attributes, declareTestPlan(referenceID, dal.SubAttrReference))

		return p
	}

	result, err = machine.ApplyEntries(ctx, store, makeEntry(t, 2,
		proposal(2, transfer, revertTransactionOrder(ledger, 9999))))
	require.NoError(t, err)
	var missing *domain.ErrTransactionNotFound
	require.ErrorAs(t, result.Results[0].Error, &missing)
	require.Equal(t, uint64(9999), missing.TransactionID, "the later order must be the rejection trigger")
	require.Empty(t, result.Results[0].Logs)
	require.Equal(t, before, readBusinessProjections(t, store, attrs),
		"rejection must preserve gross volumes, transaction IDs, references, and business logs")

	// Reusing the same reference and ID independently also checks that the
	// rejected overlay did not leak into the live FSM cache.
	result, err = machine.ApplyEntries(ctx, store, makeEntry(t, 3, proposal(3, transfer)))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	transaction := result.Results[0].Logs[0].GetCreatedLog().GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction()
	require.Equal(t, nextID, transaction.GetId())
	reference, err := attrs.References.Get(store, referenceKey.Bytes())
	require.NoError(t, err)
	require.NotNil(t, reference)
	require.Equal(t, nextID, reference.GetTransactionId())
	for account, expected := range map[string][2]int64{"world": {0, 100}, "alice": {100, 40}, "bob": {40, 0}} {
		volume, err := attrs.Volume.Get(store, domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: ledger, Account: account}, Asset: "EUR",
		}.Bytes())
		require.NoError(t, err)
		require.Equal(t, expected, [2]int64{effectiveVolumeInput(volume), effectiveVolumeOutput(volume)}, account)
	}
}

func TestAtomicProposal_StagedRevertDoesNotSurviveLaterRejection(t *testing.T) {
	t.Parallel()
	machine, store, attrs := newTestMachine(t)
	ctx := t.Context()
	const ledger = "atomic-revert"
	funding := createTransactionOrder(ledger, true, newPosting("world", "alice", "EUR", 100))
	result, err := machine.ApplyEntries(ctx, store, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledger), funding)))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	originalTx := result.Results[0].Logs[1].GetCreatedLog().GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction()
	originalID := originalTx.GetId()
	before := readBusinessProjections(t, store, attrs)
	originalKey := domain.TransactionKey{LedgerName: ledger, ID: originalID}
	boundary, err := attrs.Boundary.Get(store, domain.LedgerKey{Name: ledger}.Bytes())
	require.NoError(t, err)
	nextID := boundary.GetNextTransactionId()
	proposal := func(id uint64, orders ...*raftcmdpb.Order) *raftcmdpb.Proposal {
		p := makeProposal(id, orders...)
		// Admission declares the original postings' volumes for a revert;
		// the shared test builder only derives them from create orders.
		p.ExecutionPlan.Attributes = append(p.ExecutionPlan.Attributes, buildVolumePreloads([]*raftcmdpb.Order{funding})...)

		return p
	}

	result, err = machine.ApplyEntries(ctx, store, makeEntry(t, 2, proposal(2,
		revertObservedTransactionOrder(ledger, originalID, originalTx.GetPostings()), revertTransactionOrder(ledger, 9999))))
	require.NoError(t, err)
	var missing *domain.ErrTransactionNotFound
	require.ErrorAs(t, result.Results[0].Error, &missing)
	require.Equal(t, uint64(9999), missing.TransactionID, "the revert must stage before the later rejection")
	require.Empty(t, result.Results[0].Logs)
	require.Equal(t, before, readBusinessProjections(t, store, attrs),
		"rejection must leave the original unreverted with no compensating transaction or volume changes")
	require.False(t, machine.Registry.GetReverted(originalKey), "reverted bitset must also roll back")

	result, err = machine.ApplyEntries(ctx, store, makeEntry(t, 3, proposal(3, revertObservedTransactionOrder(ledger, originalID, originalTx.GetPostings()))))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error, "an independent revert must still succeed")
	revert := result.Results[0].Logs[0].GetCreatedLog().GetPayload().GetApply().GetLog().GetData().GetRevertedTransaction()
	require.Equal(t, originalID, revert.GetRevertedTransactionId())
	require.Equal(t, nextID, revert.GetRevertTransaction().GetId())
	original, err := attrs.Transaction.Get(store, originalKey.Bytes())
	require.NoError(t, err)
	require.NotNil(t, original.GetRevertedAt())
	require.Equal(t, nextID, original.GetRevertedByTransaction())
	require.True(t, machine.Registry.GetReverted(originalKey))
	for _, account := range []string{"world", "alice"} {
		volume, err := attrs.Volume.Get(store, domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: ledger, Account: account}, Asset: "EUR",
		}.Bytes())
		require.NoError(t, err)
		require.Equal(t, int64(100), effectiveVolumeInput(volume), account)
		require.Equal(t, int64(100), effectiveVolumeOutput(volume), account)
	}
}
