package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// EN-1522 gap B — the DeleteLedger boundary cascade runs through the gated
// Scope rather than a raw overlay delete, so it consumes the coverage
// admission declared and a missing declaration surfaces as *ErrCoverageMiss
// instead of a silent ungated delete (invariant #9).

// deleteLedgerOrder builds a DeleteLedger order for the given ledger name.
func deleteLedgerOrder(name string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: name,
				Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{
					DeleteLedger: &raftcmdpb.DeleteLedgerOrder{},
				},
			},
		},
	}
}

// TestDeleteLedger_BoundaryDeletedThroughGate covers the happy path: a
// DeleteLedger whose plan declares Boundary coverage removes the Boundary
// through the gated Scope, so after apply the boundary is gone.
func TestDeleteLedger_BoundaryDeletedThroughGate(t *testing.T) {
	t.Parallel()

	fsm, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "gated-delete"

	_, err := fsm.ApplyEntries(ctx, dataStore, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))))
	require.NoError(t, err)

	// The boundary exists after creation.
	_, _, err = fsm.Registry.Boundaries.GetKey(domain.LedgerKey{Name: ledgerName})
	require.NoError(t, err)

	result, err := fsm.ApplyEntries(ctx, dataStore, makeEntry(t, 2, makeProposal(2, deleteLedgerOrder(ledgerName))))
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error, "gated DeleteLedger with Boundary coverage must succeed")

	// The boundary was deleted through the gated Scope.
	_, _, err = fsm.Registry.Boundaries.GetKey(domain.LedgerKey{Name: ledgerName})
	require.ErrorIs(t, err, domain.ErrNotFound, "boundary must be gone after gated delete")
}

// TestDeleteLedger_MissingBoundaryCoverageSurfacesCoverageMiss covers the
// malformed-coverage path: a DeleteLedger whose plan declares the ledger but
// NOT the boundary must be rejected with *ErrCoverageMiss when the gated
// boundary delete runs — proving the cascade now goes through the gate
// instead of the old raw, ungated overlay delete.
func TestDeleteLedger_MissingBoundaryCoverageSurfacesCoverageMiss(t *testing.T) {
	t.Parallel()

	fsm, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "no-boundary-coverage"

	_, err := fsm.ApplyEntries(ctx, dataStore, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))))
	require.NoError(t, err)

	// Hand-built plan: declares ONLY the ledger key, deliberately omitting the
	// SubAttrBoundary declaration makeProposal would normally add.
	ledgerID, _ := attributes.MakeKey(domain.LedgerKey{Name: ledgerName}.Bytes())
	proposal := &raftcmdpb.Proposal{
		Id:     2,
		Orders: []*raftcmdpb.Order{deleteLedgerOrder(ledgerName)},
		Date:   &commonpb.Timestamp{Data: 1700000002},
		ExecutionPlan: &raftcmdpb.ExecutionPlan{
			Attributes: []*raftcmdpb.AttributeCoverage{
				declareTestPlan(ledgerID, dal.SubAttrLedger),
			},
		},
	}

	result, err := fsm.ApplyEntries(ctx, dataStore, makeEntry(t, 2, proposal))
	require.NoError(t, err, "a coverage miss is a business rejection, not a fatal FSM error")
	require.Len(t, result.Results, 1)
	require.Error(t, result.Results[0].Error)

	var miss *ErrCoverageMiss
	require.ErrorAs(t, result.Results[0].Error, &miss, "the gated boundary delete must surface *ErrCoverageMiss")
	require.Equal(t, "boundaries", miss.Attribute)

	// The ledger row is untouched (the proposal was rejected as a unit).
	info, _, err := fsm.Registry.Ledgers.GetKey(domain.LedgerKey{Name: ledgerName})
	require.NoError(t, err)
	require.Nil(t, info.GetDeletedAt(), "rejected DeleteLedger must not leave a soft-deleted tombstone")
}

// TestApplyThenDeleteLedger_SameProposal covers an Apply followed by a
// DeleteLedger in the SAME proposal: it stays valid and deterministic, and the
// boundary is gone at the end of the proposal.
func TestApplyThenDeleteLedger_SameProposal(t *testing.T) {
	t.Parallel()

	fsm, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	const ledgerName = "apply-then-delete"

	_, err := fsm.ApplyEntries(ctx, dataStore, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))))
	require.NoError(t, err)

	// One proposal: fund an account, then delete the ledger.
	result, err := fsm.ApplyEntries(ctx, dataStore, makeEntry(t, 2, makeProposal(2,
		createTransactionOrder(ledgerName, true, newPosting("world", "treasury", "EUR", 100)),
		deleteLedgerOrder(ledgerName),
	)))
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error, "Apply-then-DeleteLedger in one proposal must be valid")

	_, _, err = fsm.Registry.Boundaries.GetKey(domain.LedgerKey{Name: ledgerName})
	require.ErrorIs(t, err, domain.ErrNotFound, "boundary must be gone at end of the proposal")
}
