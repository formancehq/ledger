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

// injectTagCollision forces the next KeyStore.Get for canonical to return a
// non-ErrNotFound *ErrCollisionDetected by storing an entry under the key's
// U128 id but with a deliberately mismatched tag. This is the state-package
// fault-injection seam for the invariant-#7 error-classification tests
// (EN-1522 gap C): a genuine storage/cache fault, distinct from an absence.
func injectTagCollision[K attributes.Key, V any](t *testing.T, ks *attributes.KeyStore[K, V], canonical []byte, data V) {
	t.Helper()

	id, tag := attributes.NewKeyHasher().MakeKey(canonical)
	ks.M.Put(id, attributes.Entry[V]{Tag: tag ^ 0xBEEF, Data: data})
}

// TestApplyMirrorSyncUpdate_KeysOffEnvelopeNotProjection covers EN-1522 gap
// A2: applyMirrorSyncUpdate must derive MirrorSyncWrite.LedgerName from the
// command envelope (update.GetLedgerName()), not from the loaded projection's
// mutable Name. A divergent LedgerInfo.name must not redirect the queued
// cursor/status write to another ledger's keys.
func TestApplyMirrorSyncUpdate_KeysOffEnvelopeNotProjection(t *testing.T) {
	t.Parallel()

	fsm, dataStore, _ := newTestMachine(t)

	const (
		gen0Byte byte = 0
		envelope      = "mirror-envelope"
	)

	// Seed the ledger UNDER the envelope key but with a DIVERGENT stored Name.
	envKey := domain.LedgerKey{Name: envelope}
	divergentInfo := &commonpb.LedgerInfo{Id: 1, Name: "divergent-projection"}

	seedBatch := dataStore.OpenWriteSession()
	_, _, err := fsm.Registry.Ledgers.PutWithCache(seedBatch, gen0Byte, envKey.Bytes(), divergentInfo)
	require.NoError(t, err)
	require.NoError(t, SaveLedger(seedBatch, divergentInfo))
	require.NoError(t, seedBatch.Commit())

	envID, _ := attributes.MakeKey(envKey.Bytes())
	executionPlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: fsm.Registry.Cache.BaseIndex.Gen0,
		Attributes: []*raftcmdpb.AttributeCoverage{
			declareTestPlan(envID, dal.SubAttrLedger),
		},
	}

	proposal := &raftcmdpb.Proposal{
		Id:            1,
		Date:          &commonpb.Timestamp{Data: 1700000000},
		ExecutionPlan: executionPlan,
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{
			{Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: envelope, Cursor: 7}}},
		},
	}
	// Stamp coverage bits on the TU so its scope admits the declared ledger
	// (mirrors what makeEntry/admission does for production proposals).
	sealProposal(proposal)

	applyBatch := dataStore.OpenWriteSession()
	defer func() { _ = applyBatch.Cancel() }()

	fsm.writeSet.Reset(proposal.GetDate())
	buffer := fsm.writeSet
	scopeFactory := NewScopeFactory(buffer, executionPlan, fsm.logger, fsm.preloadMissCounter, proposal.GetId())

	require.NoError(t, fsm.applyTechnicalUpdates(scopeFactory, applyBatch, proposal.GetId(), proposal))

	require.Len(t, buffer.pendingMirrorSyncs, 1)
	require.Equal(t, envelope, buffer.pendingMirrorSyncs[0].LedgerName,
		"mirror-sync write must key off the envelope, not the divergent projection name")
}

// TestDeleteLedger_BoundaryDeletedThroughGate covers EN-1522 gap B (happy
// path): a DeleteLedger whose plan declares Boundary coverage removes the
// Boundary through the gated Scope, so after apply the boundary is gone.
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

// TestDeleteLedger_MissingBoundaryCoverageSurfacesCoverageMiss covers EN-1522
// gap B (malformed coverage): a DeleteLedger whose plan declares the ledger
// but NOT the boundary must be rejected with *ErrCoverageMiss when the gated
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

// TestApplyThenDeleteLedger_SameProposal covers EN-1522: an Apply followed by
// a DeleteLedger in the SAME proposal stays valid and deterministic, and the
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
