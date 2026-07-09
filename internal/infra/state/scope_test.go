package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestScope_TechnicalUpdate_CoverageMissShortCircuits pins that an
// undeclared ledger read in a technical-update handler propagates the
// *ErrCoverageMiss out of applyTechnicalUpdates, short-circuiting any
// later handlers in the loop. Coverage miss = malformed plan, surfaced
// as a business rejection — handlers that swallowed read errors before
// the refactor no longer mask an admission bug.
func TestScope_TechnicalUpdate_CoverageMissShortCircuits(t *testing.T) {
	t.Parallel()

	fsm, dataStore, _ := newTestMachine(t)

	const gen0Byte byte = 0

	// Seed the "ok" ledger in the cache + global store so the second
	// handler would have a real entry to read if it were reached.
	okKey := domain.LedgerKey{Name: "ok"}
	okInfo := &commonpb.LedgerInfo{Id: 1, Name: "ok"}

	seedBatch := dataStore.OpenWriteSession()
	_, _, err := fsm.Registry.Ledgers.PutWithCache(seedBatch, gen0Byte, okKey.Bytes(), okInfo)
	require.NoError(t, err)
	require.NoError(t, SaveLedger(seedBatch, okInfo))
	require.NoError(t, seedBatch.Commit())

	// ExecutionPlan declares ONLY "ok" — "missed" is intentionally absent so
	// the first handler hits a coverage miss.
	okID, _ := attributes.MakeKey(okKey.Bytes())
	executionPlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: fsm.Registry.Cache.BaseIndex.Gen0,
		Attributes: []*raftcmdpb.AttributeCoverage{
			declareTestPlan(okID, dal.SubAttrLedger),
		},
	}

	// Build the proposal with TWO MirrorSyncUpdate TechnicalUpdates.
	// Order matters: "missed" first so its short-circuit happens BEFORE
	// the second handler would queue a mirror-sync write. MirrorSync's
	// handler calls scope.GetLedger — so the coverage miss surfaces
	// from the same code path the historical IndexReady test exercised.
	proposal := &raftcmdpb.Proposal{
		Id:            1,
		Date:          &commonpb.Timestamp{Data: 1700000000},
		ExecutionPlan: executionPlan,
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{
			{Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "missed", Cursor: 1}}},
			{Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "ok", Cursor: 2}}},
		},
	}

	applyBatch := dataStore.OpenWriteSession()
	defer func() { _ = applyBatch.Cancel() }()

	fsm.writeSet.Reset(proposal.GetDate())
	buffer := fsm.writeSet
	scopeFactory := NewScopeFactory(buffer, executionPlan, fsm.logger, fsm.preloadMissCounter, proposal.GetId())

	err = fsm.applyTechnicalUpdates(scopeFactory, applyBatch, proposal.GetId(), proposal)
	require.Error(t, err, "coverage miss must propagate out of applyTechnicalUpdates")

	var miss *ErrCoverageMiss
	require.ErrorAs(t, err, &miss, "the propagated error must wrap *ErrCoverageMiss")
	// applyIndexReady reads LedgerInfo before the Index (to soft-skip on
	// DeletedAt), so the first undeclared key the gate rejects is "ledgers".
	require.Equal(t, "ledgers", miss.Attribute)

	// The second handler MUST NOT have been reached — the first
	// handler's coverage miss short-circuits the loop before "ok" is
	// touched. A successful second handler would have queued a
	// MirrorSyncWrite for "ok".
	require.Empty(t, buffer.pendingMirrorSyncs, "later handler must not have queued an update — short-circuit failed")
}

// TestScope_TechnicalUpdate_PerUpdateCoverageIsolation pins that the
// TechnicalUpdate envelope's coverage_bits isolates each tech-update from
// the others' declared keys. A MirrorSyncUpdate on ledger "A" with bits
// flagging ONLY "A" must NOT be able to read ledger "B" even when "B" is
// declared elsewhere in the proposal's ExecutionPlan. Symmetric to per-
// order coverage on Order.coverage_bits.
func TestScope_TechnicalUpdate_PerUpdateCoverageIsolation(t *testing.T) {
	t.Parallel()

	fsm, dataStore, _ := newTestMachine(t)

	const gen0Byte byte = 0

	// Seed both ledgers in the cache so the read path could resolve them
	// if coverage admitted the access.
	aKey := domain.LedgerKey{Name: "A"}
	bKey := domain.LedgerKey{Name: "B"}
	aInfo := &commonpb.LedgerInfo{Id: 1, Name: "A"}
	bInfo := &commonpb.LedgerInfo{Id: 2, Name: "B"}

	seedBatch := dataStore.OpenWriteSession()
	_, _, err := fsm.Registry.Ledgers.PutWithCache(seedBatch, gen0Byte, aKey.Bytes(), aInfo)
	require.NoError(t, err)
	_, _, err = fsm.Registry.Ledgers.PutWithCache(seedBatch, gen0Byte, bKey.Bytes(), bInfo)
	require.NoError(t, err)
	require.NoError(t, SaveLedger(seedBatch, aInfo))
	require.NoError(t, SaveLedger(seedBatch, bInfo))
	require.NoError(t, seedBatch.Commit())

	// Both ledgers are declared in the ExecutionPlan — under proposal-wide
	// scope this used to mean "any tech-update can read either". The
	// per-update envelope changes that: only the bits flagged on the TU
	// itself admit reads.
	aID, _ := attributes.MakeKey(aKey.Bytes())
	bID, _ := attributes.MakeKey(bKey.Bytes())
	executionPlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: fsm.Registry.Cache.BaseIndex.Gen0,
		Attributes: []*raftcmdpb.AttributeCoverage{
			declareTestPlan(aID, dal.SubAttrLedger), // bit 0
			declareTestPlan(bID, dal.SubAttrLedger), // bit 1
		},
	}

	// The TU targets ledger "B" via the MirrorSyncUpdate but its
	// coverage_bits flag ONLY position 0 (ledger "A"). The gate must
	// reject the read on "B" with *ErrCoverageMiss even though "B" is
	// declared elsewhere in the plan.
	proposal := &raftcmdpb.Proposal{
		Id:            1,
		Date:          &commonpb.Timestamp{Data: 1700000000},
		ExecutionPlan: executionPlan,
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			CoverageBits: []byte{0b00000001}, // only "A"
			Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{
				MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "B", Cursor: 42},
			},
		}},
	}

	applyBatch := dataStore.OpenWriteSession()
	defer func() { _ = applyBatch.Cancel() }()

	fsm.writeSet.Reset(proposal.GetDate())
	buffer := fsm.writeSet
	scopeFactory := NewScopeFactory(buffer, executionPlan, fsm.logger, fsm.preloadMissCounter, proposal.GetId())

	err = fsm.applyTechnicalUpdates(scopeFactory, applyBatch, proposal.GetId(), proposal)
	require.Error(t, err, "TU reading an undeclared (on this TU) ledger must surface a coverage miss")

	var miss *ErrCoverageMiss
	require.ErrorAs(t, err, &miss)
	require.Equal(t, "ledgers", miss.Attribute)
}

// TestScope_OrderRead_RequiresCoverageEvenForOverlayHit pins the overlay-
// gate invariant. A handler that writes ledger "K" into the in-batch
// overlay does not make "K" transparently readable to a later handler
// whose coverage_bits do not flag it — the gate sits ABOVE the overlay
// so an undeclared reader cannot bypass per-order isolation by hopping
// through DerivedKeyStore.
func TestScope_OrderRead_RequiresCoverageEvenForOverlayHit(t *testing.T) {
	t.Parallel()

	fsm, _, _ := newTestMachine(t)

	// Empty ExecutionPlan → no plans declared → all reads must miss.
	plan := &raftcmdpb.ExecutionPlan{}

	fsm.writeSet.Reset(&commonpb.Timestamp{Data: 1})
	buffer := fsm.writeSet
	scope, err := NewScopeFactory(buffer, plan, fsm.logger, fsm.preloadMissCounter, 42).NewScope(nil)
	require.NoError(t, err)

	// Simulate a prior handler write inside this batch.
	scope.Ledgers().Put(domain.LedgerKey{Name: "K"}, &commonpb.LedgerInfo{Id: 7, Name: "K"})

	// A later handler reads "K" through the same Scope. The overlay HAS it,
	// but coverage doesn't — the wrapper must gate before the engine reads
	// the value out of the overlay.
	_, err = scope.Ledgers().Get(domain.LedgerKey{Name: "K"})

	var miss *ErrCoverageMiss
	require.ErrorAs(t, err, &miss, "Scope.GetLedger must surface ErrCoverageMiss instead of the overlay value")
}

// TestScope_OrderDelete_RequiresCoverage pins the delete-gate invariant
// added in EN-1242: gatedAccessor.Delete must call CheckCoverage before
// delegating, so a handler that tries to delete a key its coverage_bits
// did not declare is rejected with ErrCoverageMiss rather than silently
// tombstoning an undeclared entry. Symmetric with Get's gate.
func TestScope_OrderDelete_RequiresCoverage(t *testing.T) {
	t.Parallel()

	fsm, _, _ := newTestMachine(t)

	// Empty ExecutionPlan → no plans declared → every gated operation must miss.
	plan := &raftcmdpb.ExecutionPlan{}

	fsm.writeSet.Reset(&commonpb.Timestamp{Data: 1})
	buffer := fsm.writeSet
	scope, err := NewScopeFactory(buffer, plan, fsm.logger, fsm.preloadMissCounter, 42).NewScope(nil)
	require.NoError(t, err)

	// A handler attempts to delete an undeclared metadata key. Put isn't
	// gated (the batch overlay isolates it), but Delete IS — the coverage
	// gate must reject before the deletion is queued.
	err = scope.AccountMetadata().Delete(domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "L", Account: "alice"},
		Key:        "label",
	})

	var miss *ErrCoverageMiss
	require.ErrorAs(t, err, &miss, "Scope.AccountMetadata().Delete on an undeclared key must surface ErrCoverageMiss")
	require.Equal(t, "account_metadata", miss.Attribute)
}
