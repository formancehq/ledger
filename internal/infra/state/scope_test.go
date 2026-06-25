package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
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

	// Seed the "ok" ledger in the cache + global store so saveLedgerWithCache
	// would have a fresh index entry to flip if the second handler runs.
	indexID := &commonpb.IndexID{
		Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE},
	}
	okKey := domain.LedgerKey{Name: "ok"}
	okInfo := &commonpb.LedgerInfo{
		Id:   1,
		Name: "ok",
	}

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
		Attributes: []*raftcmdpb.AttributePlan{
			declareTestPlan(okID, dal.SubAttrLedger),
		},
	}

	// Build the proposal with TWO IndexReady TechnicalUpdates. Order
	// matters: "missed" first so its short-circuit happens BEFORE the
	// second handler would mutate. Both TUs use bits=nil — "missed" is not
	// in the plan at all so any bitset would miss; the second TU is never
	// reached so its bitset is moot.
	proposal := &raftcmdpb.Proposal{
		Id:            1,
		Date:          &commonpb.Timestamp{Data: 1700000000},
		ExecutionPlan: executionPlan,
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{
			{Kind: &raftcmdpb.TechnicalUpdate_IndexReady{IndexReady: &raftcmdpb.IndexReadyUpdate{Ledger: "missed", Id: indexID}}},
			{Kind: &raftcmdpb.TechnicalUpdate_IndexReady{IndexReady: &raftcmdpb.IndexReadyUpdate{Ledger: "ok", Id: indexID}}},
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

	// The second handler MUST NOT have been reached — the first handler's
	// coverage miss short-circuits the loop before "ok" is touched. The
	// seeded entry's index is still BUILDING; a successful second handler
	// would have queued a cloned LedgerInfo with status READY in the
	// overlay's dirty set.
	dirty := buffer.Derived.Ledgers.DirtyValues()
	require.Empty(t, dirty, "later handler must not have queued an update — short-circuit failed")
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
		Attributes: []*raftcmdpb.AttributePlan{
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
	scope.PutLedger("K", &commonpb.LedgerInfo{Id: 7, Name: "K"})

	// A later handler reads "K" through the same Scope. The overlay HAS it,
	// but coverage doesn't — the wrapper must gate before the engine reads
	// the value out of the overlay.
	_, err = scope.GetLedger("K")

	var miss *ErrCoverageMiss
	require.ErrorAs(t, err, &miss, "Scope.GetLedger must surface ErrCoverageMiss instead of the overlay value")
}

// TestApplyIndexReady_SkipsDeletedLedger anchors the deleted-ledger guard:
// processDeleteLedger leaves the Index cache entry live (we removed the
// per-batch RangeIndexes cascade), and deleteLedgerData range-deletes
// Pebble out-of-band at chapter purge. An IndexReady racing through this
// window — possibly arriving AFTER deleteLedgerData has run — would
// otherwise resurrect an orphan Index row with no future cleanup
// (PendingLedgerCleanup already consumed). The fix soft-skips on
// LedgerInfo.DeletedAt != nil before touching the registry.
func TestApplyIndexReady_SkipsDeletedLedger(t *testing.T) {
	t.Parallel()

	fsm, dataStore, _ := newTestMachine(t)

	const gen0Byte byte = 0
	const ledgerName = "doomed"

	indexID := &commonpb.IndexID{
		Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE},
	}

	ledgerKey := domain.LedgerKey{Name: ledgerName}
	deletedAt := &commonpb.Timestamp{Data: 1699999999}
	ledgerInfo := &commonpb.LedgerInfo{
		Id:        1,
		Name:      ledgerName,
		DeletedAt: deletedAt,
	}

	indexCanonical := canonicalIndexID(t, indexID)
	indexKey := domain.IndexKey{LedgerName: ledgerName, Canonical: indexCanonical}
	indexValue := &commonpb.Index{
		Id:          indexID,
		Ledger:      ledgerName,
		BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
	}

	seedBatch := dataStore.OpenWriteSession()
	_, _, err := fsm.Registry.Ledgers.PutWithCache(seedBatch, gen0Byte, ledgerKey.Bytes(), ledgerInfo)
	require.NoError(t, err)
	require.NoError(t, SaveLedger(seedBatch, ledgerInfo))
	_, _, err = fsm.Registry.Indexes.PutWithCache(seedBatch, gen0Byte, indexKey.Bytes(), indexValue)
	require.NoError(t, err)
	require.NoError(t, seedBatch.Commit())

	ledgerU128, _ := attributes.MakeKey(ledgerKey.Bytes())
	indexU128, _ := attributes.MakeKey(indexKey.Bytes())
	executionPlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: fsm.Registry.Cache.BaseIndex.Gen0,
		Attributes: []*raftcmdpb.AttributePlan{
			declareTestPlan(ledgerU128, dal.SubAttrLedger),
			declareTestPlan(indexU128, dal.SubAttrIndex),
		},
	}

	proposal := &raftcmdpb.Proposal{
		Id:            1,
		Date:          &commonpb.Timestamp{Data: 1700000000},
		ExecutionPlan: executionPlan,
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			CoverageBits: []byte{0b00000011}, // both ledger + index plans
			Kind:         &raftcmdpb.TechnicalUpdate_IndexReady{IndexReady: &raftcmdpb.IndexReadyUpdate{Ledger: ledgerName, Id: indexID}},
		}},
	}

	applyBatch := dataStore.OpenWriteSession()
	defer func() { _ = applyBatch.Cancel() }()

	fsm.writeSet.Reset(proposal.GetDate())
	buffer := fsm.writeSet
	scopeFactory := NewScopeFactory(buffer, executionPlan, fsm.logger, fsm.preloadMissCounter, proposal.GetId())

	require.NoError(t, fsm.applyTechnicalUpdates(scopeFactory, applyBatch, proposal.GetId(), proposal))

	// applyIndexReady must NOT have written a Mutate'd index back into the
	// overlay — a non-empty DirtyValues here would mean the deleted-ledger
	// guard failed and an orphan READY entry was queued for flush.
	require.Empty(t, buffer.Derived.Indexes.DirtyValues(),
		"deleted ledger must not flow an Index update into the overlay")
}

// TestApplyIndexReady_FlipsLiveLedger is the positive counterpart: when
// the ledger is alive (no DeletedAt), the IndexReady TU must read the
// BUILDING entry and queue a READY clone in the WriteSet overlay.
func TestApplyIndexReady_FlipsLiveLedger(t *testing.T) {
	t.Parallel()

	fsm, dataStore, _ := newTestMachine(t)

	const gen0Byte byte = 0
	const ledgerName = "live"

	indexID := &commonpb.IndexID{
		Kind: &commonpb.IndexID_LogBuiltin{LogBuiltin: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE},
	}

	ledgerKey := domain.LedgerKey{Name: ledgerName}
	ledgerInfo := &commonpb.LedgerInfo{Id: 2, Name: ledgerName}

	indexCanonical := canonicalIndexID(t, indexID)
	indexKey := domain.IndexKey{LedgerName: ledgerName, Canonical: indexCanonical}
	indexValue := &commonpb.Index{
		Id:          indexID,
		Ledger:      ledgerName,
		BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
	}

	seedBatch := dataStore.OpenWriteSession()
	_, _, err := fsm.Registry.Ledgers.PutWithCache(seedBatch, gen0Byte, ledgerKey.Bytes(), ledgerInfo)
	require.NoError(t, err)
	require.NoError(t, SaveLedger(seedBatch, ledgerInfo))
	_, _, err = fsm.Registry.Indexes.PutWithCache(seedBatch, gen0Byte, indexKey.Bytes(), indexValue)
	require.NoError(t, err)
	require.NoError(t, seedBatch.Commit())

	ledgerU128, _ := attributes.MakeKey(ledgerKey.Bytes())
	indexU128, _ := attributes.MakeKey(indexKey.Bytes())
	executionPlan := &raftcmdpb.ExecutionPlan{
		LastPersistedIndex: fsm.Registry.Cache.BaseIndex.Gen0,
		Attributes: []*raftcmdpb.AttributePlan{
			declareTestPlan(ledgerU128, dal.SubAttrLedger),
			declareTestPlan(indexU128, dal.SubAttrIndex),
		},
	}

	proposalDate := &commonpb.Timestamp{Data: 1700000000}
	proposal := &raftcmdpb.Proposal{
		Id:            2,
		Date:          proposalDate,
		ExecutionPlan: executionPlan,
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			CoverageBits: []byte{0b00000011},
			Kind:         &raftcmdpb.TechnicalUpdate_IndexReady{IndexReady: &raftcmdpb.IndexReadyUpdate{Ledger: ledgerName, Id: indexID}},
		}},
	}

	applyBatch := dataStore.OpenWriteSession()
	defer func() { _ = applyBatch.Cancel() }()

	fsm.writeSet.Reset(proposal.GetDate())
	buffer := fsm.writeSet
	scopeFactory := NewScopeFactory(buffer, executionPlan, fsm.logger, fsm.preloadMissCounter, proposal.GetId())

	require.NoError(t, fsm.applyTechnicalUpdates(scopeFactory, applyBatch, proposal.GetId(), proposal))

	dirty := buffer.Derived.Indexes.DirtyValues()
	require.Len(t, dirty, 1, "live ledger must produce an IndexReady write in the overlay")
	updated := dirty[indexKey]
	require.NotNil(t, updated, "the dirty entry must be keyed by the IndexReady target")
	require.Equal(t, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY, updated.GetBuildStatus())
	require.Equal(t, proposalDate.GetData(), updated.GetLastBuiltAt().GetData())
}

func canonicalIndexID(t *testing.T, id *commonpb.IndexID) string {
	t.Helper()

	return indexes.Canonical(id)
}
