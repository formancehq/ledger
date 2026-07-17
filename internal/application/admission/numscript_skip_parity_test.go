package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// scriptOrderWithReference builds an inline-script CreateTransaction order that
// carries a transaction reference and (optionally) opts into the
// TRANSACTION_REFERENCE_CONFLICT skippable reason.
func scriptOrderWithReference(ledger, plain, reference string, skippable bool) *raftcmdpb.Order {
	order := scriptOrder(ledger, plain)
	ct := createTxOf(order)
	ct.Reference = reference

	if skippable {
		order.GetLedgerScoped().GetApply().SkippableReasons = []commonpb.ErrorReason{
			commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
		}
	}

	return order
}

// postingsOrderWithReference builds a postings-only (no script) CreateTransaction
// order that carries a transaction reference and (optionally) opts into the
// TRANSACTION_REFERENCE_CONFLICT skippable reason. Used to guard that the
// postings-only branch records its reference into the intra-batch overlay just
// like the scripted branch does.
func postingsOrderWithReference(ledger, source, destination, asset string, amount uint64, reference string, skippable bool) *raftcmdpb.Order {
	order := applyOrder(ledger, &raftcmdpb.LedgerApplyOrder{
		Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
			CreateTransaction: &raftcmdpb.CreateTransactionOrder{
				Reference: reference,
				Postings: []*commonpb.Posting{
					{
						Source:      source,
						Destination: destination,
						Amount:      commonpb.NewUint256FromUint64(amount),
						Asset:       asset,
					},
				},
			},
		},
	})

	if skippable {
		order.GetLedgerScoped().GetApply().SkippableReasons = []commonpb.ErrorReason{
			commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
		}
	}

	return order
}

// revertOrderSkippable builds a revert order opting into the
// TRANSACTION_ALREADY_REVERTED skippable reason.
func revertOrderSkippable(ledger string, txID uint64, original ...*commonpb.Posting) *raftcmdpb.Order {
	order := revertOrder(ledger, txID, original...)
	order.GetLedgerScoped().GetApply().SkippableReasons = []commonpb.ErrorReason{
		commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED,
	}

	return order
}

// writeReference seeds a transaction reference in Pebble so a create carrying it
// resolves to a conflict.
func writeReference(t *testing.T, admission *Admission, ledger, reference string, txID uint64) {
	t.Helper()

	key := domain.TransactionReferenceKey{LedgerName: ledger, Reference: reference}
	batch := admission.store.OpenWriteSession()
	_, err := admission.attrs.References.Set(batch, key.Bytes(), &commonpb.TransactionReferenceValue{TransactionId: txID})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

// writeReverted seeds the reversion bitset for a ledger so the given tx reads as
// already reverted. The key layout matches query.ReadReversionBitset:
// [ZonePerLedger][SubPLReversions][ledgerName padded 64B][wordIndex BE] → word LE.
func writeReverted(t *testing.T, admission *Admission, ledger string, txID uint64) {
	t.Helper()

	bs := &bitset.Bitset{}
	wordIndex := bs.Set(txID)

	batch := admission.store.OpenWriteSession()
	batch.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLReversions).
		PutLedgerNameFixed(ledger).
		PutUint64(wordIndex)
	require.NoError(t, batch.SetBytes(batch.KeyBuilder.Consume(), bitset.MarshalWord(bs.Word(wordIndex))))
	require.NoError(t, batch.Commit())
}

// dependentBalanceScript reads balance(@acct) and sends it onward — a script
// whose resolution hash depends on the account's balance, so a phantom
// predecessor balance effect would poison it.
func dependentBalanceScript(acct string) string {
	return `
vars {
  monetary $all = balance(@` + acct + `, USD/2)
}
send $all (source = @` + acct + ` destination = @dest)
`
}

// TestResolveScripts_SkippedReferenceConflictNotFolded pins the flemzord High
// finding for CreateTransaction: a predecessor create that opts into
// TRANSACTION_REFERENCE_CONFLICT and carries a reference already present in
// storage is dropped by the FSM (matchOrderSkip), so its balance effect must NOT
// be folded into a later Numscript's resolution. Before the fix, admission folded
// the phantom deposit and the dependent order's hash diverged from the FSM's,
// wedging the batch on STALE_INPUTS_RESOLUTION forever.
func TestResolveScripts_SkippedReferenceConflictNotFolded(t *testing.T) {
	t.Parallel()

	// Order 0 deposits 100 into skip:src but carries a duplicate reference and
	// opts into skip — the FSM will drop it. Order 1 reads balance(@skip:src).
	batch := []*raftcmdpb.Order{
		scriptOrderWithReference(testLedgerName,
			`send [USD/2 100] (source = @world destination = @skip:src)`, "dup-ref", true),
		scriptOrder(testLedgerName, dependentBalanceScript("skip:src")),
	}

	storeBatch := createTestStore(t)
	admissionBatch, _ := createTestAdmission(t, storeBatch)
	// The reference already exists → order 0 will be skipped by the FSM.
	writeReference(t, admissionBatch, testLedgerName, "dup-ref", 42)
	batchHash := resolveHashFor(t, admissionBatch, batch, 1)
	require.NotEmpty(t, batchHash, "order 1 reads a balance, so it must carry a resolution hash")

	// Reference: order 1 resolved standalone against an EMPTY store — the state
	// the FSM sees once order 0 has been dropped (skip:src still at 0).
	storeRef := createTestStore(t)
	admissionRef, _ := createTestAdmission(t, storeRef)
	refHash := resolveHashFor(t, admissionRef, []*raftcmdpb.Order{
		scriptOrder(testLedgerName, dependentBalanceScript("skip:src")),
	}, 0)

	require.Equal(t, refHash, batchHash,
		"a skipped predecessor must contribute no balance effect — the dependent hash must match the empty-store resolution")

	// Sanity: had the predecessor been folded (the pre-fix bug), the resolved
	// balance would be 100 and the hash would differ. Prove that by folding the
	// same deposit via a NON-skippable predecessor and confirming a different hash.
	storeFolded := createTestStore(t)
	admissionFolded, _ := createTestAdmission(t, storeFolded)
	foldedHash := resolveHashFor(t, admissionFolded, []*raftcmdpb.Order{
		scriptOrder(testLedgerName, `send [USD/2 100] (source = @world destination = @skip:src)`),
		scriptOrder(testLedgerName, dependentBalanceScript("skip:src")),
	}, 1)
	require.NotEqual(t, foldedHash, batchHash,
		"folding the deposit (100) yields a different hash — confirming the skip actually suppressed the effect")
}

// TestResolveScripts_NonConflictingReferenceStillFolded pins the OTHER half of
// the skip-parity contract: a predecessor create that opts into skip but whose
// reference is FRESH (no conflict) is applied by the FSM, so its balance effect
// MUST be folded into a later Numscript's resolution. Skip opt-in alone must not
// suppress a successful order's effects.
func TestResolveScripts_NonConflictingReferenceStillFolded(t *testing.T) {
	t.Parallel()

	// Order 0 deposits 100 into fresh:src with a fresh reference and skip opt-in;
	// no conflict exists, so the FSM applies it. Order 1 reads balance(@fresh:src).
	batch := []*raftcmdpb.Order{
		scriptOrderWithReference(testLedgerName,
			`send [USD/2 100] (source = @world destination = @fresh:src)`, "fresh-ref", true),
		scriptOrder(testLedgerName, dependentBalanceScript("fresh:src")),
	}

	storeBatch := createTestStore(t)
	admissionBatch, _ := createTestAdmission(t, storeBatch)
	batchHash := resolveHashFor(t, admissionBatch, batch, 1)
	require.NotEmpty(t, batchHash)

	// Reference: order 1 resolved standalone against a store where fresh:src
	// already holds the post-deposit 100 — the state the FSM sees once order 0
	// (which is NOT skipped) has applied.
	storeRef := createTestStore(t)
	admissionRef, _ := createTestAdmission(t, storeRef)
	writeVolume(t, admissionRef, testLedgerName, "fresh:src", "USD/2", 100, 0)
	refHash := resolveHashFor(t, admissionRef, []*raftcmdpb.Order{
		scriptOrder(testLedgerName, dependentBalanceScript("fresh:src")),
	}, 0)

	require.Equal(t, refHash, batchHash,
		"a successful predecessor (fresh reference) must still fold its balance effect, skip opt-in notwithstanding")
}

// TestResolveScripts_SkippedAlreadyRevertedNotFolded pins the flemzord High
// finding for RevertTransaction: a predecessor revert that opts into
// TRANSACTION_ALREADY_REVERTED and targets an already-reverted tx is dropped by
// the FSM, so its reversed-posting balance effect must NOT be folded into a later
// Numscript's resolution.
func TestResolveScripts_SkippedAlreadyRevertedNotFolded(t *testing.T) {
	t.Parallel()

	// Pre-batch: rev:acct holds 250. Order 0 reverts tx 1 (original world->rev:acct
	// 100) with skip opt-in, but tx 1 is ALREADY reverted → the FSM drops it, so
	// rev:acct stays at 250. Order 1 reads balance(@rev:acct).
	batch := []*raftcmdpb.Order{
		revertOrderSkippable(testLedgerName, 1, posting("world", "rev:acct", "USD/2", 100)),
		scriptOrder(testLedgerName, dependentBalanceScript("rev:acct")),
	}

	storeBatch := createTestStore(t)
	admissionBatch, _ := createTestAdmission(t, storeBatch)
	writeVolume(t, admissionBatch, testLedgerName, "rev:acct", "USD/2", 250, 0)
	writeReverted(t, admissionBatch, testLedgerName, 1) // tx 1 already reverted
	batchHash := resolveHashFor(t, admissionBatch, batch, 1)
	require.NotEmpty(t, batchHash)

	// Reference: order 1 resolved standalone against rev:acct = 250 — the state
	// the FSM sees once order 0 has been dropped (no reversed posting applied).
	storeRef := createTestStore(t)
	admissionRef, _ := createTestAdmission(t, storeRef)
	writeVolume(t, admissionRef, testLedgerName, "rev:acct", "USD/2", 250, 0)
	refHash := resolveHashFor(t, admissionRef, []*raftcmdpb.Order{
		scriptOrder(testLedgerName, dependentBalanceScript("rev:acct")),
	}, 0)

	require.Equal(t, refHash, batchHash,
		"a skipped (already-reverted) predecessor must contribute no balance effect — the dependent hash must match the un-reverted 250 resolution")

	// Sanity: a NON-already-reverted revert of the same shape folds the reversed
	// posting (rev:acct → 150), yielding a different hash.
	storeApplied := createTestStore(t)
	admissionApplied, _ := createTestAdmission(t, storeApplied)
	writeVolume(t, admissionApplied, testLedgerName, "rev:acct", "USD/2", 250, 0)
	appliedHash := resolveHashFor(t, admissionApplied, []*raftcmdpb.Order{
		revertOrderSkippable(testLedgerName, 1, posting("world", "rev:acct", "USD/2", 100)),
		scriptOrder(testLedgerName, dependentBalanceScript("rev:acct")),
	}, 1)
	require.NotEqual(t, appliedHash, batchHash,
		"an applied revert folds the reversed posting (250→150) — confirming the skip actually suppressed the effect")
}

// TestResolveScripts_IntraBatchDuplicateReferenceSkipped pins intra-batch skip
// parity: two creates in the same batch carry the SAME fresh reference. The FSM
// applies the first (registers the reference) and skips the second
// (TRANSACTION_REFERENCE_CONFLICT against the first's write). A third order's
// balance() must see only the first create's deposit, not the second's.
func TestResolveScripts_IntraBatchDuplicateReferenceSkipped(t *testing.T) {
	t.Parallel()

	// Order 0: deposit 100 into ib:src with reference "r". Order 1: deposit 100
	// into ib:src with the SAME reference "r" + skip opt-in → skipped by the FSM.
	// Order 2: reads balance(@ib:src) → must resolve to 100 (only order 0), not 200.
	batch := []*raftcmdpb.Order{
		scriptOrderWithReference(testLedgerName,
			`send [USD/2 100] (source = @world destination = @ib:src)`, "r", true),
		scriptOrderWithReference(testLedgerName,
			`send [USD/2 100] (source = @world destination = @ib:src)`, "r", true),
		scriptOrder(testLedgerName, dependentBalanceScript("ib:src")),
	}

	storeBatch := createTestStore(t)
	admissionBatch, _ := createTestAdmission(t, storeBatch)
	batchHash := resolveHashFor(t, admissionBatch, batch, 2)
	require.NotEmpty(t, batchHash)

	// Reference: order 2 resolved against ib:src = 100 (only the first deposit).
	storeRef := createTestStore(t)
	admissionRef, _ := createTestAdmission(t, storeRef)
	writeVolume(t, admissionRef, testLedgerName, "ib:src", "USD/2", 100, 0)
	refHash := resolveHashFor(t, admissionRef, []*raftcmdpb.Order{
		scriptOrder(testLedgerName, dependentBalanceScript("ib:src")),
	}, 0)

	require.Equal(t, refHash, batchHash,
		"the second create (duplicate reference) is skipped intra-batch — only the first deposit (100) may fold")

	// Sanity: had both deposits folded (200), the hash would differ.
	storeBoth := createTestStore(t)
	admissionBoth, _ := createTestAdmission(t, storeBoth)
	writeVolume(t, admissionBoth, testLedgerName, "ib:src", "USD/2", 200, 0)
	bothHash := resolveHashFor(t, admissionBoth, []*raftcmdpb.Order{
		scriptOrder(testLedgerName, dependentBalanceScript("ib:src")),
	}, 0)
	require.NotEqual(t, bothHash, batchHash,
		"folding both deposits (200) yields a different hash — confirming the second create was suppressed")
}

// TestResolveScripts_PostingsOnlyReferenceRecordedForIntraBatchSkip pins the
// NumaryBot blocker: a postings-only (non-scripted) create carrying a reference
// must record that reference into the intra-batch overlay before continuing, so a
// later same-batch create with the SAME reference is predicted to skip
// (TRANSACTION_REFERENCE_CONFLICT) exactly as the FSM will drop it. Before the
// fix the postings-only branch continued before the reference-recording block, so
// the duplicate was NOT predicted to skip, its effects were folded as phantom
// state, and the dependent order's inputs-resolution hash diverged from the FSM —
// wedging the batch on STALE_INPUTS_RESOLUTION forever.
func TestResolveScripts_PostingsOnlyReferenceRecordedForIntraBatchSkip(t *testing.T) {
	t.Parallel()

	// Order 0: POSTINGS-ONLY deposit 100 into ib:src with reference "r".
	// Order 1: postings-only deposit 100 into ib:src with the SAME reference "r"
	// + skip opt-in → must be predicted to skip (dropped by the FSM).
	// Order 2: reads balance(@ib:src) → must resolve to 100 (only order 0), not 200.
	batch := []*raftcmdpb.Order{
		postingsOrderWithReference(testLedgerName, "world", "ib:src", "USD/2", 100, "r", true),
		postingsOrderWithReference(testLedgerName, "world", "ib:src", "USD/2", 100, "r", true),
		scriptOrder(testLedgerName, dependentBalanceScript("ib:src")),
	}

	storeBatch := createTestStore(t)
	admissionBatch, _ := createTestAdmission(t, storeBatch)
	batchHash := resolveHashFor(t, admissionBatch, batch, 2)
	require.NotEmpty(t, batchHash)

	// Reference: order 2 resolved against ib:src = 100 (only the first deposit).
	storeRef := createTestStore(t)
	admissionRef, _ := createTestAdmission(t, storeRef)
	writeVolume(t, admissionRef, testLedgerName, "ib:src", "USD/2", 100, 0)
	refHash := resolveHashFor(t, admissionRef, []*raftcmdpb.Order{
		scriptOrder(testLedgerName, dependentBalanceScript("ib:src")),
	}, 0)

	require.Equal(t, refHash, batchHash,
		"the postings-only duplicate-reference create is skipped intra-batch — only the first deposit (100) may fold")

	// Sanity: had both deposits folded (200), the hash would differ — this is the
	// pre-fix behaviour where the postings-only reference was never recorded.
	storeBoth := createTestStore(t)
	admissionBoth, _ := createTestAdmission(t, storeBoth)
	writeVolume(t, admissionBoth, testLedgerName, "ib:src", "USD/2", 200, 0)
	bothHash := resolveHashFor(t, admissionBoth, []*raftcmdpb.Order{
		scriptOrder(testLedgerName, dependentBalanceScript("ib:src")),
	}, 0)
	require.NotEqual(t, bothHash, batchHash,
		"folding both deposits (200) yields a different hash — confirming the second create was suppressed")
}

// TestPredictOrderSkip_NoOptInNeverSkips guards the matchOrderSkip parity: an
// order that does NOT opt into skippable_reasons is never predicted to skip even
// when its predicate would match (reference already present), mirroring
// matchOrderSkip's len(allowed)==0 early return.
func TestPredictOrderSkip_NoOptInNeverSkips(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)
	writeReference(t, admission, testLedgerName, "present", 7)

	// Reference present but NO skip opt-in → not predicted to skip.
	order := scriptOrderWithReference(testLedgerName,
		`send [USD/2 1] (source = @world destination = @x)`, "present", false)
	skip, err := admission.predictOrderSkip(order, testLedgerName, newBatchEffects())
	require.NoError(t, err)
	require.False(t, skip, "without skip opt-in the order must never be predicted to skip")

	// Same order WITH opt-in → predicted to skip.
	orderOptIn := scriptOrderWithReference(testLedgerName,
		`send [USD/2 1] (source = @world destination = @x)`, "present", true)
	skip, err = admission.predictOrderSkip(orderOptIn, testLedgerName, newBatchEffects())
	require.NoError(t, err)
	require.True(t, skip, "with opt-in and a present reference the order must be predicted to skip")
}
