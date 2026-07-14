package admission

import (
	"slices"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// predictOrderSkip mirrors the FSM's matchOrderSkip decision at admission fold
// time so admission's batch overlay contains exactly the effects the FSM will
// actually apply. ProcessOrders drops a skip-tolerant order's overlay when the
// sub-processor returns a whitelisted skippable error (matchOrderSkip), leaving
// no balance / metadata / reference / reverted trace for later orders. If
// admission folded that skipped order's effects into batchEffects anyway, a
// later Numscript's inputs_resolution_hash would embed phantom predecessor
// effects and be rejected STALE_INPUTS_RESOLUTION forever.
//
// The prediction is a PURE function of the order plus the resolved batch state
// (predecessors' effects layered over the pre-batch Pebble snapshot) — the same
// horizon the FSM's mutated WriteSet exposes when it reaches the order. It
// reproduces only the skip predicates that leave a fold-relevant trace:
//
//   - CreateTransaction / TRANSACTION_REFERENCE_CONFLICT — the FSM rejects when
//     the reference already exists (processCreateTransaction's dry prologue).
//     Admission checks the intra-batch createdReferences set first, then Pebble.
//   - RevertTransaction / TRANSACTION_ALREADY_REVERTED — the FSM rejects when the
//     transaction is already reverted (processRevertTransaction's bitset probe).
//     Admission checks the intra-batch revertedTxs set first, then the Pebble
//     reversion bitset.
//   - DeleteMetadata / METADATA_NOT_FOUND — the FSM rejects when the targeted
//     account-metadata key is absent. DeleteMetadata's only fold-relevant effect
//     is a tombstone; predicting the skip prevents folding a phantom tombstone.
//
// A predecessor whose skippable_reasons whitelist does not cover the reason it
// would hit is NOT skipped by the FSM (matchOrderSkip returns false), so it is
// applied and its effects DO count — this function returns false for that case,
// letting the caller fold as usual.
func (a *Admission) predictOrderSkip(order *raftcmdpb.Order, ledgerName string, effects *batchEffects) (bool, error) {
	allowed := order.GetLedgerScoped().GetApply().GetSkippableReasons()
	if len(allowed) == 0 {
		// No opt-in: matchOrderSkip never converts a failure into a skip, so
		// the order either applies (effects count) or fails the whole batch.
		return false, nil
	}

	switch data := order.GetLedgerScoped().GetApply().GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		ref := data.CreateTransaction.GetReference()
		if ref == "" {
			return false, nil
		}

		if !slices.Contains(allowed, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT) {
			return false, nil
		}

		return a.referenceExists(ledgerName, ref, effects)

	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		if !slices.Contains(allowed, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED) {
			return false, nil
		}

		return a.transactionReverted(ledgerName, data.RevertTransaction.GetTransactionId(), effects)

	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		// Only account-targeted deletes leave a fold-relevant tombstone; a
		// transaction-targeted delete is not observable by a later meta().
		acct, isAcct := data.DeleteMetadata.GetTarget().GetTarget().(*commonpb.Target_Account)
		if !isAcct {
			return false, nil
		}

		if !slices.Contains(allowed, commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND) {
			return false, nil
		}

		return a.accountMetadataAbsent(ledgerName, acct.Account.GetAddr(), data.DeleteMetadata.GetKey(), effects)

	default:
		return false, nil
	}
}

// referenceExists reports whether a transaction reference is already registered,
// checking the intra-batch createdReferences set first (a preceding successful
// create in this batch) then Pebble — mirroring processCreateTransaction's read
// of TransactionReferences against the FSM's mutated WriteSet.
func (a *Admission) referenceExists(ledgerName, reference string, effects *batchEffects) (bool, error) {
	key := domain.TransactionReferenceKey{LedgerName: ledgerName, Reference: reference}

	if effects != nil && effects.hasReference(key) {
		return true, nil
	}

	existing, err := a.attrs.References.Get(a.store, key.Bytes())
	if err != nil {
		return false, err
	}

	return existing != nil, nil
}

// transactionReverted reports whether a transaction is already reverted,
// checking the intra-batch revertedTxs set first (a preceding successful revert
// in this batch) then the Pebble reversion bitset — mirroring
// processRevertTransaction's GetReverted probe against the FSM's mutated bitset.
//
// The bitset lives under a per-ledger Pebble key range, so the read needs an
// iterator: *dal.Store exposes only point lookups (PebbleGetter), so we take a
// short-lived direct read handle (holds dbMu.RLock for its lifetime, does not
// block compactions) and close it before returning.
func (a *Admission) transactionReverted(ledgerName string, txID uint64, effects *batchEffects) (bool, error) {
	key := domain.TransactionKey{LedgerName: ledgerName, ID: txID}

	if effects != nil && effects.hasReverted(key) {
		return true, nil
	}

	handle, err := a.store.NewDirectReadHandle()
	if err != nil {
		return false, err
	}
	defer func() { _ = handle.Close() }()

	bs, err := query.ReadReversionBitset(handle, ledgerName)
	if err != nil {
		return false, err
	}

	return bs.Test(txID), nil
}

// accountMetadataAbsent reports whether an account-metadata key resolves absent,
// checking the intra-batch metadataWrites first (a preceding set makes it
// present; a preceding delete makes it absent) then Pebble — mirroring the state
// the FSM's DeleteMetadata handler sees.
func (a *Admission) accountMetadataAbsent(ledgerName, account, metaKey string, effects *batchEffects) (bool, error) {
	key := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
		Key:        metaKey,
	}

	if effects != nil {
		if write, ok := effects.metadataWrites[key]; ok {
			return write.deleted, nil
		}
	}

	value, err := a.attrs.Metadata.Get(a.store, key.Bytes())
	if err != nil {
		return false, err
	}

	return value == nil, nil
}
