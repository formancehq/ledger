package processing

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func processRevertTransaction(ledger string, order *raftcmdpb.RevertTransactionOrder, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	boundaries := ctx.Boundaries
	s := ctx.Scope
	info := ctx.LedgerInfo

	txKey := domain.TransactionKey{
		LedgerName: ledger,
		ID:         order.GetTransactionId(),
	}

	// Check if transaction exists (ID must be less than next transaction ID)
	if order.GetTransactionId() >= boundaries.GetNextTransactionId() {
		return nil, &domain.ErrTransactionNotFound{TransactionID: order.GetTransactionId()}
	}

	// Check if the transaction is already reverted (bitset lookup, never errors)
	reverted, err := s.GetReverted(txKey)
	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "checking reverted status", Cause: err}
	}

	if reverted {
		return nil, &domain.ErrTransactionAlreadyReverted{TransactionID: order.GetTransactionId()}
	}

	// Admission attaches OriginalPostings from either the signed receipt or
	// its own read of TxState.Postings (via attrs.Transaction.Get). A revert
	// order that reaches the FSM with an empty OriginalPostings means the
	// tx was not visible to admission at propose time — a business
	// rejection that must appear in the audit chain (invariant #8).
	originalPostings := order.GetOriginalPostings()
	if len(originalPostings) == 0 {
		return nil, &domain.ErrTransactionNotFound{TransactionID: order.GetTransactionId()}
	}

	origStateReader, err := s.TransactionStates().Get(txKey)
	if errors.Is(err, domain.ErrNotFound) {
		// Impossible by design under invariants #1/#6: the len(originalPostings)
		// check above already established the tx was visible to admission at
		// propose time (so admission preloaded TxState). A miss here implies
		// a cache/Pebble desync, not a routine business "not found" — surface
		// loudly with the invariant-violation error class.
		return nil, &domain.ErrTransactionStateInconsistent{TransactionID: order.GetTransactionId(), Operation: "revert"}
	}

	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "getting original transaction state", Cause: err}
	}

	origState := origStateReader.Mutate()

	// Create reversed postings and update volumes
	// For a revert: original destination becomes source, original source becomes destination.
	// Color carries over from the original posting — the funds were segregated under
	// (account, asset, color) on the way out, so they must return under the same bucket.
	revertPostings := make([]*commonpb.Posting, len(originalPostings))
	for i, originalPosting := range originalPostings {
		revertPostings[i] = &commonpb.Posting{
			Source:      originalPosting.GetDestination(),
			Destination: originalPosting.GetSource(),
			Amount:      originalPosting.GetAmount(),
			Asset:       originalPosting.GetAsset(),
			Color:       originalPosting.GetColor(),
		}
	}

	// Validate reversed postings against account types.
	if compiled := compiledTypesFor(ctx.CompiledTypes, ledger, info); len(compiled) > 0 {
		if typeErr := validatePostingsAgainstAccountTypes(revertPostings, compiled, info.GetDefaultEnforcementMode()); typeErr != nil {
			return nil, typeErr
		}
	}

	for _, posting := range revertPostings {
		// Apply the reversed posting (skip balance check if force is set)
		err := applyPosting(s, ledger, posting, order.GetForce(), ctx.AssetCache)
		if err != nil {
			return nil, err
		}
	}

	// Mark the original transaction as reverted
	s.PutReverted(txKey, true)

	// Get new transaction ID for the revert transaction
	revertTxID := boundaries.GetNextTransactionId()
	boundaries.NextTransactionId = revertTxID + 1

	// posting_count and revert_count are no longer maintained on
	// LedgerBoundaries — the usagebuilder derives them from the audit
	// chain. See EN-1420.

	// Resolve the revert timestamp. When at_effective_date is set, the compensating
	// transaction inherits the original's effective timestamp (parity with
	// formancehq/ledger). Otherwise it stamps with the current FSM date.
	// origState.Timestamp is populated at create time on every code path; missing
	// it on an at_effective_date revert means we observed an inconsistent state.
	revertTimestamp := s.GetDate().Mutate()
	if order.GetAtEffectiveDate() {
		if origState.GetTimestamp() == nil {
			return nil, &domain.ErrTransactionStateInconsistent{TransactionID: order.GetTransactionId(), Operation: "revert at_effective_date"}
		}

		revertTimestamp = origState.GetTimestamp()
	}

	// Record the reversion on the original transaction's state: the id of the
	// compensating transaction and the effective time it was reverted.
	origState.RevertedByTransaction = revertTxID
	origState.RevertedAt = revertTimestamp
	s.TransactionStates().Put(txKey, origState)

	// Store the revert transaction's state (include metadata from the revert
	// order); RevertsTransaction back-links it to the transaction it compensates.
	s.TransactionStates().Put(domain.TransactionKey{LedgerName: ledger, ID: revertTxID}, &commonpb.TransactionState{
		CreatedByLog:       s.GetNextSequenceID(),
		Metadata:           order.GetMetadata(),
		Timestamp:          revertTimestamp,
		Postings:           revertPostings,
		RevertsTransaction: order.GetTransactionId(),
	})

	// Compute post-commit volumes if requested
	var postCommitVolumes *commonpb.PostCommitVolumes
	if order.GetExpandVolumes() {
		var pcvErr domain.Describable
		postCommitVolumes, pcvErr = buildPostCommitVolumes(s, ledger, revertPostings)
		if pcvErr != nil {
			return nil, pcvErr
		}
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: order.GetTransactionId(),
				RevertTransaction: &commonpb.Transaction{
					Postings:           revertPostings,
					Metadata:           order.GetMetadata(),
					Timestamp:          revertTimestamp,
					Id:                 revertTxID,
					InsertedAt:         s.GetDate().Mutate(),
					UpdatedAt:          s.GetDate().Mutate(),
					RevertsTransaction: order.GetTransactionId(),
				},
				PostCommitVolumes: postCommitVolumes,
			},
		},
	}, nil
}
