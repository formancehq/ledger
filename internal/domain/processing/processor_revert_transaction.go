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
	// For a revert: original destination becomes source, original source becomes destination
	revertPostings := make([]*commonpb.Posting, len(originalPostings))
	for i, originalPosting := range originalPostings {
		// Create reversed posting
		revertPostings[i] = &commonpb.Posting{
			Source:      originalPosting.GetDestination(), // Original destination is now source
			Destination: originalPosting.GetSource(),      // Original source is now destination
			Amount:      originalPosting.GetAmount(),
			Asset:       originalPosting.GetAsset(),
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
	boundaries.PostingCount += uint64(len(revertPostings))
	boundaries.RevertCount++

	// Record the reversion on the original transaction's state.
	origState.RevertedByTransaction = revertTxID
	s.TransactionStates().Put(txKey, origState)

	// Resolve the revert timestamp. When at_effective_date is set, the compensating
	// transaction inherits the original's effective timestamp (parity with
	// formancehq/ledger). Otherwise it stamps with the current FSM date.
	// origState.Timestamp is populated at create time on every code path; missing
	// it on an at_effective_date revert means we observed an inconsistent state.
	revertTimestamp := s.GetDate().Micros()
	if order.GetAtEffectiveDate() {
		if origState.GetTimestamp() == 0 {
			return nil, &domain.ErrTransactionStateInconsistent{TransactionID: order.GetTransactionId(), Operation: "revert at_effective_date"}
		}

		revertTimestamp = origState.GetTimestamp()
	}

	// Store the revert transaction's state (include metadata from the revert order)
	s.TransactionStates().Put(domain.TransactionKey{LedgerName: ledger, ID: revertTxID}, &commonpb.TransactionState{
		CreatedByLog: s.GetNextSequenceID(),
		Metadata:     order.GetMetadata(),
		Timestamp:    revertTimestamp,
		Postings:     revertPostings,
	})

	// Compute post-commit volumes if requested
	var postCommitVolumes *commonpb.PostCommitVolumes
	if order.GetExpandVolumes() {
		var err domain.Describable
		postCommitVolumes, err = buildPostCommitVolumes(s, ledger, revertPostings)
		if err != nil {
			return nil, err
		}
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: order.GetTransactionId(),
				RevertTransaction: &commonpb.Transaction{
					Postings:   revertPostings,
					Metadata:   order.GetMetadata(),
					Timestamp:  revertTimestamp,
					Id:         revertTxID,
					InsertedAt: s.GetDate().Micros(),
					UpdatedAt:  s.GetDate().Micros(),
				},
				PostCommitVolumes: postCommitVolumes,
			},
		},
	}, nil
}
