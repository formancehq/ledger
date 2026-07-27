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

	// The original postings live on the transaction's own state, which
	// admission preloaded (addTransactionTargetNeeds) and the FSM reads
	// through the coverage gate — never off the order, which carries only
	// caller intent (invariant #8). A genuinely non-existent tx is already
	// rejected by the boundary check above; a miss here means an allocated
	// tx with no state (chapter archival does not evict TransactionState,
	// and IDs have no gaps), i.e. a cache/Pebble desync — surface loudly
	// with the invariant-violation error class (invariant #7).
	origStateReader, err := s.TransactionStates().Get(txKey)
	if errors.Is(err, domain.ErrNotFound) {
		return nil, &domain.ErrTransactionStateInconsistent{TransactionID: order.GetTransactionId(), Operation: "revert"}
	}

	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "getting original transaction state", Cause: err}
	}

	origState := origStateReader.Mutate()

	originalPostings := origState.GetPostings()
	if len(originalPostings) == 0 {
		// Create rejects empty transactions, so a stored state always carries
		// at least one posting; an empty set here is an inconsistent projection
		// (invariant #7), not a revertable transaction.
		return nil, &domain.ErrTransactionStateInconsistent{TransactionID: order.GetTransactionId(), Operation: "revert"}
	}

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

	// Post-commit volumes are part of every persisted transaction: compute
	// them unconditionally from the volume state after the compensating
	// postings applied (before any proposal-level ephemeral purge). The
	// compensating transaction carries its own post-revert snapshot; the
	// original keeps its creation-time snapshot untouched.
	postCommitVolumes, pcvErr := buildPostCommitVolumes(s, ledger, revertPostings)
	if pcvErr != nil {
		return nil, pcvErr
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
					PostCommitVolumes:  postCommitVolumes,
				},
			},
		},
	}, nil
}
