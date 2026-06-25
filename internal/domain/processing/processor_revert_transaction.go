package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processRevertTransaction(ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.RevertTransactionOrder, s Scope, info *commonpb.LedgerInfo) (*commonpb.LedgerLogPayload, domain.Describable) {
	txKey := domain.TransactionKey{
		LedgerName: ledgerName,
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

	// Create reversed postings and update volumes
	// For a revert: original destination becomes source, original source becomes destination
	revertPostings := make([]*commonpb.Posting, len(order.GetOriginalPostings()))
	for i, originalPosting := range order.GetOriginalPostings() {
		// Create reversed posting
		revertPostings[i] = &commonpb.Posting{
			Source:      originalPosting.GetDestination(), // Original destination is now source
			Destination: originalPosting.GetSource(),      // Original source is now destination
			Amount:      originalPosting.GetAmount(),
			Asset:       originalPosting.GetAsset(),
		}
	}

	// Validate reversed postings against account types.
	if compiled := p.getCompiledTypes(ledgerName, info); len(compiled) > 0 {
		if typeErr := validatePostingsAgainstAccountTypes(revertPostings, compiled, info.GetDefaultEnforcementMode()); typeErr != nil {
			return nil, typeErr
		}
	}

	for _, posting := range revertPostings {
		// Apply the reversed posting (skip balance check if force is set)
		err := applyPosting(s, ledgerName, posting, order.GetForce(), p.assetCache)
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

	// Update the original transaction's state to record the reversion
	origStateReader, err := s.GetTransactionState(txKey)
	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "getting original transaction state", Cause: err}
	}

	if origStateReader == nil {
		return nil, &domain.ErrTransactionStateInconsistent{TransactionID: order.GetTransactionId(), Operation: "revert"}
	}

	origState := origStateReader.Mutate()
	origState.RevertedByTransaction = revertTxID
	s.PutTransactionState(txKey, origState)

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

	// Store the revert transaction's state (include metadata from the revert order)
	s.PutTransactionState(domain.TransactionKey{LedgerName: ledgerName, ID: revertTxID}, &commonpb.TransactionState{
		CreatedByLog: s.GetNextSequenceID(),
		Metadata:     order.GetMetadata(),
		Timestamp:    revertTimestamp,
	})

	// Compute post-commit volumes if requested
	var postCommitVolumes *commonpb.PostCommitVolumes
	if order.GetExpandVolumes() {
		postCommitVolumes = buildPostCommitVolumes(s, ledgerName, revertPostings)
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
					InsertedAt: s.GetDate().Mutate(),
					UpdatedAt:  s.GetDate().Mutate(),
				},
				PostCommitVolumes: postCommitVolumes,
			},
		},
	}, nil
}
