package processing

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processRevertTransaction(ledger string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.RevertTransactionOrder, s InMemoryStore) (*commonpb.LedgerLogPayload, error) {
	txKey := domain.TransactionKey{
		Ledger: ledger,
		ID:     order.GetTransactionId(),
	}

	// Check if transaction exists (ID must be less than next transaction ID)
	if order.GetTransactionId() >= boundaries.GetNextTransactionId() {
		return nil, &domain.ErrTransactionNotFound{TransactionID: order.GetTransactionId()}
	}

	// Check if the transaction is already reverted (bitset lookup, never errors)
	reverted, err := s.GetReverted(txKey)
	if err != nil {
		return nil, fmt.Errorf("checking reverted status: %w", err)
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
	if info, ok := s.GetLedger(ledger); ok {
		if len(info.GetAccountTypes()) > 0 {
			if typeErr := validatePostingsAgainstAccountTypes(revertPostings, info.GetAccountTypes()); typeErr != nil {
				return nil, typeErr
			}
		}
	}

	for _, posting := range revertPostings {
		// Apply the reversed posting (skip balance check if force is set)
		err := applyPosting(s, ledger, posting, order.GetForce())
		if err != nil {
			return nil, err
		}
	}

	// Mark the original transaction as reverted
	s.PutReverted(txKey, true)

	// Get new transaction ID for the revert transaction
	revertTxID := boundaries.GetNextTransactionId()
	boundaries.NextTransactionId = revertTxID + 1

	// Update the original transaction's state to record the reversion
	origState, err := s.GetTransactionState(txKey)
	if err != nil {
		return nil, fmt.Errorf("getting original transaction state: %w", err)
	}

	if origState == nil {
		return nil, fmt.Errorf("original transaction state not found for tx %d", order.GetTransactionId())
	}

	origState.RevertedByTransaction = revertTxID
	s.PutTransactionState(txKey, origState)

	// Store the revert transaction's state (include metadata from the revert order)
	s.PutTransactionState(domain.TransactionKey{Ledger: ledger, ID: revertTxID}, &commonpb.TransactionState{
		CreatedByLog: s.GetNextSequenceID(),
		Metadata:     order.GetMetadata(),
	})

	// Compute post-commit volumes if requested
	var postCommitVolumes *commonpb.PostCommitVolumes
	if order.GetExpandVolumes() {
		postCommitVolumes = buildPostCommitVolumes(s, ledger, revertPostings)
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: order.GetTransactionId(),
				RevertTransaction: &commonpb.Transaction{
					Postings:   revertPostings,
					Metadata:   order.GetMetadata(),
					Timestamp:  s.GetDate(),
					Id:         revertTxID,
					InsertedAt: s.GetDate(),
					UpdatedAt:  s.GetDate(),
				},
				PostCommitVolumes: postCommitVolumes,
			},
		},
	}, nil
}
