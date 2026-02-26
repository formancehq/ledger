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
		ID:     order.TransactionId,
	}

	// Check if transaction exists (ID must be less than next transaction ID)
	if order.TransactionId >= boundaries.NextTransactionId {
		return nil, &domain.ErrTransactionNotFound{TransactionID: order.TransactionId}
	}

	// Check if the transaction is already reverted (bitset lookup, never errors)
	reverted, err := s.GetReverted(txKey)
	if err != nil {
		return nil, fmt.Errorf("checking reverted status: %w", err)
	}
	if reverted {
		return nil, &domain.ErrTransactionAlreadyReverted{TransactionID: order.TransactionId}
	}

	// Create reversed postings and update volumes
	// For a revert: original destination becomes source, original source becomes destination
	revertPostings := make([]*commonpb.Posting, len(order.OriginalPostings))
	for i, originalPosting := range order.OriginalPostings {
		// Create reversed posting
		revertPostings[i] = &commonpb.Posting{
			Source:      originalPosting.Destination, // Original destination is now source
			Destination: originalPosting.Source,      // Original source is now destination
			Amount:      originalPosting.Amount,
			Asset:       originalPosting.Asset,
		}

		// Apply the reversed posting (skip balance check if force is set)
		if err := applyPosting(s, ledger, revertPostings[i], order.Force); err != nil {
			return nil, err
		}
	}

	// Mark the original transaction as reverted
	s.PutReverted(txKey, true)

	// Get new transaction ID for the revert transaction
	revertTxID := boundaries.NextTransactionId
	boundaries.NextTransactionId = revertTxID + 1

	// Add transaction update for the original transaction (mark as reverted)
	s.AddTransactionUpdate(domain.TransactionKey{Ledger: ledger, ID: order.TransactionId}, &commonpb.TransactionUpdate{
		ByLog: s.GetNextSequenceID(),
		Updates: []*commonpb.TransactionUpdateType{{
			TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationRevert{
				TransactionModificationRevert: &commonpb.TransactionUpdateRevert{
					ByTransaction: revertTxID,
				},
			},
		}},
	})

	// Add transaction init for the revert transaction
	s.AddTransactionUpdate(domain.TransactionKey{Ledger: ledger, ID: revertTxID}, &commonpb.TransactionUpdate{
		ByLog: s.GetNextSequenceID(),
		Updates: []*commonpb.TransactionUpdateType{{
			TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
				TransactionInit: &commonpb.TransactionInit{},
			},
		}},
	})

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: order.TransactionId,
				RevertTransaction: &commonpb.Transaction{
					Postings:   revertPostings,
					Metadata:   order.Metadata,
					Timestamp:  s.GetDate(),
					Id:         revertTxID,
					InsertedAt: s.GetDate(),
					UpdatedAt:  s.GetDate(),
				},
			},
		},
	}, nil
}
