package processing

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

func (p *RequestProcessor) processRevertTransaction(ledgerID uint32, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.RevertTransactionOrder, s Store) (*commonpb.LedgerLogPayload, error) {
	txKey := dal.TransactionKey{
		LedgerID: ledgerID,
		ID:       order.TransactionId,
	}

	// Check if transaction exists (ID must be less than next transaction ID)
	if order.TransactionId >= boundaries.NextTransactionId {
		return nil, &ErrTransactionNotFound{TransactionID: order.TransactionId}
	}

	// Check if the transaction is already reverted
	reverted, err := s.GetReverted(txKey)
	if err != nil && !errors.Is(err, dal.ErrNotFound) {
		return nil, fmt.Errorf("checking reverted status: %w", err)
	}
	if reverted {
		return nil, &ErrTransactionAlreadyReverted{TransactionID: order.TransactionId}
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
		if err := applyPosting(s, ledgerID, revertPostings[i], order.Force); err != nil {
			return nil, err
		}
	}

	// Mark the original transaction as reverted
	s.PutReverted(txKey, true)

	// Get new transaction ID for the revert transaction
	revertTxID := boundaries.NextTransactionId
	boundaries.NextTransactionId = revertTxID + 1

	// Add transaction update for the original transaction (mark as reverted)
	s.AddTransactionUpdate(dal.TransactionKey{LedgerID: ledgerID, ID: order.TransactionId}, &commonpb.TransactionUpdate{
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
	s.AddTransactionUpdate(dal.TransactionKey{LedgerID: ledgerID, ID: revertTxID}, &commonpb.TransactionUpdate{
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
