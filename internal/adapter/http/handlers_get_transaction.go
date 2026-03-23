package http

import (
	"net/http"
)

// handleGetTransaction handles GET /{ledgerName}/transactions/{transactionId} to retrieve a transaction.
func (s *Server) handleGetTransaction(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	transactionID, ok := requireTransactionID(w, r)
	if !ok {
		return
	}

	// Verify ledger exists
	_, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	transaction, err := s.backend.GetTransaction(r.Context(), ledgerName, transactionID)
	if err != nil {
		s.logger.WithFields(map[string]any{
			"ledger":         ledgerName,
			"transaction_id": transactionID,
			"error":          err,
		}).Errorf("Failed to get transaction")
		handleError(w, r, err)

		return
	}

	writeOK(w, transaction)
}
