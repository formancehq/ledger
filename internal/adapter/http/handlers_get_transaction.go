package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// getTransactionData is the `data` envelope of GET
// /{ledgerName}/transactions/{transactionId}. It pairs the transaction with the
// verifiability receipt the controller read path produced, mirroring the gRPC
// GetTransactionResponse{transaction, receipt} shape (EN-1510).
//
// The receipt is always emitted: transactions that legitimately have none
// (e.g. reversals, or nodes that cannot sign) surface an empty string, so
// clients see a stable field rather than a sometimes-absent key. The receipt is
// reused verbatim from the backend — never recomputed here — so checkpoint/live
// consistency stays owned by the controller read path.
type getTransactionData struct {
	Transaction *commonpb.Transaction `json:"transaction"`
	Receipt     string                `json:"receipt"`
}

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

	transaction, receipt, err := s.backend.GetTransaction(r.Context(), ledgerName, transactionID)
	if err != nil {
		s.logger.WithFields(map[string]any{
			"ledger":         ledgerName,
			"transaction_id": transactionID,
			"error":          err,
		}).Errorf("Failed to get transaction")
		handleError(w, r, err)

		return
	}

	// receipt is non-nil on the success path (the gRPC client always returns a
	// non-nil token, empty for transactions without one); nil-guard defensively
	// so a future backend that returns nil still renders a consistent "".
	var receiptToken string
	if receipt != nil {
		receiptToken = *receipt
	}

	writeOK(w, getTransactionData{
		Transaction: transaction,
		Receipt:     receiptToken,
	})
}
