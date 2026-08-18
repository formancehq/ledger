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
//
// Transaction is typed `any` so the same struct serves both amount wire modes
// (EN-1779). It carries no `omitempty`, so a nil value renders as null in both,
// matching the previous *commonpb.Transaction.
type getTransactionData struct {
	Transaction any    `json:"transaction"`
	Receipt     string `json:"receipt"`
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

	// The wrapper has a value receiver, so the value goes into the interface: a
	// pointer to it is not addressable behind an `any` field, the declared
	// MarshalJSON is bypassed, and sonic reflects the protobuf struct into
	// PascalCase field names with neither a compile nor a runtime error.
	var transactionValue any = transaction
	if wantsBigintAsString(r) {
		transactionValue = commonpb.StringAmountTransaction{Transaction: transaction}
	}

	// writeCheckedStatus, not writeOKChecked: this route has always written
	// through the ConfigStd encoder, and writeOKChecked would silently drop the
	// HTML escaping and the trailing newline for every client (EN-1779).
	writeCheckedStatus(w, r, http.StatusOK, getTransactionData{
		Transaction: transactionValue,
		Receipt:     receiptToken,
	})
}
