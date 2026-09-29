package http

import (
	"net/http"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// handleSaveTransactionMetadata handles POST /{ledgerName}/transactions/{transactionId}/metadata to save transaction metadata.
func (s *Server) handleSaveTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	transactionID, ok := requireTransactionID(w, r)
	if !ok {
		return
	}

	ms, ok := parseMetadataBody(w, r)
	if !ok {
		return
	}

	_, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{
					Data: &commonpb.LedgerAction_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target: &commonpb.Target{
								Target: &commonpb.Target_TransactionId{TransactionId: transactionID},
							},
							Metadata: ms,
						},
					},
				},
			},
		},
	})
	if err != nil {
		s.logger.WithFields(map[string]any{
			"ledger":         ledgerName,
			"transaction_id": transactionID,
			"error":          err,
		}).Errorf("Failed to save transaction metadata")
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
