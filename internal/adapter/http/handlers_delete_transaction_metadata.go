package http

import (
	"net/http"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// handleDeleteTransactionMetadata handles DELETE /{ledgerName}/transactions/{transactionId}/metadata/{key} to delete transaction metadata.
func (s *Server) handleDeleteTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	transactionID, ok := requireTransactionID(w, r)
	if !ok {
		return
	}

	key, ok := requireMetadataKey(w, r)
	if !ok {
		return
	}

	_, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{
					Data: &commonpb.LedgerAction_DeleteMetadata{
						DeleteMetadata: &commonpb.DeleteMetadataCommand{
							Target: &commonpb.Target{
								Target: &commonpb.Target_TransactionId{TransactionId: transactionID},
							},
							Key: key,
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
			"key":            key,
			"error":          err,
		}).Errorf("Failed to delete transaction metadata")
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
