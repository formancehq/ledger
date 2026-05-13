package http

import (
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
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

	var inputMetadata map[string]any
	if err := json.UnmarshalRead(r.Body, &inputMetadata); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return
	}

	ms, metaErr := commonpb.MetadataFromAnyMap(inputMetadata)
	if metaErr != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid metadata: %w", metaErr))

		return
	}

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target: &commonpb.Target{
								Target: &commonpb.Target_Transaction{
									Transaction: &commonpb.TargetTransaction{
										Id: transactionID,
									},
								},
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
