package http

import (
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// handleRevertTransaction handles POST /{ledgerName}/transactions/{transactionId}/revert to revert a transaction.
func (s *Server) handleRevertTransaction(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	transactionID, ok := requireTransactionID(w, r)
	if !ok {
		return
	}

	// Decode request body (optional - can be empty or contain metadata, force, atEffectiveDate)
	var reqBody map[string]any
	if r.ContentLength > 0 {
		err := json.UnmarshalRead(r.Body, &reqBody)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

			return
		}
	}

	// Build request payload
	payload := &servicepb.RevertTransactionPayload{
		TransactionId: transactionID,
	}

	// Extract optional fields from request body
	if reqBody != nil {
		if metadata, ok := reqBody["metadata"].(map[string]any); ok {
			metadataMap := make(map[string]string)

			for k, v := range metadata {
				if strVal, ok := v.(string); ok {
					metadataMap[k] = strVal
				}
			}

			payload.Metadata = commonpb.MetadataSetFromMap(metadataMap)
		}

		if force, ok := reqBody["force"].(bool); ok {
			payload.Force = force
		}

		if atEffectiveDate, ok := reqBody["atEffectiveDate"].(bool); ok {
			payload.AtEffectiveDate = atEffectiveDate
		}

		if ev, ok := reqBody["expandVolumes"].(bool); ok {
			payload.ExpandVolumes = ev
		}
	}

	logs, err := s.backend.Apply(r.Context(), &servicepb.Request{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_RevertTransaction{
					RevertTransaction: payload,
				},
			},
		},
	})
	if err != nil {
		s.logger.WithFields(map[string]any{
			"ledger":         ledgerName,
			"transaction_id": transactionID,
			"error":          err,
		}).Errorf("Failed to revert transaction")
		handleError(w, r, err)

		return
	}

	// Return the full reverted transaction response (includes post-commit volumes when requested)
	ledgerLog := logs[0].GetPayload().GetApply().GetLog()
	rt, ok := ledgerLog.GetData().GetPayload().(*commonpb.LedgerLogPayload_RevertedTransaction)
	if !ok {
		http.Error(w, "unexpected log payload type", http.StatusInternalServerError)

		return
	}
	revertedPayload := rt.RevertedTransaction
	writeCreated(w, revertedPayload)
}
