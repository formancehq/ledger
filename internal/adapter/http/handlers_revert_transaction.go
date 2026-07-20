package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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
			// Decode metadata through the shared typed-metadata path so numeric,
			// boolean and other non-string values survive losslessly (parity with
			// create-transaction / set-metadata). Invalid values (objects, arrays,
			// non-integer floats) are rejected with 400 INVALID_REQUEST instead of
			// being silently dropped (EN-1509).
			ms, err := commonpb.MetadataFromAnyMap(metadata)
			if err != nil {
				writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid metadata: %w", err))

				return
			}

			payload.Metadata = ms
		}

		if force, ok := reqBody["force"].(bool); ok {
			payload.Force = force
		}

		if atEffectiveDate, ok := reqBody["atEffectiveDate"].(bool); ok {
			payload.AtEffectiveDate = atEffectiveDate
		}
	}

	logs, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_RevertTransaction{
						RevertTransaction: payload,
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
		}).Errorf("Failed to revert transaction")
		handleError(w, r, err)

		return
	}

	// Return the full reverted transaction response (includes post-commit volumes when requested)
	if len(logs) == 0 {
		unreachable("revert-transaction apply returned no log", map[string]any{
			"ledger":         ledgerName,
			"transaction_id": transactionID,
		})
	}

	ledgerLog := logs[0].GetPayload().GetApply().GetLog()
	rt, ok := ledgerLog.GetData().GetPayload().(*commonpb.LedgerLogPayload_RevertedTransaction)
	if !ok {
		writeInternalServerError(w, r, errors.New("unexpected log payload type"))

		return
	}
	revertedPayload := rt.RevertedTransaction
	writeCreated(w, revertedPayload)
}
