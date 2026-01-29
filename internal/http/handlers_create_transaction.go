package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/go-chi/chi/v5"
)

// handleCreateTransaction handles POST /{ledgerName}/transactions to create a new transaction
func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Decode request body into protobuf CreateTransactionPayload
	req := &servicepb.CreateTransactionPayload{}
	err := json.UnmarshalRead(r.Body, req)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	ledgerInfo, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)
		return
	}

	// Call ledger service via Apply
	logs, err := s.backend.Apply(r.Context(), &servicepb.Action{
		Type: &servicepb.Action_Apply{
			Apply: &servicepb.LedgerApplyAction{
				LedgerId:       ledgerInfo.Id,
				IdempotencyKey: r.Header.Get("Idempotency-Key"),
				Data: &servicepb.LedgerApplyAction_CreateTransaction{
					CreateTransaction: req,
				},
			},
		},
	})
	if err != nil {
		s.logger.WithFields(map[string]any{"ledger": ledgerName, "error": err}).Errorf("Failed to create transaction")
		handleError(w, r, err)
		return
	}

	// Return the service response directly - JSON encoding will handle it
	ledgerLog := logs[0].GetApply().GetLog()
	writeCreated(w, ledgerLog.Data.Payload.(*commonpb.LogPayload_CreatedTransaction).CreatedTransaction)
}
