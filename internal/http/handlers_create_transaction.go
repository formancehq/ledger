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

	// Call ledger service via Apply
	logs, err := s.backend.Apply(r.Context(), &servicepb.Request{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: servicepb.LedgerName(ledgerName),
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
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
	ledgerLog := logs[0].Payload.GetApply().GetLog()
	writeCreated(w, ledgerLog.Data.Payload.(*commonpb.LedgerLogPayload_CreatedTransaction).CreatedTransaction)
}
