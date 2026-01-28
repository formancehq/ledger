package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
)

// handleCreateTransaction handles POST /{ledgerName}/transactions to create a new transaction
func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Decode request body into protobuf CreateTransactionRequest
	req := &ledgerpb.CreateTransactionRequestPayload{}
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

	// Call ledger service
	log, err := s.backend.CreateTransaction(r.Context(), ledgerInfo.Id, service.Parameters[*ledgerpb.CreateTransactionRequestPayload]{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Input:          req,
	})
	if err != nil {
		s.logger.WithFields(map[string]any{"ledger": ledgerName, "error": err}).Errorf("Failed to create transaction")
		handleError(w, r, err)
		return
	}

	// Return the service response directly - JSON encoding will handle it
	writeCreated(w, log.Data.Payload.(*commonpb.LogPayload_CreatedTransaction).CreatedTransaction)
}
