package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
)

// handleCreateTransaction handles POST /{ledgerName}/transactions to create a new transaction
func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Decode request body directly into service.CreateTransaction
	var input service.CreateTransaction
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	// Validate postings
	if len(input.Postings) == 0 {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("postings are required"))
		return
	}

	// Extract dryRun from query parameter
	dryRun := r.URL.Query().Get("dryRun") == "true"

	// Extract idempotencyKey from header
	idempotencyKey := r.Header.Get("Idempotency-Key")

	// Build service.Parameters[service.CreateTransaction]
	params := service.Parameters[service.CreateTransaction]{
		DryRun:         dryRun,
		IdempotencyKey: idempotencyKey,
		Input:          input,
	}

	// Call ledger service
	_, createdTx, err := s.ledgerService.CreateTransaction(r.Context(), ledgerName, params)
	if err != nil {
		s.logger.Error("Failed to create transaction", zap.String("ledger", ledgerName), zap.Error(err))
		api.InternalServerError(w, r, err)
		return
	}

	// Return the service response directly - JSON encoding will handle it
	api.Created(w, createdTx)
}

