package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
)

// handleCreateTransaction handles POST /{ledgerName}/transactions to create a new transaction
func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Decode request body into protobuf CreateTransactionRequest
	req := &ledgerpb.CreateTransactionRequestPayload{}
	err := json.NewDecoder(r.Body).Decode(req)
	if err != nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	// Build service.Parameters[*ledgerpb.CreateTransactionRequest]
	params := service.Parameters[*ledgerpb.CreateTransactionRequestPayload]{
		DryRun:         r.URL.Query().Get("dryRun") == "true",
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Input:          req,
	}

	ledgerCluster, err := s.cluster.GetLedgerCluster(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Call ledger service
	_, createdTx, err := ledgerCluster.CreateTransaction(r.Context(), ledgerName, params)
	if err != nil {
		s.logger.WithFields(map[string]any{"ledger": ledgerName, "error": err}).Errorf("Failed to create transaction")
		handleError(w, r, err)
		return
	}

	// Return the service response directly - JSON encoding will handle it
	api.Created(w, createdTx)
}
