package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
)

// handleSaveAccountMetadata handles POST /{ledgerName}/accounts/{address}/metadata to save account metadata
func (s *Server) handleSaveAccountMetadata(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	address := chi.URLParam(r, "address")
	if address == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("account address is required"))
		return
	}

	// Decode request body into metadata
	var inputMetadata map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&inputMetadata); err != nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	// Convert map[string]interface{} to metadata.Metadata
	accountMetadata := make(metadata.Metadata)
	for k, v := range inputMetadata {
		accountMetadata[k] = fmt.Sprintf("%v", v)
	}

	// Extract dryRun from query parameter
	dryRun := r.URL.Query().Get("dryRun") == "true"

	// Extract idempotencyKey from header
	idempotencyKey := r.Header.Get("Idempotency-Key")

	// Build service.Parameters[service.SaveAccountMetadata]
	params := service.Parameters[service.SaveAccountMetadata]{
		DryRun:         dryRun,
		IdempotencyKey: idempotencyKey,
		Input: service.SaveAccountMetadata{
			Address:  address,
			Metadata: accountMetadata,
		},
	}

	ledgerCluster, err := s.cluster.GetLedgerCluster(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Call ledger service
	_, err = ledgerCluster.SaveAccountMetadata(r.Context(), ledgerName, params)
	if err != nil {
		s.logger.WithFields(map[string]any{"ledger": ledgerName, "address": address, "error": err}).Errorf("Failed to save account metadata")
		handleError(w, r, err)
		return
	}

	// Return 204 No Content (no Content-Type header for 204)
	w.WriteHeader(http.StatusNoContent)
}
