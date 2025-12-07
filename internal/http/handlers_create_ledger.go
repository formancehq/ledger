package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
)

// handleCreateLedger handles POST /ledgers/{ledgerName} to create a new ledger
// The bucket is determined by checking if the ledger already exists, or by trying to create it in the first available bucket
func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Parse request body (bucket and optional metadata)
	var req struct {
		Bucket   string            `json:"bucket"`
		Metadata map[string]string `json:"metadata,omitempty"`
	}

	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
			api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
			return
		}
	}

	// Bucket is required in request body
	if req.Bucket == "" {
		req.Bucket = "default"
	}

	bucket, err := s.cluster.GetBucket(r.Context(), req.Bucket)
	if err != nil {
		api.InternalServerError(w, r, err)
		return
	}

	// Create ledger via cluster in the specified bucket
	ledger, err := bucket.CreateLedger(r.Context(), ledgerName, req.Metadata)
	if err != nil {
		api.InternalServerError(w, r, err)
		return
	}

	// Return the ledger info with bucket name
	api.Created(w, LedgerResponse{
		LedgerInfo: *ledger,
		Bucket:     req.Bucket,
	})
}
