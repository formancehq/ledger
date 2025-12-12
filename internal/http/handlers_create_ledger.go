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
// The bucket must be specified in the request body
func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Parse request body (bucket is required, metadata is optional)
	var req struct {
		Bucket   string            `json:"bucket"`
		Metadata map[string]string `json:"metadata,omitempty"`
	}

	if r.Body == nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("request body is required"))
		return
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	// Bucket is required in request body
	if req.Bucket == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("bucket is required"))
		return
	}

	bucket, err := s.cluster.GetBucket(r.Context(), req.Bucket)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Create ledger via cluster in the specified bucket
	ledger, err := bucket.CreateLedger(r.Context(), ledgerName, req.Metadata)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Return the ledger info with bucket name
	api.Created(w, LedgerResponse{
		LedgerInfo: *ledger,
		Bucket:     req.Bucket,
	})
}
