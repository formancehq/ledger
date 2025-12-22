package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
)

// handleCreateLedger handles POST /{ledgerName} to create a new ledger
func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Parse request body (driver, config, metadata, snapshotThreshold are optional)
	var req struct {
		Driver            string                 `json:"driver,omitempty"`
		Config            map[string]interface{} `json:"config,omitempty"`
		Metadata          map[string]string      `json:"metadata,omitempty"`
		SnapshotThreshold *uint64                `json:"snapshot_threshold,omitempty"`
	}

	if r.Body == nil {
		// Use defaults: SQLite driver with empty config
		req.Driver = "sqlite"
		req.Config = make(map[string]interface{})
	} else {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
			return
		}
	}

	// Use defaults if not provided
	if req.Driver == "" {
		req.Driver = "sqlite"
	}
	if req.Config == nil {
		req.Config = make(map[string]interface{})
	}

	// Create ledger via cluster
	ledgerInfo, err := s.cluster.CreateLedger(r.Context(), ledgerName, req.Driver, req.Config, req.Metadata, req.SnapshotThreshold)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Return the ledger info
	api.Created(w, LedgerResponse{
		LedgerInfo: ledgerInfo,
	})
}
