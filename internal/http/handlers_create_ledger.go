package http

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleCreateLedger handles POST /{ledgerName} to create a new ledger
func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Parse request body (logStoreDriver, runtimeStoreDriver, logStoreConfig, runtimeStoreConfig, metadata, snapshotThreshold)
	var req struct {
		LogStoreDriver     string                 `json:"logStoreDriver"`
		RuntimeStoreDriver string                 `json:"runtimeStoreDriver"`
		LogStoreConfig     map[string]interface{} `json:"logStoreConfig,omitempty"`
		RuntimeStoreConfig map[string]interface{} `json:"runtimeStoreConfig,omitempty"`
		Metadata           map[string]string      `json:"metadata,omitempty"`
		SnapshotThreshold  *uint64                `json:"snapshotThreshold,omitempty"`
	}

	if r.Body == nil {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("request body is required"))
		return
	}

	if err := json.UnmarshalRead(r.Body, &req); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	// Validate required fields
	if req.LogStoreDriver == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("logStoreDriver is required"))
		return
	}

	if req.RuntimeStoreDriver == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("runtimeStoreDriver is required"))
		return
	}

	// Create ledger via cluster
	ledgerInfo, err := s.cluster.CreateLedger(r.Context(), ledgerName, req.LogStoreConfig, req.RuntimeStoreConfig, req.Metadata, req.SnapshotThreshold, req.LogStoreDriver, req.RuntimeStoreDriver)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Return the ledger info wrapped in BaseResponse
	writeCreated(w, ledgerInfo)
}
