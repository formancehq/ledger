package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

// handleCreateLedger handles POST /ledgers/{ledgerName} to create a new ledger
// The bucket is determined by checking if the ledger already exists, or by trying to create it in the first available bucket
func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Check if ledger already exists to find its bucket
	bucketName, err := s.cluster.FindBucketForLedger(ledgerName)
	if err == nil {
		// Ledger already exists
		api.WriteErrorResponse(w, http.StatusConflict, "LEDGER_ALREADY_EXISTS", fmt.Errorf("ledger %s already exists in bucket %s", ledgerName, bucketName))
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
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("bucket name is required in request body"))
		return
	}

	// Create ledger via cluster in the specified bucket
	if err := s.cluster.CreateLedger(req.Bucket, ledgerName, req.Metadata); err != nil {
		s.logger.WithFields(map[string]any{"bucket": req.Bucket, "name": ledgerName, "error": err}).Errorf("Failed to create ledger")

		// Check if ledger already exists (in this bucket or globally)
		errMsg := err.Error()
		if errMsg == fmt.Sprintf("ledger already exists in bucket %s: %s", req.Bucket, ledgerName) ||
			errMsg == fmt.Sprintf("ledger with name %s already exists in bucket", ledgerName) ||
			errMsg == fmt.Sprintf("creating ledger in bucket %s: ledger already exists in bucket %s: %s", req.Bucket, req.Bucket, ledgerName) ||
			errMsg == fmt.Sprintf("creating ledger in bucket %s: ledger with name %s already exists in bucket", req.Bucket, ledgerName) {
			api.WriteErrorResponse(w, http.StatusConflict, "LEDGER_ALREADY_EXISTS", err)
			return
		}

		api.InternalServerError(w, r, err)
		return
	}

	// Get the created ledger to return it
	ledgerInfo, exists, err := s.cluster.GetLedger(req.Bucket, ledgerName)
	if err != nil || !exists {
		s.logger.WithFields(map[string]any{"bucket": req.Bucket, "name": ledgerName, "error": err}).Infof("WARN: Failed to retrieve created ledger")
		// Still return success since creation succeeded
		api.Created(w, LedgerResponse{
			LedgerInfo: service.LedgerInfo{
				Name:     ledgerName,
				Metadata: req.Metadata,
			},
			Bucket: req.Bucket,
		})
		return
	}

	// Return the ledger info with bucket name
	api.Created(w, LedgerResponse{
		LedgerInfo: ledgerInfo,
		Bucket:     req.Bucket,
	})
}

