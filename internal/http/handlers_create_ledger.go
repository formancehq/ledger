package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/davecgh/go-spew/spew"
	"github.com/formancehq/go-libs/v3/api"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/go-chi/chi/v5"
)

// handleCreateLedger handles POST /ledgers/{ledgerName} to create a new ledger
// The bucket is optional in the request body. If not specified or empty, a bucket with the ledger name will be created automatically.
// If the bucket doesn't exist, it will be created automatically with SQLite driver and default configuration.
func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Parse request body (bucket is optional, metadata is optional)
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

	// If bucket is empty, use ledger name as bucket name
	bucketName := req.Bucket
	if bucketName == "" {
		bucketName = ledgerName
	}

	// Try to get the bucket cluster
	bucket, err := s.cluster.GetBucketCluster(r.Context(), bucketName)
	if err != nil {
		// If bucket doesn't exist, create it automatically with SQLite driver and default config
		if errors.Is(err, &ledger.NotFoundError{}) {
			_, err = s.cluster.CreateBucket(r.Context(), bucketName, "sqlite", make(map[string]interface{}), nil)
			if err != nil {
				handleError(w, r, fmt.Errorf("failed to create bucket '%s': %w", bucketName, err))
				return
			}

			// Get the newly created bucket
			bucket, err = s.cluster.GetBucketCluster(r.Context(), bucketName)
			if err != nil {
				handleError(w, r, fmt.Errorf("failed to get newly created bucket '%s': %w", bucketName, err))
				return
			}
		} else {
			spew.Dump(err)
			handleError(w, r, err)
			return
		}
	}

	// Create ledger via cluster in the specified bucket
	ledgerInfo, err := bucket.CreateLedger(r.Context(), ledgerName, req.Metadata)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Return the ledger info with bucket name
	api.Created(w, LedgerResponse{
		LedgerInfo: *ledgerInfo,
		Bucket:     bucketName,
	})
}
