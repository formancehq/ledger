package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
)

// handleGetLedger handles GET /ledgers/{ledgerName} to get a ledger
func (s *Server) handleGetLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	bucketName, _, err := s.cluster.ResolveLedger(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	bucketInfo, err := s.cluster.GetBucketInfo(r.Context(), bucketName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	bucket, err := s.cluster.GetBucketCluster(r.Context(), bucketName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	ledgerInfo, err := bucket.GetLedger(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Return ledger info with bucket name
	api.Ok(w, LedgerResponse{
		LedgerInfo: *ledgerInfo,
		Bucket:     bucketInfo.Name,
	})
}
