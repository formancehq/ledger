package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

// handleCancelRestoreDownload handles DELETE /_/restore/downloads/{jobId}.
// Cancelling a finished job succeeds without effect.
func (s *restoreServer) handleCancelRestoreDownload(w http.ResponseWriter, r *http.Request) {
	_, err := s.backend.CancelDownload(r.Context(), &restorepb.CancelDownloadRequest{
		JobId: chi.URLParam(r, "jobId"),
	})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
