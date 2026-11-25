package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

// handleGetRestoreDownload handles GET /_/restore/downloads/{jobId}.
func (s *restoreServer) handleGetRestoreDownload(w http.ResponseWriter, r *http.Request) {
	resp, err := s.backend.GetDownloadStatus(r.Context(), &restorepb.GetDownloadStatusRequest{
		JobId: chi.URLParam(r, "jobId"),
	})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	writeOK(w, r, resp)
}
