package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

// handlePreviewRestore handles GET /_/restore/preview.
func (s *restoreServer) handlePreviewRestore(w http.ResponseWriter, r *http.Request) {
	resp, err := s.backend.PreviewRestore(r.Context(), &restorepb.PreviewRestoreRequest{})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	writeOK(w, r, resp)
}
