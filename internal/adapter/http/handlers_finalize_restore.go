package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

// handleFinalizeRestore handles POST /_/restore/finalize.
func (s *restoreServer) handleFinalizeRestore(w http.ResponseWriter, r *http.Request) {
	resp, err := s.backend.FinalizeRestore(r.Context(), &restorepb.FinalizeRestoreRequest{})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	writeOK(w, r, resp)
}
