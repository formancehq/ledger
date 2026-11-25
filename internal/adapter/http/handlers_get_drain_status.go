package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// handleGetDrainStatus handles GET /_/cluster/drain. It reports the receiving
// node's local drain cursor.
func (s *Server) handleGetDrainStatus(w http.ResponseWriter, r *http.Request) {
	resp, err := s.backend.GetDrainStatus(r.Context(), &clusterpb.GetDrainStatusRequest{})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	writeOK(w, r, resp)
}
