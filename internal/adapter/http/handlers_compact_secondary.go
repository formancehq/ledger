package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// handleCompactSecondary handles POST /_/cluster/compactions/secondary. It
// compacts the receiving node's read index store.
func (s *Server) handleCompactSecondary(w http.ResponseWriter, r *http.Request) {
	resp, err := s.backend.CompactSecondary(r.Context(), &clusterpb.CompactSecondaryRequest{})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	writeOK(w, r, resp)
}
