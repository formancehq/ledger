package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// handleCompactPrimary handles POST /_/cluster/compactions/primary. It
// compacts the receiving node's primary store.
func (s *Server) handleCompactPrimary(w http.ResponseWriter, r *http.Request) {
	resp, err := s.backend.CompactPrimary(r.Context(), &clusterpb.CompactPrimaryRequest{})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	writeOK(w, r, resp)
}
