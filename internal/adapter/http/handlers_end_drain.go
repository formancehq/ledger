package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// handleEndDrain handles POST /_/cluster/drain/end.
func (s *Server) handleEndDrain(w http.ResponseWriter, r *http.Request) {
	var body drainBody
	if !decodeClusterBody(w, r, &body) {
		return
	}

	resp, err := s.backend.EndDrain(r.Context(), &clusterpb.EndDrainRequest{Epoch: body.Epoch})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	writeOK(w, r, resp)
}
