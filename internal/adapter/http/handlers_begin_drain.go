package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

type drainBody struct {
	Epoch string `json:"epoch"`
}

// handleBeginDrain handles POST /_/cluster/drain.
func (s *Server) handleBeginDrain(w http.ResponseWriter, r *http.Request) {
	var body drainBody
	if !decodeClusterBody(w, r, &body) {
		return
	}

	resp, err := s.backend.BeginDrain(r.Context(), &clusterpb.BeginDrainRequest{Epoch: body.Epoch})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	writeOK(w, r, resp)
}
