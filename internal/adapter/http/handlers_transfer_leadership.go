package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

type transferLeadershipBody struct {
	Transferee *clusterNodeID `json:"transferee"`
}

// handleTransferLeadership handles POST /_/cluster/leadership/transfer.
func (s *Server) handleTransferLeadership(w http.ResponseWriter, r *http.Request) {
	var body transferLeadershipBody
	if !decodeClusterBody(w, r, &body) {
		return
	}
	if body.Transferee == nil {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("transferee is required"))

		return
	}

	resp, err := s.backend.TransferLeadership(r.Context(), &clusterpb.TransferLeadershipRequest{Transferee: uint64(*body.Transferee)})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	writeOK(w, r, resp)
}
