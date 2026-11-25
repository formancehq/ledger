package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// handlePromoteLearner handles POST /_/cluster/learners/{nodeId}/promote.
func (s *Server) handlePromoteLearner(w http.ResponseWriter, r *http.Request) {
	nodeID, ok := requireClusterNodeID(w, r)
	if !ok {
		return
	}

	if _, err := s.backend.PromoteLearner(r.Context(), &clusterpb.PromoteLearnerRequest{NodeId: nodeID}); err != nil {
		handleClusterError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
