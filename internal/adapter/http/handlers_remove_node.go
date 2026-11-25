package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// handleRemoveNode handles DELETE /_/cluster/nodes/{nodeId}.
func (s *Server) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	nodeID, ok := requireClusterNodeID(w, r)
	if !ok {
		return
	}
	force := false
	if values, present := r.URL.Query()["force"]; present {
		if len(values) != 1 || (values[0] != "true" && values[0] != "false") {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("force must be exactly one true or false value"))

			return
		}
		force = values[0] == "true"
	}

	if _, err := s.backend.RemoveNode(r.Context(), &clusterpb.RemoveNodeRequest{NodeId: nodeID, Force: force}); err != nil {
		handleClusterError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
