package http

import (
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
)

func (s *Server) handleClusterState(w http.ResponseWriter, r *http.Request) {
	clusterState, err := s.cluster.GetClusterState(r.Context())
	if err != nil {
		api.WriteErrorResponse(w, http.StatusInternalServerError, "CLUSTER_STATE_ERROR", err)
		return
	}

	api.Ok(w, clusterState)
}

