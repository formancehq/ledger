package http

import (
	"net/http"
)

func (s *Server) handleClusterState(w http.ResponseWriter, r *http.Request) {
	clusterState, err := s.backend.GetClusterState(r.Context())
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, "CLUSTER_STATE_ERROR", err)
		return
	}

	writeOK(w, clusterState)
}
