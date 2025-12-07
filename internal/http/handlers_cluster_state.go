package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"go.uber.org/zap"
)

func (s *Server) handleClusterState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.WriteErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	clusterState, err := s.cluster.GetClusterState()
	if err != nil {
		s.logger.Error("Failed to get cluster state", zap.Error(err))
		api.WriteErrorResponse(w, http.StatusInternalServerError, "CLUSTER_STATE_ERROR", err)
		return
	}

	api.Ok(w, clusterState)
}

