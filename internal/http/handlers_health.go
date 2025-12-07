package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
)

// HealthData represents the response for health check
type HealthData struct {
	Status string `json:"status"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.WriteErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	if !s.cluster.IsHealthy() {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "UNHEALTHY", errors.New("node is not connected to the cluster"))
		return
	}

	response := HealthData{
		Status: "ok",
	}

	api.Ok(w, response)
}

