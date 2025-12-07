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
	if !s.cluster.IsHealthy() {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "UNHEALTHY", errors.New("node is not connected to the cluster"))
		return
	}

	api.Ok(w, HealthData{
		Status: "ok",
	})
}

