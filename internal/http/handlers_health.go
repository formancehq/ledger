package http

import (
	"errors"
	"net/http"
)

// HealthData represents the response for health check
type HealthData struct {
	Status string `json:"status"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !s.cluster.IsHealthy() {
		writeErrorResponse(w, http.StatusServiceUnavailable, "UNHEALTHY", errors.New("node is not connected to the cluster"))
		return
	}

	writeOK(w, HealthData{
		Status: "ok",
	})
}

