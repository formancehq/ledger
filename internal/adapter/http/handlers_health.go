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
	if !s.backend.IsHealthy() {
		writeErrorResponse(w, http.StatusServiceUnavailable, "UNHEALTHY", errors.New("node is not connected to the cluster"))
		return
	}

	writeOK(w, HealthData{
		Status: "ok",
	})
}

// handleLivez is a liveness probe: returns 200 as long as the process is alive.
func (s *Server) handleLivez(w http.ResponseWriter, _ *http.Request) {
	writeOK(w, HealthData{Status: "ok"})
}

// handleReadyz is a readiness probe: returns 200 only when the node is part of
// a healthy cluster (Raft state healthy, leader elected, disk/clock OK).
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if !s.backend.IsReady() {
		writeErrorResponse(w, http.StatusServiceUnavailable, "NOT_READY", errors.New("node is not ready to serve traffic"))
		return
	}

	writeOK(w, HealthData{Status: "ok"})
}
