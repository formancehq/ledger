package http

import (
	"errors"
	"net/http"
	"strings"
)

// HealthData represents the response for health check
type HealthData struct {
	Status string `json:"status"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !s.backend.IsHealthy() {
		s.logger.Infof("Health check failed: raft state machine is not healthy")
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
	if reasons := s.backend.NotReadyReasons(); len(reasons) > 0 {
		s.logger.Infof("Readiness check failed: %s", strings.Join(reasons, "; "))
		writeErrorResponse(w, http.StatusServiceUnavailable, "NOT_READY", errors.New("node is not ready to serve traffic"))
		return
	}

	writeOK(w, HealthData{Status: "ok"})
}
