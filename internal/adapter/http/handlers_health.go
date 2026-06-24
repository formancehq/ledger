package http

import (
	"errors"
	"net/http"
	"strings"
)

// HealthData represents the response for health check.
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

// handleReadyz is the StatefulSet readiness probe: returns 200 once the local
// Raft loop has started, regardless of whether a leader has been elected. This
// is intentionally permissive so the operator's OrderedReady gate can advance
// during a cold start (where quorum cannot be reached until peer pods are
// scheduled). Use /clusterz for the stricter cluster-availability signal.
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if reasons := s.backend.NotReadyReasons(); len(reasons) > 0 {
		s.logger.Infof("Readiness check failed: %s", strings.Join(reasons, "; "))
		writeErrorResponse(w, http.StatusServiceUnavailable, "NOT_READY", errors.New("node is not ready"))

		return
	}

	writeOK(w, HealthData{Status: "ok"})
}

// handleClusterz is a cluster-availability probe: returns 200 only when the
// node is part of a healthy cluster (Raft state healthy and a leader elected).
// Disk and clock skew gate writes (see admission), not cluster readiness. Use
// this for external monitoring or any client that wants to wait until the node
// can actually serve cluster-dependent traffic.
func (s *Server) handleClusterz(w http.ResponseWriter, _ *http.Request) {
	if reasons := s.backend.NotClusterReadyReasons(); len(reasons) > 0 {
		s.logger.Infof("Cluster readiness check failed: %s", strings.Join(reasons, "; "))
		writeErrorResponse(w, http.StatusServiceUnavailable, "CLUSTER_NOT_READY", errors.New("node is not part of a healthy cluster"))

		return
	}

	writeOK(w, HealthData{Status: "ok"})
}
