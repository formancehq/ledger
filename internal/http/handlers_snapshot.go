package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

// SnapshotData represents the response for snapshot operations
type SnapshotData struct {
	Message string `json:"message"`
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Check if we are the leader
	if s.isLeader() {
		// We are the leader, call directly
		if err := s.cluster.Snapshot(); err != nil {
			s.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to create snapshot")
			api.WriteErrorResponse(w, http.StatusInternalServerError, "SNAPSHOT_FAILED", err)
			return
		}

		response := SnapshotData{
			Message: "Snapshot created successfully",
		}
		api.Ok(w, response)
		return
	}

	// We are a follower, forward via gRPC
	client := s.cluster.GetLeaderGRPCClient()
	if client == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("not connected to leader gRPC server"))
		return
	}

	// Call leader via gRPC
	resp, err := client.CreateClusterSnapshot(r.Context(), &service.CreateClusterSnapshotRequest{})
	if err != nil {
		s.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to create snapshot via gRPC")
		api.WriteErrorResponse(w, http.StatusInternalServerError, "SNAPSHOT_FAILED", err)
		return
	}

	response := SnapshotData{
		Message: resp.Message,
	}
	api.Ok(w, response)
}

