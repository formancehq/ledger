package http

import (
	"net/http"
)

// SnapshotData represents the response for snapshot operations
type SnapshotData struct {
	Message string `json:"message"`
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {

	if err := s.cluster.Snapshot(r.Context()); err != nil {
		handleError(w, r, err)
		return
	}

	writeOK(w, SnapshotData{
		Message: "Snapshot created successfully.",
	})
}

