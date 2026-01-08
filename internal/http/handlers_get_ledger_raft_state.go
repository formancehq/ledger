package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleGetLedgerRaftState handles GET /{ledgerName}/raft/state
// Returns the Raft cluster state for the specified ledger
func (s *Server) handleGetLedgerRaftState(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	ledgerCluster, err := s.cluster.GetLedgerClusterLocal(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	clusterState, err := ledgerCluster.GetClusterState(r.Context())
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, "CLUSTER_STATE_ERROR", err)
		return
	}

	writeOK(w, clusterState)
}
