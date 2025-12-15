package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
)

// handleGetBucketRaftState handles GET /buckets/{bucketName}/raft/state to get the Raft cluster state of a bucket
// Query parameter 'local' can be used to get the local state without checking if we're the leader
func (s *Server) handleGetBucketRaftState(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	if bucketName == "" {
		api.BadRequest(w, "bucket name is required", errors.New("bucket name is required"))
		return
	}

	// Check if 'local' query parameter is present
	local := r.URL.Query().Get("local") == "true"

	var bucketCluster service.BucketCluster
	var err error

	if local {
		// Get local bucket cluster directly without checking leader
		bucketCluster, err = s.cluster.GetBucketClusterLocal(r.Context(), bucketName)
	} else {
		// Get bucket cluster (may redirect to leader)
		bucketCluster, err = s.cluster.GetBucketCluster(r.Context(), bucketName)
	}

	if err != nil {
		s.logger.WithFields(map[string]any{"bucket": bucketName, "local": local, "error": err}).Errorf("Failed to get bucket cluster")
		handleError(w, r, err)
		return
	}

	state, err := bucketCluster.GetClusterState(r.Context())
	if err != nil {
		handleError(w, r, err)
		return
	}

	api.Ok(w, state)
}
