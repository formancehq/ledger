package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/go-chi/chi/v5"
)

// handleGetBucket handles GET /buckets/{bucketName} to get a bucket with its Raft state
func (s *Server) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	if bucketName == "" {
		api.BadRequest(w, "bucket name is required", errors.New("bucket name is required"))
		return
	}

	bucket, err := s.cluster.GetBucket(r.Context(), bucketName)
	if err != nil {
		s.logger.WithFields(map[string]any{"bucket": bucketName, "error": err}).Errorf("Failed to get bucket")
		handleError(w, r, err)
		return
	}

	state, err := bucket.GetClusterState(r.Context())
	if err != nil {
		handleError(w, r, err)
		return
	}

	// BucketWithRaftState represents a bucket with its Raft cluster state
	type BucketWithRaftState struct {
		ledger.BucketInfo
		RaftState *ledger.ClusterState `json:"raftState"`
	}

	api.Ok(w, BucketWithRaftState{
		BucketInfo: bucket.Info(),
		RaftState:  state,
	})
}
