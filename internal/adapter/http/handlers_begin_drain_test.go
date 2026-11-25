package http

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func TestHandleBeginDrain(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().BeginDrain(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *clusterpb.BeginDrainRequest) (*clusterpb.BeginDrainResponse, error) {
		require.Equal(t, "rollout-7", req.GetEpoch())

		return &clusterpb.BeginDrainResponse{CutoffIndex: 18446744073709551615, Members: []uint64{1, 18446744073709551615}}, nil
	})
	w := serveCluster(t, backend, http.MethodPost, "/v3/_/cluster/drain", `{"epoch":"rollout-7"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.JSONEq(t, `{"data":{"cutoffIndex":18446744073709551615,"members":[1,18446744073709551615]}}`, w.Body.String())
}

// The epoch length contract belongs to the ClusterService: an empty epoch
// reaches it and its InvalidArgument answers 400.
func TestHandleBeginDrain_EmptyEpochReachesService(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().BeginDrain(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *clusterpb.BeginDrainRequest) (*clusterpb.BeginDrainResponse, error) {
		require.Empty(t, req.GetEpoch())

		return nil, status.Error(codes.InvalidArgument, "drain epoch must contain 1 to 128 bytes")
	})
	w := serveCluster(t, backend, http.MethodPost, "/v3/_/cluster/drain", `{}`)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	require.Equal(t, "drain epoch must contain 1 to 128 bytes", decodeErrorResponse(t, w).ErrorMessage)
}
