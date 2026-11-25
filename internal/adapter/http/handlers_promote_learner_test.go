package http

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func TestHandlePromoteLearner(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().PromoteLearner(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *clusterpb.PromoteLearnerRequest) (*clusterpb.PromoteLearnerResponse, error) {
		require.Equal(t, uint64(18446744073709551615), req.GetNodeId())

		return &clusterpb.PromoteLearnerResponse{}, nil
	})
	w := serveCluster(t, backend, http.MethodPost, "/v3/_/cluster/learners/18446744073709551615/promote", "")
	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
	require.Empty(t, w.Body.String())
}
