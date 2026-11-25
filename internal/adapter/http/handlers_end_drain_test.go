package http

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func TestHandleEndDrain(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().EndDrain(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *clusterpb.EndDrainRequest) (*clusterpb.EndDrainResponse, error) {
		require.Equal(t, "rollout-7", req.GetEpoch())

		return &clusterpb.EndDrainResponse{ReleaseIndex: 12, Members: []uint64{1, 2}}, nil
	})
	w := serveCluster(t, backend, http.MethodPost, "/v3/_/cluster/drain/end", `{"epoch":"rollout-7"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.JSONEq(t, `{"data":{"releaseIndex":12,"members":[1,2]}}`, w.Body.String())
}
