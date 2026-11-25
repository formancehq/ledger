package http

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func TestHandleRemoveNode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		query string
		force bool
	}{
		{"", false},
		{"?force=false", false},
		{"?force=true", true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().RemoveNode(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *clusterpb.RemoveNodeRequest) (*clusterpb.RemoveNodeResponse, error) {
				require.Equal(t, uint64(18446744073709551615), req.GetNodeId())
				require.Equal(t, tc.force, req.GetForce())

				return &clusterpb.RemoveNodeResponse{}, nil
			})
			w := serveCluster(t, backend, http.MethodDelete, "/v3/_/cluster/nodes/18446744073709551615"+tc.query, "")
			require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
		})
	}
}

func TestHandleRemoveNode_InvalidQuery(t *testing.T) {
	t.Parallel()
	for _, query := range []string{"?force=", "?force=1", "?force=true&force=true", "?other=1"} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()
			w := serveCluster(t, NewMockBackend(gomock.NewController(t)), http.MethodDelete, "/v3/_/cluster/nodes/2"+query, "")
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		})
	}
}
