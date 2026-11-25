package http

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func TestHandleCompactPrimary(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().CompactPrimary(gomock.Any(), gomock.Any()).Return(&clusterpb.CompactPrimaryResponse{DurationMs: 1500}, nil)
	w := serveCluster(t, backend, http.MethodPost, "/v3/_/cluster/compactions/primary", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.JSONEq(t, `{"data":{"durationMs":1500}}`, w.Body.String())
}
