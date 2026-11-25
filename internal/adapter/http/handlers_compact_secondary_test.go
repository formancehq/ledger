package http

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func TestHandleCompactSecondary(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().CompactSecondary(gomock.Any(), gomock.Any()).Return(&clusterpb.CompactSecondaryResponse{
		DurationMs: 1500, SizeBeforeBytes: 4096, SizeAfterBytes: 1024,
	}, nil)
	w := serveCluster(t, backend, http.MethodPost, "/v3/_/cluster/compactions/secondary", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.JSONEq(t, `{"data":{"durationMs":1500,"sizeBeforeBytes":4096,"sizeAfterBytes":1024}}`, w.Body.String())
}
