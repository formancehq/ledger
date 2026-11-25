package http

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

func TestHandleCancelRestoreDownload(t *testing.T) {
	t.Parallel()
	backend := NewMockRestoreBackend(gomock.NewController(t))
	backend.EXPECT().CancelDownload(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *restorepb.CancelDownloadRequest) (*restorepb.CancelDownloadResponse, error) {
		require.Equal(t, "job-1", req.GetJobId())

		return &restorepb.CancelDownloadResponse{}, nil
	})
	w := serveRestore(t, backend, http.MethodDelete, "/v3/_/restore/downloads/job-1", "")
	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
	require.Empty(t, w.Body.String())
}

func TestHandleCancelRestoreDownload_UnknownJob(t *testing.T) {
	t.Parallel()
	backend := NewMockRestoreBackend(gomock.NewController(t))
	backend.EXPECT().CancelDownload(gomock.Any(), gomock.Any()).Return(nil, status.Error(codes.NotFound, "unknown job id"))
	w := serveRestore(t, backend, http.MethodDelete, "/v3/_/restore/downloads/other", "")
	require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
}
