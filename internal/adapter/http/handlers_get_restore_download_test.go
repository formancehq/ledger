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

func TestHandleGetRestoreDownload(t *testing.T) {
	t.Parallel()
	backend := NewMockRestoreBackend(gomock.NewController(t))
	backend.EXPECT().GetDownloadStatus(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *restorepb.GetDownloadStatusRequest) (*restorepb.GetDownloadStatusResponse, error) {
		require.Equal(t, "job-1", req.GetJobId())

		return &restorepb.GetDownloadStatusResponse{
			State: restorepb.DownloadState_DOWNLOAD_STATE_FAILED, FilesDownloaded: 2, TotalFiles: 5,
			BytesDownloaded: 100, TotalBytes: 500, CurrentFile: "000012.sst", ErrorMessage: "s3: access denied",
			StartedAtUnix: 1700000000, FinishedAtUnix: 1700000060,
		}, nil
	})
	w := serveRestore(t, backend, http.MethodGet, "/v3/_/restore/downloads/job-1", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.JSONEq(t, `{"data":{"state":"DOWNLOAD_STATE_FAILED","filesDownloaded":2,"totalFiles":5,
		"bytesDownloaded":100,"totalBytes":500,"currentFile":"000012.sst","errorMessage":"s3: access denied",
		"startedAtUnix":1700000000,"finishedAtUnix":1700000060}}`, w.Body.String())
}

func TestHandleGetRestoreDownload_UnknownJob(t *testing.T) {
	t.Parallel()
	backend := NewMockRestoreBackend(gomock.NewController(t))
	backend.EXPECT().GetDownloadStatus(gomock.Any(), gomock.Any()).Return(nil, status.Error(codes.NotFound, "unknown job id"))
	w := serveRestore(t, backend, http.MethodGet, "/v3/_/restore/downloads/other", "")
	require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
	require.Equal(t, "NOT_FOUND", decodeErrorResponse(t, w).ErrorCode)
}
