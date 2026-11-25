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

func TestHandleStartRestoreDownload(t *testing.T) {
	t.Parallel()
	backend := NewMockRestoreBackend(gomock.NewController(t))
	backend.EXPECT().StartDownloadBackup(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *restorepb.StartDownloadBackupRequest) (*restorepb.StartDownloadBackupResponse, error) {
		require.Equal(t, "prod", req.GetBucketId())
		require.Equal(t, "backups", req.GetStorage().GetS3().GetBucket())

		return &restorepb.StartDownloadBackupResponse{JobId: "0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e"}, nil
	})
	w := serveRestore(t, backend, http.MethodPost, "/v3/_/restore/downloads", `{"bucketId":"prod","storage":{"s3":{"bucket":"backups"}}}`)
	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	require.Equal(t, "/v3/_/restore/downloads/0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e", w.Header().Get("Location"))
	require.JSONEq(t, `{"data":{"jobId":"0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e"}}`, w.Body.String())
}

func TestHandleStartRestoreDownload_InvalidInput(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"empty body":      ``,
		"missing storage": `{"bucketId":"prod"}`,
		"s3 without bucket":      `{"storage":{"s3":{"region":"r"}}}`,
		"azure without container": `{"storage":{"azure":{"accountName":"a"}}}`,
		"azure without account":   `{"storage":{"azure":{"container":"c"}}}`,
		"both providers":  `{"storage":{"s3":{"bucket":"b"},"azure":{"accountName":"a","container":"c"}}}`,
		"unknown field":   `{"basePath":"x","storage":{"s3":{"bucket":"b"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := serveRestore(t, NewMockRestoreBackend(gomock.NewController(t)), http.MethodPost, "/v3/_/restore/downloads", body)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			require.Equal(t, "INVALID_REQUEST", decodeErrorResponse(t, w).ErrorCode)
		})
	}
}

func TestHandleStartRestoreDownload_Errors(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err    error
		status int
		code   string
	}{
		"already running":    {status.Error(codes.FailedPrecondition, "another download is already in progress"), http.StatusConflict, "FAILED_PRECONDITION"},
		"already downloaded": {status.Error(codes.FailedPrecondition, "backup already downloaded; finalize or restart to download again"), http.StatusConflict, "FAILED_PRECONDITION"},
		"shutting down":      {status.Error(codes.Unavailable, "restore service is shutting down"), http.StatusServiceUnavailable, "UNAVAILABLE"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockRestoreBackend(gomock.NewController(t))
			backend.EXPECT().StartDownloadBackup(gomock.Any(), gomock.Any()).Return(nil, tc.err)
			w := serveRestore(t, backend, http.MethodPost, "/v3/_/restore/downloads", `{"storage":{"s3":{"bucket":"b"}}}`)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			require.Equal(t, tc.code, decodeErrorResponse(t, w).ErrorCode)
		})
	}
}
