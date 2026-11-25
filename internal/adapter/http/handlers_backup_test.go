package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func TestHandleBackup(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Backup(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *clusterpb.BackupRequest) (*clusterpb.BackupResponse, error) {
		require.Equal(t, "prod", req.GetBucketId())
		require.Empty(t, req.GetBasePath())
		s3 := req.GetStorage().GetS3()
		require.NotNil(t, s3)
		require.Equal(t, "backups", s3.GetBucket())
		require.Equal(t, "eu-west-1", s3.GetRegion())
		require.Equal(t, "http://minio:9000", s3.GetEndpoint())
		require.Equal(t, "key", s3.GetAccessKeyId())
		require.Equal(t, "secret", s3.GetSecretAccessKey())

		return &clusterpb.BackupResponse{
			FilesUploaded: 3, FilesDeleted: 1, TotalFiles: 10, DurationMs: 1200,
			LastLogSequence: 18446744073709551615, LastAuditSequence: 7, LastAppliedIndex: 42, OrphansDeleted: 2,
		}, nil
	})
	w := serveCluster(t, backend, http.MethodPost, "/v3/_/cluster/backups", `{
		"bucketId": "prod",
		"storage": {"s3": {"bucket": "backups", "region": "eu-west-1", "endpoint": "http://minio:9000", "accessKeyId": "key", "secretAccessKey": "secret"}}
	}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.JSONEq(t, `{"data":{"filesUploaded":3,"filesDeleted":1,"totalFiles":10,"durationMs":1200,
		"lastLogSequence":18446744073709551615,"lastAuditSequence":7,"lastAppliedIndex":42,"orphansDeleted":2}}`, w.Body.String())
}

func TestHandleBackup_Azure(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Backup(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *clusterpb.BackupRequest) (*clusterpb.BackupResponse, error) {
		require.Empty(t, req.GetBucketId())
		azure := req.GetStorage().GetAzure()
		require.NotNil(t, azure)
		require.Equal(t, "acct", azure.GetAccountName())
		require.Equal(t, "k", azure.GetAccountKey())
		require.Equal(t, "c", azure.GetContainer())
		require.Equal(t, "http://azurite:10000", azure.GetEndpoint())

		return &clusterpb.BackupResponse{}, nil
	})
	w := serveCluster(t, backend, http.MethodPost, "/v3/_/cluster/backups",
		`{"storage":{"azure":{"accountName":"acct","accountKey":"k","container":"c","endpoint":"http://azurite:10000"}}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
}

func TestHandleBackup_InvalidInput(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"empty body":       ``,
		"missing storage":  `{"bucketId":"prod"}`,
		"empty storage":    `{"storage":{}}`,
		"both providers":   `{"storage":{"s3":{"bucket":"b"},"azure":{"accountName":"a","container":"c"}}}`,
		"reserved path":    `{"basePath":"x","storage":{"s3":{"bucket":"b"}}}`,
		"unknown provider": `{"storage":{"gcs":{"bucket":"b"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := serveCluster(t, NewMockBackend(gomock.NewController(t)), http.MethodPost, "/v3/_/cluster/backups", body)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			require.Equal(t, "INVALID_REQUEST", decodeErrorResponse(t, w).ErrorCode)
		})
	}
}

func TestHandleBackup_Errors(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err    error
		status int
		code   string
	}{
		"in progress on leader": {
			err:    fmt.Errorf("backup already in progress for this destination: %w", state.ErrBackupInProgress),
			status: http.StatusConflict, code: "FAILED_PRECONDITION",
		},
		"in progress forwarded": {
			err:    status.Error(codes.FailedPrecondition, "backup already in progress for this destination"),
			status: http.StatusConflict, code: "FAILED_PRECONDITION",
		},
		"storage rejected": {
			err:    status.Error(codes.InvalidArgument, "backup storage provider is required (s3 or azure)"),
			status: http.StatusBadRequest, code: "INVALID_REQUEST",
		},
		"upload failure": {
			err:    fmt.Errorf("backup failed: %w", errors.New("s3: access denied")),
			status: http.StatusInternalServerError, code: "INTERNAL_ERROR",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().Backup(gomock.Any(), gomock.Any()).Return(nil, tc.err)
			w := serveCluster(t, backend, http.MethodPost, "/v3/_/cluster/backups", `{"storage":{"s3":{"bucket":"b"}}}`)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			require.Equal(t, tc.code, decodeErrorResponse(t, w).ErrorCode)
		})
	}
}
