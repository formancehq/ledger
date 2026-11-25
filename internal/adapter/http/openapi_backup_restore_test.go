package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

const openAPIS3Destination = `{"bucketId":"prod","storage":{"s3":{"bucket":"b","region":"r","endpoint":"http://minio:9000","accessKeyId":"k","secretAccessKey":"s"}}}`

// TestOpenAPI_Backup validates the backup route's requests and responses
// against openapi.yml.
func TestOpenAPI_Backup(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)

	for _, tc := range []struct {
		name, body string
		expect     func(*MockBackend)
		status     int
	}{
		{"s3", openAPIS3Destination, func(b *MockBackend) {
			b.EXPECT().Backup(gomock.Any(), gomock.Any()).Return(&clusterpb.BackupResponse{
				FilesUploaded: 1, TotalFiles: 2, DurationMs: 3, LastLogSequence: 18446744073709551615, LastAppliedIndex: 4,
			}, nil)
		}, http.StatusOK},
		{"azure", `{"storage":{"azure":{"accountName":"a","accountKey":"k","container":"c","endpoint":"http://azurite:10000"}}}`, func(b *MockBackend) {
			b.EXPECT().Backup(gomock.Any(), gomock.Any()).Return(&clusterpb.BackupResponse{}, nil)
		}, http.StatusOK},
		{"in progress", openAPIS3Destination, func(b *MockBackend) {
			b.EXPECT().Backup(gomock.Any(), gomock.Any()).Return(nil, state.ErrBackupInProgress)
		}, http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			newRequest := func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "http://localhost:9000/v3/_/cluster/backups", strings.NewReader(tc.body))
				r.Header.Set("Content-Type", "application/json")

				return r
			}
			validateOpenAPIRequest(t, doc, newRequest())

			backend := NewMockBackend(gomock.NewController(t))
			tc.expect(backend)
			r := newRequest()
			w := httptest.NewRecorder()
			NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil).ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			validateOpenAPIResponse(t, doc, r, w)
		})
	}
}

// TestOpenAPI_Restore validates each restore-mode request and its success and
// documented error responses against openapi.yml.
func TestOpenAPI_Restore(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)

	for _, tc := range []struct {
		name, method, path, body string
		expect                   func(*MockRestoreBackend)
		status                   int
	}{
		{"start", http.MethodPost, "/v3/_/restore/downloads", openAPIS3Destination, func(b *MockRestoreBackend) {
			b.EXPECT().StartDownloadBackup(gomock.Any(), gomock.Any()).Return(&restorepb.StartDownloadBackupResponse{JobId: "job"}, nil)
		}, http.StatusAccepted},
		{"start busy", http.MethodPost, "/v3/_/restore/downloads", openAPIS3Destination, func(b *MockRestoreBackend) {
			b.EXPECT().StartDownloadBackup(gomock.Any(), gomock.Any()).Return(nil, status.Error(codes.FailedPrecondition, "another download is already in progress"))
		}, http.StatusConflict},
		{"status", http.MethodGet, "/v3/_/restore/downloads/job", "", func(b *MockRestoreBackend) {
			b.EXPECT().GetDownloadStatus(gomock.Any(), gomock.Any()).Return(&restorepb.GetDownloadStatusResponse{
				State: restorepb.DownloadState_DOWNLOAD_STATE_RUNNING, TotalBytes: 18446744073709551615, StartedAtUnix: 1,
			}, nil)
		}, http.StatusOK},
		{"status unknown", http.MethodGet, "/v3/_/restore/downloads/other", "", func(b *MockRestoreBackend) {
			b.EXPECT().GetDownloadStatus(gomock.Any(), gomock.Any()).Return(nil, status.Error(codes.NotFound, "unknown job id"))
		}, http.StatusNotFound},
		{"cancel", http.MethodDelete, "/v3/_/restore/downloads/job", "", func(b *MockRestoreBackend) {
			b.EXPECT().CancelDownload(gomock.Any(), gomock.Any()).Return(&restorepb.CancelDownloadResponse{}, nil)
		}, http.StatusNoContent},
		{"validate not staged", http.MethodPost, "/v3/_/restore/validate", "{}", func(b *MockRestoreBackend) {
			b.EXPECT().ValidateRestore(gomock.Any(), gomock.Any()).Return(status.Error(codes.FailedPrecondition, "no backup downloaded; download a backup first"))
		}, http.StatusConflict},
		{"preview", http.MethodGet, "/v3/_/restore/preview", "", func(b *MockRestoreBackend) {
			b.EXPECT().PreviewRestore(gomock.Any(), gomock.Any()).Return(&restorepb.PreviewRestoreResponse{LastAppliedIndex: 1}, nil)
		}, http.StatusOK},
		{"finalize", http.MethodPost, "/v3/_/restore/finalize", "{}", func(b *MockRestoreBackend) {
			b.EXPECT().FinalizeRestore(gomock.Any(), gomock.Any()).Return(&restorepb.FinalizeRestoreResponse{Message: "done"}, nil)
		}, http.StatusOK},
		{"finalize shutting down", http.MethodPost, "/v3/_/restore/finalize", "{}", func(b *MockRestoreBackend) {
			b.EXPECT().FinalizeRestore(gomock.Any(), gomock.Any()).Return(nil, status.Error(codes.Unavailable, "restore service is shutting down"))
		}, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			newRequest := func() *http.Request {
				r := httptest.NewRequest(tc.method, "http://localhost:9000"+tc.path, strings.NewReader(tc.body))
				r.Header.Set("Content-Type", "application/json")

				return r
			}
			validateOpenAPIRequest(t, doc, newRequest())

			backend := NewMockRestoreBackend(gomock.NewController(t))
			tc.expect(backend)
			r := newRequest()
			w := httptest.NewRecorder()
			NewRestoreHandler(logging.Testing(), backend).ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			validateOpenAPIResponse(t, doc, r, w)
		})
	}
}

// TestOpenAPI_ValidateRestoreStream checks every NDJSON line against the
// documented line schema.
func TestOpenAPI_ValidateRestoreStream(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "http://localhost:9000/v3/_/restore/validate", strings.NewReader("{}"))
	r.Header.Set("Content-Type", "application/json")
	validateOpenAPIRequest(t, doc, r)

	backend := NewMockRestoreBackend(gomock.NewController(t))
	backend.EXPECT().ValidateRestore(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ *restorepb.ValidateRestoreRequest, stream ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error {
			require.NoError(t, stream.Send(progressEvent(1, 18446744073709551615)))
			require.NoError(t, stream.Send(findingEvent("bad")))

			return status.Error(codes.Internal, "checker failed")
		})
	w := httptest.NewRecorder()
	NewRestoreHandler(logging.Testing(), backend).ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	schema := doc.Components.Schemas["ValidateRestoreLine"].Value
	lines := strings.Split(strings.TrimSpace(w.Body.String()), "\n")
	require.Len(t, lines, 3)
	for _, line := range lines {
		var value any
		require.NoError(t, json.Unmarshal([]byte(line), &value))
		require.NoError(t, schema.VisitJSON(value), line)
	}
}

// TestOpenAPI_BodylessRestorePostsRequireJSON checks that the spec and the
// handler both refuse a validate or finalize POST without a JSON body.
func TestOpenAPI_BodylessRestorePostsRequireJSON(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	router, err := legacy.NewRouter(doc, openapi3.AllowExtraSiblingFields("description"))
	require.NoError(t, err)

	for _, path := range []string{"/v3/_/restore/validate", "/v3/_/restore/finalize"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			newRequest := func() *http.Request {
				return httptest.NewRequest(http.MethodPost, "http://localhost:9000"+path, nil)
			}
			route, params, err := router.FindRoute(newRequest())
			require.NoError(t, err)
			err = openapi3filter.ValidateRequest(context.Background(), &openapi3filter.RequestValidationInput{
				Request: newRequest(), PathParams: params, Route: route,
				Options: &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc},
			})
			var reqErr *openapi3filter.RequestError
			require.ErrorAs(t, err, &reqErr)
			require.NotNil(t, reqErr.RequestBody)

			w := httptest.NewRecorder()
			NewRestoreHandler(logging.Testing(), NewMockRestoreBackend(gomock.NewController(t))).ServeHTTP(w, newRequest())
			require.Equal(t, http.StatusUnsupportedMediaType, w.Code, w.Body.String())
		})
	}
}
