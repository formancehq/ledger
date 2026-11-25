package http

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	ggrpc "google.golang.org/grpc"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source restore_handler.go -destination restore_backend_generated_test.go -typed -package http RestoreBackend

// RestoreBackend is the gRPC RestoreService. HTTP routes call it in process so
// admission, state checks and shutdown behave as they do over gRPC.
type RestoreBackend interface {
	StartDownloadBackup(ctx context.Context, req *restorepb.StartDownloadBackupRequest) (*restorepb.StartDownloadBackupResponse, error)
	GetDownloadStatus(ctx context.Context, req *restorepb.GetDownloadStatusRequest) (*restorepb.GetDownloadStatusResponse, error)
	CancelDownload(ctx context.Context, req *restorepb.CancelDownloadRequest) (*restorepb.CancelDownloadResponse, error)
	ValidateRestore(req *restorepb.ValidateRestoreRequest, stream ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error
	PreviewRestore(ctx context.Context, req *restorepb.PreviewRestoreRequest) (*restorepb.PreviewRestoreResponse, error)
	FinalizeRestore(ctx context.Context, req *restorepb.FinalizeRestoreRequest) (*restorepb.FinalizeRestoreResponse, error)
}

var _ RestoreBackend = (restorepb.RestoreServiceServer)(nil)

type restoreServer struct {
	backend RestoreBackend
}

// NewRestoreHandler serves the restore-mode HTTP API. Like the restore gRPC
// service, it has no authentication; restore mode serves it only on a
// loopback listen address.
func NewRestoreHandler(logger logging.Logger, backend RestoreBackend) http.Handler {
	r := newRestoreRouter(logger)
	server := &restoreServer{backend: backend}

	r.Route(APIVersionPrefix+"/_/restore", func(r chi.Router) {
		r.Post("/downloads", knownQuery(server.handleStartRestoreDownload))
		r.Get("/downloads/{jobId}", knownQuery(server.handleGetRestoreDownload))
		r.Delete("/downloads/{jobId}", knownQuery(server.handleCancelRestoreDownload))
		r.Post("/validate", knownQuery(server.handleValidateRestore))
		r.Get("/preview", knownQuery(server.handlePreviewRestore))
		r.Post("/finalize", knownQuery(server.handleFinalizeRestore))
	})

	return r
}

// NewRestoreHealthHandler serves only the restore-mode health endpoint.
func NewRestoreHealthHandler(logger logging.Logger) http.Handler {
	return newRestoreRouter(logger)
}

func newRestoreRouter(logger logging.Logger) chi.Router {
	r := chi.NewRouter()

	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Errorf("method %s not allowed on %s", r.Method, r.URL.Path))
	})
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeErrorResponse(w, http.StatusNotFound, "ROUTE_NOT_FOUND",
			fmt.Errorf("no route matches %s %s", r.Method, r.URL.Path))
	})

	r.Use(
		middleware.RequestID,
		loggerMiddleware(logger),
		middleware.RequestLogger(&chiLogFormatter{logger: logger}),
		jsonRecoverer,
		maxBodySizeMiddleware(defaultMaxBodySize),
	)

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"restore_mode"}`))
	})

	return r
}
