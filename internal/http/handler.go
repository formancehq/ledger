package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/http/bulking"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
)

// NewHandler creates a new HTTP handler (router) for the ledger service
func NewHandler(logger logging.Logger, cluster service.MasterCluster) http.Handler {
	r := chi.NewRouter()

	// Apply middlewares
	r.Use(
		middleware.RequestID,
		middleware.RealIP,
		otelhttp.NewMiddleware("ledger-http-server",
			otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
		),
		middleware.RequestLogger(&chiLogFormatter{
			logger: logger,
		}),
		middleware.Recoverer,
	)

	// Create bulker factory
	bulkerFactory := bulking.NewDefaultBulkerFactory()

	// Create bulk handler factories
	bulkHandlerFactories := map[string]bulking.HandlerFactory{
		// todo: set limit and add bulk streaming support
		"application/json": bulking.NewJSONBulkHandlerFactory(0), // 0 = no limit
	}

	// Create server instance for handlers
	server := &Server{
		logger:               logger,
		cluster:              cluster,
		bulkerFactory:        bulkerFactory,
		bulkHandlerFactories: bulkHandlerFactories,
	}

	// Register routes function - can be called with different prefixes
	registerRoutes := func(r chi.Router) {
		// Register pprof routes (for profiling and debugging)
		// These routes are available at /debug/pprof/*
		r.Route("/debug/pprof", func(r chi.Router) {
			r.Get("/", pprof.Index)
			r.Get("/cmdline", pprof.Cmdline)
			r.Get("/profile", pprof.Profile)
			r.Get("/symbol", pprof.Symbol)
			r.Get("/trace", pprof.Trace)
			r.Handle("/allocs", pprof.Handler("allocs"))
			r.Handle("/block", pprof.Handler("block"))
			r.Handle("/goroutine", pprof.Handler("goroutine"))
			r.Handle("/heap", pprof.Handler("heap"))
			r.Handle("/mutex", pprof.Handler("mutex"))
			r.Handle("/threadcreate", pprof.Handler("threadcreate"))
		})

		r.With(contentTypeMiddleware).Group(func(r chi.Router) {
			// Register known routes (specific routes first)
			r.Post("/snapshot", server.handleSnapshot)
			r.Get("/health", server.handleHealth)
			r.Get("/cluster/state", server.handleClusterState)

			r.Post("/{ledgerName}", server.handleCreateLedger)                                    // POST /{ledgerName}
			r.Get("/{ledgerName}", server.handleGetLedger)                                        // GET /{ledgerName}
			r.Delete("/{ledgerName}", server.handleDeleteLedger)                                  // DELETE /{ledgerName}
			r.Get("/{ledgerName}/raft/state", server.handleGetLedgerRaftState)                    // GET /{ledgerName}/raft/state
			r.Post("/{ledgerName}/transactions", server.handleCreateTransaction)                  // POST /{ledgerName}/transactions
			r.Post("/{ledgerName}/accounts/{address}/metadata", server.handleSaveAccountMetadata) // POST /{ledgerName}/accounts/{address}/metadata
			r.Post("/{ledgerName}/bulk", server.handleBulk)                                       // POST /{ledgerName}/bulk
			r.Post("/{ledgerName}/_bulk", server.handleBulk)                                      // For compat
			r.Get("/", server.handleListAllLedgers)                                               // GET / - must be last
		})
	}

	// Register routes without prefix (backward compatibility)
	registerRoutes(r)

	// Register routes with /v2 prefix (optional)
	r.Route("/v2", registerRoutes)

	return r
}

// contentTypeMiddleware sets Content-Type header for JSON responses
// For 204 No Content responses, no Content-Type header is set
func contentTypeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Wrap the response writer to intercept WriteHeader calls
		rw := &contentTypeResponseWriter{ResponseWriter: w}
		next.ServeHTTP(rw, r)
	})
}

// contentTypeResponseWriter wraps http.ResponseWriter to set Content-Type automatically
type contentTypeResponseWriter struct {
	http.ResponseWriter
	statusCode     int
	wroteHeader    bool
	contentTypeSet bool
}

func (rw *contentTypeResponseWriter) WriteHeader(code int) {
	if !rw.wroteHeader {
		rw.statusCode = code
		rw.wroteHeader = true

		// Set Content-Type to application/json if:
		// 1. Status code is not 204 No Content
		// 2. Content-Type hasn't been explicitly set
		if code != http.StatusNoContent && !rw.contentTypeSet {
			rw.ResponseWriter.Header().Set("Content-Type", "application/json")
		}

		rw.ResponseWriter.WriteHeader(code)
	}
}

func (rw *contentTypeResponseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}
	return rw.ResponseWriter.Write(b)
}

func (rw *contentTypeResponseWriter) Header() http.Header {
	// Track if Content-Type is explicitly set
	header := rw.ResponseWriter.Header()
	if header.Get("Content-Type") != "" {
		rw.contentTypeSet = true
	}
	return header
}

type chiLogFormatter struct {
	logger logging.Logger
}

func (c chiLogFormatter) NewLogEntry(r *http.Request) middleware.LogEntry {
	fields := map[string]any{}
	if span := trace.SpanFromContext(r.Context()); span.SpanContext().IsValid() {
		fields["trace_id"] = span.SpanContext().TraceID()
		fields["span_id"] = span.SpanContext().SpanID()
	}
	return chiLogEntry{
		logger: c.logger.WithFields(fields),
		ctx:    r.Context(),
	}
}

var _ middleware.LogFormatter = (*chiLogFormatter)(nil)

type chiLogEntry struct {
	logger logging.Logger
	ctx    context.Context
}

func (c chiLogEntry) Write(status, bytes int, _ http.Header, elapsed stdtime.Duration, extra interface{}) {
	fields := map[string]any{
		"status":  status,
		"bytes":   bytes,
		"elapsed": elapsed,
	}
	if extra != nil {
		fields["extra"] = extra
	}
	c.logger.WithFields(fields).Info("HTTP request completed")
}

func (c chiLogEntry) Panic(v interface{}, stack []byte) {
	c.logger.Errorf("Panicked: %v", v)
	_, _ = c.logger.Writer().Write(stack)
	if span := trace.SpanFromContext(c.ctx); span.SpanContext().IsValid() {
		span.RecordError(fmt.Errorf("%s", v), trace.WithStackTrace(true))
	}
}
