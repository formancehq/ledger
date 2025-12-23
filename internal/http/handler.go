package http

import (
	"net/http"
	"net/http/pprof"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/http/bulking"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// NewHandler creates a new HTTP handler (router) for the ledger service
func NewHandler(logger logging.Logger, cluster service.MasterCluster) http.Handler {
	r := chi.NewRouter()

	// Apply middlewares
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(loggingMiddleware(logger))

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

	// Wrap handler with OpenTelemetry instrumentation
	handler := otelhttp.NewHandler(r, "ledger-http-server",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	)

	return handler
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

// loggingMiddleware logs HTTP requests (chi middleware)
func loggingMiddleware(logger logging.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := stdtime.Now()

			// Create a response writer wrapper to capture status code
			rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			// Call next handler
			next.ServeHTTP(rw, r)

			// Skip logging for health check and pprof requests
			if r.URL.Path == "/health" || r.URL.Path == "/v2/health" {
				return
			}
			// Skip pprof routes
			if len(r.URL.Path) >= 12 && r.URL.Path[:12] == "/debug/pprof" {
				return
			}
			if len(r.URL.Path) >= 15 && r.URL.Path[:15] == "/v2/debug/pprof" {
				return
			}

			// Log the request
			duration := stdtime.Since(start)
			logger.WithFields(map[string]any{
				"method":      r.Method,
				"path":        r.URL.Path,
				"query":       r.URL.RawQuery,
				"status":      rw.statusCode,
				"duration":    duration,
				"remote_addr": r.RemoteAddr,
				"user_agent":  r.UserAgent(),
			}).Infof("HTTP request")
		})
	}
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
