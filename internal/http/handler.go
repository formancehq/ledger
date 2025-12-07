package http

import (
	"net/http"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/logging"
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

	// Create server instance for handlers
	server := &Server{
		logger:  logger,
		cluster: cluster,
	}

	// Register known routes (specific routes first)
	r.Post("/snapshot", server.handleSnapshot)
	r.Get("/health", server.handleHealth)
	r.Get("/cluster/state", server.handleClusterState)

	// Register bucket routes
	r.Get("/buckets", server.handleListBuckets) // GET /buckets
	r.Route("/buckets/{bucketName}", func(r chi.Router) {
		r.Get("/", server.handleGetBucket)                     // GET /buckets/{bucketName}
		r.Post("/", server.handleCreateBucket)                 // POST /buckets/{bucketName}
		r.Delete("/", server.handleDeleteBucket)               // DELETE /buckets/{bucketName}
		r.Post("/snapshot", server.handleCreateBucketSnapshot) // POST /buckets/{bucketName}/snapshot
	})

	// Register ledger routes at root (without /ledgers prefix)
	// Note: Routes with parameters must come before the root route
	r.Post("/{ledgerName}", server.handleCreateLedger)                   // POST /{ledgerName}
	r.Get("/{ledgerName}", server.handleGetLedger)                       // GET /{ledgerName}
	r.Post("/{ledgerName}/transactions", server.handleCreateTransaction) // POST /{ledgerName}/transactions
	r.Get("/", server.handleListAllLedgers)                              // GET / (cross-bucket) - must be last

	// Wrap handler with OpenTelemetry instrumentation
	handler := otelhttp.NewHandler(r, "ledger-http-server",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	)

	return handler
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

			// Skip logging for health check requests
			if r.URL.Path == "/health" {
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
