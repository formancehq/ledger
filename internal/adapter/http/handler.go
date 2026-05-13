package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	stdtime "time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger-v3-poc/internal/adapter/auth"
)

// NewHandler creates a new HTTP handler (router) for the ledger service.
// The authCfg parameter controls JWT authentication with read/write scopes:
// when Enabled is false, requests pass through without authentication.
func NewHandler(logger logging.Logger, backend Backend, authCfg internalauth.AuthConfig) http.Handler {
	r := chi.NewRouter()

	// Granular scope-bound middleware helpers
	requireLedgersRead := internalauth.RequireScope(authCfg, internalauth.ScopeLedgersRead)
	requireLedgersWrite := internalauth.RequireScope(authCfg, internalauth.ScopeLedgersWrite)
	requireTransactionsRead := internalauth.RequireScope(authCfg, internalauth.ScopeTransactionsRead)
	requireTransactionsWrite := internalauth.RequireScope(authCfg, internalauth.ScopeTransactionsWrite)
	requireAccountsRead := internalauth.RequireScope(authCfg, internalauth.ScopeAccountsRead)
	requireMetadataWrite := internalauth.RequireScope(authCfg, internalauth.ScopeMetadataWrite)
	requireQueriesRead := internalauth.RequireScope(authCfg, internalauth.ScopeQueriesRead)
	requireQueriesWrite := internalauth.RequireScope(authCfg, internalauth.ScopeQueriesWrite)

	// Return JSON for 405 Method Not Allowed (Chi default returns text/plain)
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Errorf("method %s not allowed on %s", r.Method, r.URL.Path))
	})

	// Return JSON for 404 Not Found on unmatched routes
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeErrorResponse(w, http.StatusNotFound, "ROUTE_NOT_FOUND",
			fmt.Errorf("no route matches %s %s", r.Method, r.URL.Path))
	})

	// Apply middlewares
	r.Use(
		middleware.RequestID,
		middleware.RealIP,
		otelhttp.NewMiddleware("ledger-http-server"),
		middleware.RequestLogger(&chiLogFormatter{
			logger: logger,
		}),
		jsonRecoverer,
		utf8BodyValidator,
		internalauth.HTTPAuthMiddleware(authCfg),
	)

	// Create server instance for handlers
	server := NewServer(logger, backend, 0) // 0 = no bulk size limit

	// Register routes function - can be called with different prefixes
	registerRoutes := func(r chi.Router) {
		// Unauthenticated: pprof and health
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

		r.With(contentTypeMiddleware, utf8PathParamValidator).Group(func(r chi.Router) {
			r.Get("/health", server.handleHealth)
			r.Get("/livez", server.handleLivez)
			r.Get("/readyz", server.handleReadyz)

			// Ledgers read scope
			r.With(requireLedgersRead).Group(func(r chi.Router) {
				r.Get("/", server.handleListAllLedgers)
				r.Get("/{ledgerName}", server.handleGetLedger)
				r.Get("/{ledgerName}/stats", server.handleGetLedgerStats)
				r.Get("/{ledgerName}/logs", server.handleListLedgerLogs)
				r.Get("/{ledgerName}/numscripts", server.handleListNumscripts)
				r.Get("/{ledgerName}/numscripts/{name}", server.handleGetNumscript)
				r.Get("/{ledgerName}/indexes/{metadataKey}", server.handleInspectIndex)
			})

			// Transactions read scope
			r.With(requireTransactionsRead).Group(func(r chi.Router) {
				r.Get("/{ledgerName}/transactions/{transactionId}", server.handleGetTransaction)
			})

			// Accounts read scope
			r.With(requireAccountsRead).Group(func(r chi.Router) {
				r.Get("/{ledgerName}/accounts", server.handleListAccounts)
				r.Get("/{ledgerName}/accounts/{address}", server.handleGetAccount)
				r.Get("/{ledgerName}/volumes", server.handleAggregateVolumes)
				r.Get("/{ledgerName}/metadata-schema", server.handleGetMetadataSchema)
				r.Get("/{ledgerName}/analyze-accounts", server.handleAnalyzeAccounts)
				r.Get("/{ledgerName}/analyze-transactions", server.handleAnalyzeTransactions)
				r.Get("/{ledgerName}/account-types", server.handleListAccountTypes)
				r.Get("/{ledgerName}/account-types/{typeName}", server.handleGetAccountType)
			})

			// Ledgers write scope
			r.With(requireLedgersWrite).Group(func(r chi.Router) {
				r.Post("/{ledgerName}", server.handleCreateLedger)
				r.Delete("/{ledgerName}", server.handleDeleteLedger)
				r.Post("/{ledgerName}/promote", server.handlePromoteLedger)
				r.Put("/{ledgerName}/numscripts/{name}", server.handleSaveNumscript)
				r.Delete("/{ledgerName}/numscripts/{name}", server.handleDeleteNumscript)
			})

			// Transactions write scope
			r.With(requireTransactionsWrite).Group(func(r chi.Router) {
				r.Post("/{ledgerName}/transactions", server.handleCreateTransaction)
				r.Post("/{ledgerName}/transactions/{transactionId}/revert", server.handleRevertTransaction)
			})

			// Metadata write scope
			r.With(requireMetadataWrite).Group(func(r chi.Router) {
				r.Post("/{ledgerName}/transactions/{transactionId}/metadata", server.handleSaveTransactionMetadata)
				r.Delete("/{ledgerName}/transactions/{transactionId}/metadata/{key}", server.handleDeleteTransactionMetadata)
				r.Post("/{ledgerName}/accounts/{address}/metadata", server.handleSaveAccountMetadata)
				r.Delete("/{ledgerName}/accounts/{address}/metadata/{key}", server.handleDeleteAccountMetadata)
				r.Put("/{ledgerName}/metadata-schema/{targetType}/{key}", server.handleSetMetadataType)
				r.Delete("/{ledgerName}/metadata-schema/{targetType}/{key}", server.handleRemoveMetadataType)
				r.Post("/{ledgerName}/account-types", server.handleAddAccountType)
				r.Delete("/{ledgerName}/account-types/{typeName}", server.handleRemoveAccountType)
				r.Put("/{ledgerName}/account-types/default-enforcement-mode", server.handleSetDefaultEnforcementMode)
			})

			// Bulk endpoints: per-element scope check handled inside the handler
			r.Post("/{ledgerName}/bulk", server.handleBulk)
			r.Post("/{ledgerName}/_bulk", server.handleBulk)

			// Prepared queries (read)
			r.With(requireQueriesRead).Group(func(r chi.Router) {
				r.Get("/{ledgerName}/prepared-queries", server.handleListPreparedQueries)
				r.Post("/{ledgerName}/prepared-queries/{queryName}/execute", server.handleExecutePreparedQuery)
			})

			// Prepared queries (write)
			r.With(requireQueriesWrite).Group(func(r chi.Router) {
				r.Post("/{ledgerName}/prepared-queries", server.handleCreatePreparedQuery)
				r.Put("/{ledgerName}/prepared-queries/{queryName}", server.handleUpdatePreparedQuery)
				r.Delete("/{ledgerName}/prepared-queries/{queryName}", server.handleDeletePreparedQuery)
			})
		})
	}

	// Register routes without prefix (backward compatibility)
	registerRoutes(r)

	// Register routes with /v2 prefix (optional)
	r.Route("/v2", registerRoutes)

	return r
}

// contentTypeMiddleware sets Content-Type header for JSON responses
// For 204 No Content responses, no Content-Type header is set.
func contentTypeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Wrap the response writer to intercept WriteHeader calls
		rw := &contentTypeResponseWriter{ResponseWriter: w}
		next.ServeHTTP(rw, r)
	})
}

// contentTypeResponseWriter wraps http.ResponseWriter to set Content-Type automatically.
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

func (c chiLogEntry) Write(status, bytes int, _ http.Header, elapsed stdtime.Duration, extra any) {
	if c.logger.Enabled(logging.DebugLevel) {
		fields := map[string]any{
			"status":  status,
			"bytes":   bytes,
			"elapsed": elapsed,
		}
		if extra != nil {
			fields["extra"] = extra
		}

		c.logger.WithFields(fields).Debugf("HTTP request completed")
	}
}

func (c chiLogEntry) Panic(v any, stack []byte) {
	c.logger.Errorf("Panicked: %v", v)

	_, _ = c.logger.Writer().Write(stack)
	if span := trace.SpanFromContext(c.ctx); span.SpanContext().IsValid() {
		span.RecordError(fmt.Errorf("%s", v), trace.WithStackTrace(true))
	}
}
