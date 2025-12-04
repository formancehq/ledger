package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"runtime/debug"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
)

type Server struct {
	server        *http.Server
	logger        *zap.Logger
	ledgerService service.Ledger
	cluster       ClusterClient
	port          int
}

// ClusterClient is an interface for cluster operations
type ClusterClient interface {
	Snapshot() error
}

func NewServer(port int, logger *zap.Logger, ledgerService service.Ledger, cluster ClusterClient) *Server {
	return &Server{
		logger:        logger,
		ledgerService: ledgerService,
		cluster:       cluster,
		port:          port,
	}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/transactions", s.handleCreateTransaction)
	mux.HandleFunc("/snapshot", s.handleSnapshot)

	// Wrap handler with middlewares
	handler := s.middlewareChain(mux)

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: handler,
	}

	s.logger.Info("Starting HTTP server", zap.Int("port", s.port))

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server failed", zap.Error(err))
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	return s.Stop()
}

func (s *Server) Stop() error {
	if s.server != nil {
		s.logger.Info("Stopping HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 5*stdtime.Second)
		defer cancel()
		return s.server.Shutdown(ctx)
	}
	return nil
}

// Request/Response structures for JSON

type CreateTransactionRequest struct {
	AccountMetadata map[string]metadata.Metadata `json:"accountMetadata,omitempty"`
	Timestamp       string                       `json:"timestamp,omitempty"` // ISO 8601 format
	Metadata        metadata.Metadata            `json:"metadata,omitempty"`
	Reference       string                       `json:"reference,omitempty"`
	Postings        []PostingRequest             `json:"postings"`
	DryRun          bool                         `json:"dryRun,omitempty"`
	IdempotencyKey  string                       `json:"idempotencyKey,omitempty"`
}

type PostingRequest struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      string `json:"amount"` // big.Int as string
	Asset       string `json:"asset"`
}

type CreateTransactionData struct {
	Transaction     TransactionResponse          `json:"transaction"`
	AccountMetadata map[string]metadata.Metadata `json:"accountMetadata,omitempty"`
}

type TransactionResponse struct {
	Postings  []PostingResponse `json:"postings"`
	Metadata  metadata.Metadata `json:"metadata,omitempty"`
	Timestamp string            `json:"timestamp"` // ISO 8601 format
	Reference string            `json:"reference,omitempty"`
	ID        *uint64           `json:"id,omitempty"`
}

type PostingResponse struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      string `json:"amount"` // big.Int as string
	Asset       string `json:"asset"`
}

func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
		return
	}

	var req CreateTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.BadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request: %w", err))
		return
	}

	// Convert request to service parameters
	postings := make(ledger.Postings, 0, len(req.Postings))
	for _, p := range req.Postings {
		amount, ok := new(big.Int).SetString(p.Amount, 10)
		if !ok {
			api.BadRequest(w, "INVALID_AMOUNT", fmt.Errorf("invalid amount: %s", p.Amount))
			return
		}
		postings = append(postings, ledger.NewPosting(p.Source, p.Destination, p.Asset, amount))
	}

	// Parse timestamp
	var timestamp time.Time
	if req.Timestamp != "" {
		parsed, err := stdtime.Parse(stdtime.RFC3339, req.Timestamp)
		if err != nil {
			api.BadRequest(w, "INVALID_TIMESTAMP", fmt.Errorf("invalid timestamp format: %w", err))
			return
		}
		timestamp = time.New(parsed)
	} else {
		timestamp = time.Now()
	}

	params := service.Parameters[service.CreateTransaction]{
		DryRun:         req.DryRun,
		IdempotencyKey: req.IdempotencyKey,
		Input: service.CreateTransaction{
			AccountMetadata: req.AccountMetadata,
			Timestamp:       timestamp,
			Metadata:        req.Metadata,
			Reference:       req.Reference,
			Postings:        postings,
		},
	}

	// Call ledger service
	_, createdTx, err := s.ledgerService.CreateTransaction(r.Context(), params)
	if err != nil {
		s.logger.Error("Failed to create transaction", zap.Error(err))
		api.InternalServerError(w, r, err)
		return
	}

	// Convert response to JSON
	response := CreateTransactionData{
		Transaction:     transactionToResponse(createdTx.Transaction),
		AccountMetadata: createdTx.AccountMetadata,
	}

	api.Created(w, response)
}

func transactionToResponse(tx ledger.Transaction) TransactionResponse {
	postings := make([]PostingResponse, 0, len(tx.Postings))
	for _, p := range tx.Postings {
		postings = append(postings, PostingResponse{
			Source:      p.Source,
			Destination: p.Destination,
			Amount:      p.Amount.String(),
			Asset:       p.Asset,
		})
	}

	var id *uint64
	if tx.ID != nil {
		id = tx.ID
	}

	timestamp := ""
	if !tx.Timestamp.IsZero() {
		timestamp = tx.Timestamp.Time.Format(stdtime.RFC3339)
	}

	return TransactionResponse{
		Postings:  postings,
		Metadata:  tx.Metadata,
		Timestamp: timestamp,
		Reference: tx.Reference,
		ID:        id,
	}
}

type SnapshotData struct {
	Message string `json:"message"`
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	if err := s.cluster.Snapshot(); err != nil {
		s.logger.Error("Failed to create snapshot", zap.Error(err))
		api.WriteErrorResponse(w, http.StatusInternalServerError, "SNAPSHOT_FAILED", err)
		return
	}

	response := SnapshotData{
		Message: "Snapshot created successfully",
	}

	api.Ok(w, response)
}

// middlewareChain applies all middlewares to the handler
func (s *Server) middlewareChain(handler http.Handler) http.Handler {
	// Apply panic recovery middleware first (outermost)
	handler = recoveryMiddleware(s.logger, handler)

	// Apply logging middleware
	// Use the zap logger directly with a simple wrapper
	handler = loggingMiddleware(s.logger, handler)

	return handler
}

// recoveryMiddleware recovers from panics and returns a 500 error
func recoveryMiddleware(logger *zap.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// Log the panic with stack trace
				logger.Error("Panic recovered",
					zap.Any("error", err),
					zap.String("stack", string(debug.Stack())),
					zap.String("method", r.Method),
					zap.String("path", r.URL.Path),
				)

				// Return 500 error
				api.InternalServerError(w, r, fmt.Errorf("internal server error: %v", err))
			}
		}()

		next.ServeHTTP(w, r)
	})
}

// loggingMiddleware logs HTTP requests
func loggingMiddleware(logger *zap.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := stdtime.Now()

		// Create a response writer wrapper to capture status code
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Call next handler
		next.ServeHTTP(rw, r)

		// Log the request
		duration := stdtime.Since(start)
		logger.Info("HTTP request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("query", r.URL.RawQuery),
			zap.Int("status", rw.statusCode),
			zap.Duration("duration", duration),
			zap.String("remote_addr", r.RemoteAddr),
			zap.String("user_agent", r.UserAgent()),
		)
	})
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
