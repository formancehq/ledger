package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
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
	IsHealthy() bool
	GetClusterState() (*ClusterState, error)
	CreateLedger(name string, metadata map[string]string) error
	CreateBucket(name, driver string, config map[string]interface{}) error
	DeleteBucket(name string) error
	GetAllBuckets() map[string]service.BucketInfo
	GetBucketWithRaftState(name string) (*BucketWithRaftState, error)
}

// ClusterState represents the state of the Raft cluster
type ClusterState struct {
	State     string     `json:"state"`     // Leader, Follower, Candidate, Shutdown
	Leader    string     `json:"leader"`    // ID of the current leader (empty if no leader)
	Nodes     []NodeInfo `json:"nodes"`     // List of all nodes in the cluster
	LocalNode string     `json:"localNode"` // ID of the local node
}

// NodeInfo represents information about a node in the cluster
type NodeInfo struct {
	ID       string `json:"id"`       // Node ID
	Address  string `json:"address"`  // Node address
	Suffrage string `json:"suffrage"` // Voter or Nonvoter
}

// BucketWithRaftState represents a bucket with its Raft cluster state
type BucketWithRaftState struct {
	service.BucketInfo
	RaftState *ClusterState `json:"raftState"`
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
	r := chi.NewRouter()

	// Apply middlewares
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(s.loggingMiddleware)

	// Register known routes
	r.Post("/snapshot", s.handleSnapshot)
	r.Get("/health", s.handleHealth)
	r.Get("/cluster/state", s.handleClusterState)

	// Register ledger routes with dynamic parameter
	r.Route("/{ledgerName}", func(r chi.Router) {
		r.Post("/", s.handleCreateLedger)                  // POST /{ledgerName}
		r.Post("/transactions", s.handleCreateTransaction) // POST /{ledgerName}/transactions
	})

	// Register bucket routes
	r.Get("/buckets", s.handleListBuckets) // GET /buckets
	r.Route("/buckets/{bucketName}", func(r chi.Router) {
		r.Get("/", s.handleGetBucket)        // GET /buckets/{bucketName}
		r.Post("/", s.handleCreateBucket)   // POST /buckets/{bucketName}
		r.Delete("/", s.handleDeleteBucket) // DELETE /buckets/{bucketName}
	})

	handler := r

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
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Amount      *big.Int `json:"amount"`
	Asset       string   `json:"asset"`
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
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Amount      *big.Int `json:"amount"`
	Asset       string   `json:"asset"`
}

func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
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
		if p.Amount == nil {
			api.BadRequest(w, "INVALID_AMOUNT", errors.New("amount is required"))
			return
		}
		postings = append(postings, ledger.NewPosting(p.Source, p.Destination, p.Asset, p.Amount))
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
	_, createdTx, err := s.ledgerService.CreateTransaction(r.Context(), ledgerName, params)
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
			Amount:      p.Amount,
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

type HealthData struct {
	Status string `json:"status"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.WriteErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	if !s.cluster.IsHealthy() {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "UNHEALTHY", errors.New("node is not connected to the cluster"))
		return
	}

	response := HealthData{
		Status: "ok",
	}

	api.Ok(w, response)
}

func (s *Server) handleClusterState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.WriteErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	clusterState, err := s.cluster.GetClusterState()
	if err != nil {
		s.logger.Error("Failed to get cluster state", zap.Error(err))
		api.WriteErrorResponse(w, http.StatusInternalServerError, "CLUSTER_STATE_ERROR", err)
		return
	}

	api.Ok(w, clusterState)
}

// handleCreateLedger handles POST /{ledgerName} to create a new ledger
func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Parse request body (optional metadata)
	var req struct {
		Metadata map[string]string `json:"metadata,omitempty"`
	}

	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
			api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
			return
		}
	}

	// Create ledger via cluster
	if err := s.cluster.CreateLedger(ledgerName, req.Metadata); err != nil {
		s.logger.Error("Failed to create ledger", zap.String("name", ledgerName), zap.Error(err))

		// Check if ledger already exists
		if err.Error() == fmt.Sprintf("ledger already exists: %s", ledgerName) {
			api.WriteErrorResponse(w, http.StatusConflict, "LEDGER_ALREADY_EXISTS", err)
			return
		}

		api.InternalServerError(w, r, err)
		return
	}

	// Return success response
	api.Created(w, map[string]interface{}{
		"name":     ledgerName,
		"metadata": req.Metadata,
	})
}

// handleListBuckets handles GET /buckets to list all buckets
func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.WriteErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Get all buckets from cluster
	buckets := s.cluster.GetAllBuckets()

	// Convert to response format matching BucketInfo structure from SDK
	// The SDK expects createdAt as *time.Time, so we need to parse the string
	bucketsList := make([]map[string]interface{}, 0, len(buckets))
	for name, bucket := range buckets {
		bucketMap := map[string]interface{}{
			"name":   name,
			"driver": bucket.Driver,
			"config": bucket.Config,
		}
		
		// Parse createdAt string to time.Time for SDK compatibility
		if bucket.CreatedAt != "" {
			if createdAt, err := stdtime.Parse(stdtime.RFC3339, bucket.CreatedAt); err == nil {
				bucketMap["createdAt"] = createdAt
			} else {
				// If parsing fails, include as string (shouldn't happen with valid data)
				bucketMap["createdAt"] = bucket.CreatedAt
			}
		}
		
		bucketsList = append(bucketsList, bucketMap)
	}

	// Return success response matching ListBucketsResponse schema
	// api.Ok wraps the response in {"data": ...}, so we pass the list directly
	api.Ok(w, bucketsList)
}

// handleCreateBucket handles POST /buckets/{bucketName} to create a new bucket
func (s *Server) handleCreateBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	if bucketName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("bucket name is required"))
		return
	}

	// Parse request body (driver and config are required)
	var req struct {
		Driver string                 `json:"driver"`
		Config map[string]interface{} `json:"config"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	// Validate required fields
	if req.Driver == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("driver is required"))
		return
	}

	if req.Config == nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("config is required"))
		return
	}

	// Validate bucket configuration
	if err := service.ValidateBucketConfig(req.Driver, req.Config); err != nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_CONFIG", err)
		return
	}

	// Create bucket via cluster
	if err := s.cluster.CreateBucket(bucketName, req.Driver, req.Config); err != nil {
		s.logger.Error("Failed to create bucket", zap.String("name", bucketName), zap.Error(err))

		// Check if bucket already exists
		if err.Error() == fmt.Sprintf("bucket already exists: %s", bucketName) {
			api.WriteErrorResponse(w, http.StatusConflict, "BUCKET_ALREADY_EXISTS", err)
			return
		}

		api.InternalServerError(w, r, err)
		return
	}

	// Return success response
	api.Created(w, map[string]interface{}{
		"name":     bucketName,
		"driver":   req.Driver,
		"config":   req.Config,
	})
}

// handleGetBucket handles GET /buckets/{bucketName} to get a bucket with its Raft state
func (s *Server) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	if bucketName == "" {
		api.BadRequest(w, "bucket name is required", errors.New("bucket name is required"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	bucket, err := s.cluster.GetBucketWithRaftState(bucketName)
	if err != nil {
		s.logger.Error("Failed to get bucket", zap.String("bucket", bucketName), zap.Error(err))
		api.InternalServerError(w, r, err)
		return
	}

	if bucket == nil {
		api.NotFound(w, errors.New("bucket not found"))
		return
	}

	api.Ok(w, bucket)
}

// handleDeleteBucket handles DELETE /buckets/{bucketName} to delete a bucket
func (s *Server) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	if bucketName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("bucket name is required"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Delete bucket via cluster
	if err := s.cluster.DeleteBucket(bucketName); err != nil {
		s.logger.Error("Failed to delete bucket", zap.String("name", bucketName), zap.Error(err))

		// Check if bucket does not exist
		if err.Error() == fmt.Sprintf("bucket does not exist: %s", bucketName) {
			api.WriteErrorResponse(w, http.StatusNotFound, "BUCKET_NOT_FOUND", err)
			return
		}

		api.InternalServerError(w, r, err)
		return
	}

	// Return success response
	api.Ok(w, map[string]interface{}{
		"message": fmt.Sprintf("Bucket %s deleted successfully", bucketName),
	})
}

// loggingMiddleware logs HTTP requests (chi middleware)
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
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
		s.logger.Info("HTTP request",
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
