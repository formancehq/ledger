package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.etcd.io/etcd/raft/v3"
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
	CreateLedger(bucketName, ledgerName string, metadata metadata.Metadata) error
	GetLedger(bucketName, ledgerName string) (service.LedgerInfo, bool, error)
	GetLedgerByName(ledgerName string) (service.LedgerInfo, string, bool, error)
	FindBucketForLedger(ledgerName string) (string, error)
	GetAllLedgers(bucketName string) (map[string]service.LedgerInfo, error)
	CreateBucket(name, driver string, config map[string]interface{}) error
	DeleteBucket(name string) error
	CreateBucketSnapshot(bucketName string) error
	GetAllBuckets() map[string]service.BucketInfo
	GetBucket(name string) (service.BucketInfo, bool)
	GetBucketWithRaftState(name string) (*BucketWithRaftState, error)
	GetGRPCClient() service.GRPCClient
	GetRaft() *raft.RawNode
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

// LedgerResponse represents a ledger with its bucket name
type LedgerResponse struct {
	service.LedgerInfo
	Bucket string `json:"bucket"`
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

	// Register known routes (specific routes first)
	r.Post("/snapshot", s.handleSnapshot)
	r.Get("/health", s.handleHealth)
	r.Get("/cluster/state", s.handleClusterState)

	// Register bucket routes
	r.Get("/buckets", s.handleListBuckets) // GET /buckets
	r.Route("/buckets/{bucketName}", func(r chi.Router) {
		r.Get("/", s.handleGetBucket)                     // GET /buckets/{bucketName}
		r.Post("/", s.handleCreateBucket)                 // POST /buckets/{bucketName}
		r.Delete("/", s.handleDeleteBucket)               // DELETE /buckets/{bucketName}
		r.Post("/snapshot", s.handleCreateBucketSnapshot) // POST /buckets/{bucketName}/snapshot
	})

	// Register ledger routes at root (without /ledgers prefix)
	// Note: Routes with parameters must come before the root route
	r.Post("/{ledgerName}", s.handleCreateLedger)                   // POST /{ledgerName}
	r.Get("/{ledgerName}", s.handleGetLedger)                       // GET /{ledgerName}
	r.Post("/{ledgerName}/transactions", s.handleCreateTransaction) // POST /{ledgerName}/transactions
	r.Get("/", s.handleListAllLedgers)                              // GET / (cross-bucket) - must be last

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

	// Check if we are the leader
	if s.isLeader() {
		// We are the leader, call directly
		if err := s.cluster.Snapshot(); err != nil {
			s.logger.Error("Failed to create snapshot", zap.Error(err))
			api.WriteErrorResponse(w, http.StatusInternalServerError, "SNAPSHOT_FAILED", err)
			return
		}

		response := SnapshotData{
			Message: "Snapshot created successfully",
		}
		api.Ok(w, response)
		return
	}

	// We are a follower, forward via gRPC
	grpcClient := s.cluster.GetGRPCClient()
	if grpcClient == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("not connected to leader gRPC server"))
		return
	}

	client := grpcClient.GetClient()
	if client == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("gRPC client not available"))
		return
	}

	// Call leader via gRPC
	resp, err := client.CreateClusterSnapshot(r.Context(), &service.CreateClusterSnapshotRequest{})
	if err != nil {
		s.logger.Error("Failed to create snapshot via gRPC", zap.Error(err))
		api.WriteErrorResponse(w, http.StatusInternalServerError, "SNAPSHOT_FAILED", err)
		return
	}

	response := SnapshotData{
		Message: resp.Message,
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

// handleCreateLedger handles POST /ledgers/{ledgerName} to create a new ledger
// The bucket is determined by checking if the ledger already exists, or by trying to create it in the first available bucket
func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Check if ledger already exists to find its bucket
	bucketName, err := s.cluster.FindBucketForLedger(ledgerName)
	if err == nil {
		// Ledger already exists
		api.WriteErrorResponse(w, http.StatusConflict, "LEDGER_ALREADY_EXISTS", fmt.Errorf("ledger %s already exists in bucket %s", ledgerName, bucketName))
		return
	}

	// Parse request body (bucket and optional metadata)
	var req struct {
		Bucket   string            `json:"bucket"`
		Metadata map[string]string `json:"metadata,omitempty"`
	}

	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
			api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
			return
		}
	}

	// Bucket is required in request body
	if req.Bucket == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("bucket name is required in request body"))
		return
	}

	// Create ledger via cluster in the specified bucket
	if err := s.cluster.CreateLedger(req.Bucket, ledgerName, req.Metadata); err != nil {
		s.logger.Error("Failed to create ledger", zap.String("bucket", req.Bucket), zap.String("name", ledgerName), zap.Error(err))

		// Check if ledger already exists (in this bucket or globally)
		errMsg := err.Error()
		if errMsg == fmt.Sprintf("ledger already exists in bucket %s: %s", req.Bucket, ledgerName) ||
			errMsg == fmt.Sprintf("ledger with name %s already exists in bucket", ledgerName) ||
			errMsg == fmt.Sprintf("creating ledger in bucket %s: ledger already exists in bucket %s: %s", req.Bucket, req.Bucket, ledgerName) ||
			errMsg == fmt.Sprintf("creating ledger in bucket %s: ledger with name %s already exists in bucket", req.Bucket, ledgerName) {
			api.WriteErrorResponse(w, http.StatusConflict, "LEDGER_ALREADY_EXISTS", err)
			return
		}

		api.InternalServerError(w, r, err)
		return
	}

	// Get the created ledger to return it
	ledgerInfo, exists, err := s.cluster.GetLedger(req.Bucket, ledgerName)
	if err != nil || !exists {
		s.logger.Warn("Failed to retrieve created ledger", zap.String("bucket", req.Bucket), zap.String("name", ledgerName), zap.Error(err))
		// Still return success since creation succeeded
		api.Created(w, LedgerResponse{
			LedgerInfo: service.LedgerInfo{
				Name:     ledgerName,
				Metadata: req.Metadata,
			},
			Bucket: req.Bucket,
		})
		return
	}

	// Return the ledger info with bucket name
	api.Created(w, LedgerResponse{
		LedgerInfo: ledgerInfo,
		Bucket:     req.Bucket,
	})
}

// handleListAllLedgers handles GET /ledgers to list all ledgers across all buckets
func (s *Server) handleListAllLedgers(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Get all buckets
	buckets := s.cluster.GetAllBuckets()

	// Collect all ledgers with their bucket names
	ledgersList := make([]LedgerResponse, 0)
	for bucketName := range buckets {
		ledgers, err := s.cluster.GetAllLedgers(bucketName)
		if err != nil {
			s.logger.Warn("Failed to get ledgers from bucket", zap.String("bucket", bucketName), zap.Error(err))
			continue
		}
		for _, ledgerInfo := range ledgers {
			ledgersList = append(ledgersList, LedgerResponse{
				LedgerInfo: ledgerInfo,
				Bucket:     bucketName,
			})
		}
	}

	api.Ok(w, ledgersList)
}

// handleGetLedger handles GET /ledgers/{ledgerName} to get a ledger
func (s *Server) handleGetLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Get ledger by name (finds bucket automatically)
	ledgerInfo, bucketName, exists, err := s.cluster.GetLedgerByName(ledgerName)
	if err != nil {
		s.logger.Error("Failed to get ledger", zap.String("name", ledgerName), zap.Error(err))
		api.InternalServerError(w, r, err)
		return
	}

	if !exists {
		api.WriteErrorResponse(w, http.StatusNotFound, "LEDGER_NOT_FOUND", fmt.Errorf("ledger %s not found", ledgerName))
		return
	}

	// Return ledger info with bucket name
	api.Ok(w, LedgerResponse{
		LedgerInfo: ledgerInfo,
		Bucket:     bucketName,
	})
}

// handleCreateTransaction handles POST /{ledgerName}/transactions to create a new transaction
func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// Decode request body directly into service.CreateTransaction
	var input service.CreateTransaction
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	// Validate postings
	if len(input.Postings) == 0 {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("postings are required"))
		return
	}

	// Extract dryRun from query parameter
	dryRun := r.URL.Query().Get("dryRun") == "true"

	// Extract idempotencyKey from header
	idempotencyKey := r.Header.Get("Idempotency-Key")

	// Build service.Parameters[service.CreateTransaction]
	params := service.Parameters[service.CreateTransaction]{
		DryRun:         dryRun,
		IdempotencyKey: idempotencyKey,
		Input:          input,
	}

	// Call ledger service
	_, createdTx, err := s.ledgerService.CreateTransaction(r.Context(), ledgerName, params)
	if err != nil {
		s.logger.Error("Failed to create transaction", zap.String("ledger", ledgerName), zap.Error(err))
		api.InternalServerError(w, r, err)
		return
	}

	// Return the service response directly - JSON encoding will handle it
	api.Created(w, createdTx)
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

	// Convert map to slice
	bucketsList := make([]service.BucketInfo, 0, len(buckets))
	for _, bucket := range buckets {
		bucketsList = append(bucketsList, bucket)
	}

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

	// Check if bucket already exists (validation en amont)
	if _, exists := s.cluster.GetBucket(bucketName); exists {
		api.WriteErrorResponse(w, http.StatusConflict, "BUCKET_ALREADY_EXISTS", fmt.Errorf("bucket %s already exists", bucketName))
		return
	}

	// Create bucket via cluster
	if err := s.cluster.CreateBucket(bucketName, req.Driver, req.Config); err != nil {
		s.logger.Error("Failed to create bucket", zap.String("name", bucketName), zap.Error(err))
		api.InternalServerError(w, r, err)
		return
	}

	// Get the created bucket to return it
	bucket, exists := s.cluster.GetBucket(bucketName)
	if !exists {
		s.logger.Warn("Failed to retrieve created bucket", zap.String("name", bucketName))
		// Still return success since creation succeeded
		api.Created(w, service.BucketInfo{
			Name:   bucketName,
			Driver: req.Driver,
			Config: req.Config,
		})
		return
	}

	// Return the bucket info
	api.Created(w, bucket)
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

// handleCreateBucketSnapshot handles POST /buckets/{bucketName}/snapshot to create a snapshot for a bucket
func (s *Server) handleCreateBucketSnapshot(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	if bucketName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("bucket name is required"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Get bucket Raft state to check if we are the leader of this bucket's Raft group
	bucketWithState, err := s.cluster.GetBucketWithRaftState(bucketName)
	if err != nil {
		s.logger.Error("Failed to get bucket Raft state", zap.String("bucket", bucketName), zap.Error(err))
		api.InternalServerError(w, r, err)
		return
	}

	if bucketWithState == nil {
		api.WriteErrorResponse(w, http.StatusNotFound, "BUCKET_NOT_FOUND", errors.New("bucket not found"))
		return
	}

	// Check if we are the leader of this bucket's Raft group
	isBucketLeader := bucketWithState.RaftState != nil && bucketWithState.RaftState.State == "Leader"

	if isBucketLeader {
		// We are the leader of this bucket's Raft group, call directly
		if err := s.cluster.CreateBucketSnapshot(bucketName); err != nil {
			s.logger.Error("Failed to create bucket snapshot", zap.String("bucket", bucketName), zap.Error(err))

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
			"message": fmt.Sprintf("Snapshot created successfully for bucket %s", bucketName),
		})
		return
	}

	// We are not the leader of this bucket's Raft group, forward via gRPC
	// The gRPC server will route to the leader of the bucket's Raft group
	grpcClient := s.cluster.GetGRPCClient()
	if grpcClient == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("not connected to leader gRPC server"))
		return
	}

	client := grpcClient.GetClient()
	if client == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("gRPC client not available"))
		return
	}

	// Call leader via gRPC (will route to the leader of the bucket's Raft group)
	resp, err := client.CreateBucketSnapshot(r.Context(), &service.CreateBucketSnapshotRequest{
		BucketName: bucketName,
	})
	if err != nil {
		s.logger.Error("Failed to create bucket snapshot via gRPC", zap.String("bucket", bucketName), zap.Error(err))
		api.WriteErrorResponse(w, http.StatusInternalServerError, "SNAPSHOT_FAILED", err)
		return
	}

	// Return success response
	api.Ok(w, map[string]interface{}{
		"message": resp.Message,
	})
}

// isLeader checks if the current node is the leader
func (s *Server) isLeader() bool {
	if s.cluster == nil {
		return false
	}
	raftInstance := s.cluster.GetRaft()
	if raftInstance == nil {
		return false
	}
	status := raftInstance.Status()
	return status.RaftState == raft.StateLeader
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
