package http

import (
	"context"
	"fmt"
	"net/http"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.etcd.io/etcd/raft/v3"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

type Server struct {
	server        *http.Server
	logger        *zap.Logger
	ledgerService service.Ledger
	cluster       ClusterClient
	port          int
}

// todo: use in place of ClusterClient
type LeaderClient interface {
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
	GetLeaderGRPCClient() service.LedgerServiceClient
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

	// Wrap handler with OpenTelemetry instrumentation
	handler := otelhttp.NewHandler(r, "ledger-http-server",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	)

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
