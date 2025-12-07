package application

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"

	"github.com/formancehq/go-libs/v3/httpserver"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/config"
	grpcserver "github.com/formancehq/ledger-v3-poc/internal/grpc"
	httphandler "github.com/formancehq/ledger-v3-poc/internal/http"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/formancehq/ledger-v3-poc/internal/transport"
	"go.uber.org/fx"
)

// clusterAdapter adapts *raft.Cluster to http.ClusterClient
type clusterAdapter struct {
	*raft.Cluster
}

// Ensure clusterAdapter implements httphandler.ClusterClient
var _ httphandler.ClusterClient = (*clusterAdapter)(nil)

// Module provides all application dependencies
func Module() fx.Option {
	return fx.Options(
		// Transport module (provides ConnectionPool)
		transport.Module(),
		// Provide core dependencies
		fx.Provide(
			NewTransport,
			NewRaftCluster,
			NewLedgerService,
			NewGRPCServer,
			NewHTTPServer,
			NewClusterAdapter,
			httphandler.NewHandler,
		),
		// Invoke lifecycle hooks
		fx.Invoke(
			StartRaftCluster,
			StartGRPCServerHook,
			StartHTTPServerHook,
		),
	)
}

// NewTransport creates a new transport
func NewTransport(logger logging.Logger, connectionPool *transport.ConnectionPool) *raft.Transport {
	return raft.NewTransport(logger, connectionPool)
}

// NewRaftCluster creates a new Raft cluster
func NewRaftCluster(lc fx.Lifecycle, cfg *config.Config, logger logging.Logger, transport *raft.Transport) (*raft.Cluster, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := raft.NewRaftCluster(ctx, cfg, logger, transport)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("creating raft cluster: %w", err)
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := cluster.Start(); err != nil {
				return fmt.Errorf("starting raft cluster: %w", err)
			}
			logger.Infof("Raft cluster started successfully")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			if err := cluster.Shutdown(); err != nil {
				return fmt.Errorf("shutting down raft cluster: %w", err)
			}
			return nil
		},
	})

	return cluster, nil
}

// NewLedgerService creates a new ledger service from the cluster
func NewLedgerService(cluster *raft.Cluster) service.Ledger {
	return cluster.GetLedgerService()
}

// NewHTTPServer creates a new HTTP server instance (used by handlers)
func NewHTTPServer(logger logging.Logger, ledgerService service.Ledger, cluster httphandler.ClusterClient) *httphandler.Server {
	return httphandler.NewServer(logger, ledgerService, cluster)
}

// StartHTTPServerHook starts the HTTP server using httpserver.NewHook
func StartHTTPServerHook(lc fx.Lifecycle, cfg *config.Config, handler http.Handler) {
	lc.Append(httpserver.NewHook(handler,
		httpserver.WithAddress(fmt.Sprintf(":%d", cfg.HTTPPort)),
	))
}

// NewClusterAdapter creates an adapter that makes *raft.Cluster implement http.ClusterClient
func NewClusterAdapter(cluster *raft.Cluster) httphandler.ClusterClient {
	return &clusterAdapter{Cluster: cluster}
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(cfg *config.Config, logger logging.Logger, ledgerService service.Ledger, cluster *raft.Cluster) (*grpcserver.Server, error) {
	// Extract port from BindAddr for the unified gRPC server
	// The unified server listens on the same port as Raft transport (BindAddr)
	_, raftPort, err := net.SplitHostPort(cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid bind address format: %w", err)
	}
	grpcPort, err := strconv.Atoi(raftPort)
	if err != nil {
		return nil, fmt.Errorf("invalid port in bind address: %w", err)
	}

	// Get transport from cluster to register Raft service
	transport := cluster.GetTransport()
	return grpcserver.NewServer(grpcPort, logger, ledgerService, transport, cluster), nil
}

// StartGRPCServerHook starts the gRPC server using fx lifecycle
func StartGRPCServerHook(lc fx.Lifecycle, grpcServer *grpcserver.Server, logger logging.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// Start gRPC server in a goroutine
			go func() {
				if err := grpcServer.Start(ctx); err != nil {
					logger.WithFields(map[string]any{"error": err}).Errorf("gRPC server error")
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return grpcServer.Stop()
		},
	})
}

// StartRaftCluster is a no-op hook since cluster is started in NewRaftCluster
func StartRaftCluster() {
	// Raft cluster is started in NewRaftCluster lifecycle hook
}
