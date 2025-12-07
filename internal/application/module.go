package application

import (
	"context"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/httpserver"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/config"
	httphandler "github.com/formancehq/ledger-v3-poc/internal/http"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
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
		// Provide core dependencies
		fx.Provide(
			NewRaftCluster,
			NewLedgerService,
			NewHTTPServer,
			NewClusterAdapter,
			httphandler.NewHandler,
		),
		// Invoke lifecycle hooks
		fx.Invoke(
			StartRaftCluster,
			StartHTTPServerHook,
		),
	)
}

// NewRaftCluster creates a new Raft cluster
func NewRaftCluster(lc fx.Lifecycle, cfg *config.Config, logger logging.Logger) (*raft.Cluster, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := raft.NewRaftCluster(ctx, cfg, logger)
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

// StartRaftCluster is a no-op hook since cluster is started in NewRaftCluster
// This is kept for clarity and potential future use
func StartRaftCluster() {
	// Raft cluster is started in NewRaftCluster lifecycle hook
}
