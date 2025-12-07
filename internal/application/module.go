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

func Module() fx.Option {
	return fx.Options(
		transport.Module(),
		fx.Provide(
			NewTransport,
			NewRaftCluster,
			NewLedgerService,
			NewGRPCServer,
			NewSnapshotClient,
			NewSystemServiceServer,
			NewLedgerServiceServer,
			NewHTTPServer,
			NewClusterAdapter,
			httphandler.NewHandler,
		),
		fx.Invoke(
			RegisterRaftTransportServiceHook,
			RegisterSystemServiceHook,
			RegisterLedgerServiceHook,
			StartGRPCServerHook,
			StartHTTPServerHook,
		),
	)
}

func NewTransport(logger logging.Logger, connectionPool *transport.ConnectionPool) *raft.Transport {
	return raft.NewTransport(logger, connectionPool)
}

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

func NewLedgerService(cluster *raft.Cluster) service.Ledger {
	return cluster.GetLedgerService()
}

func NewHTTPServer(logger logging.Logger, ledgerService service.Ledger, cluster httphandler.ClusterClient) *httphandler.Server {
	return httphandler.NewServer(logger, ledgerService, cluster)
}

func StartHTTPServerHook(lc fx.Lifecycle, cfg *config.Config, handler http.Handler) {
	lc.Append(httpserver.NewHook(handler,
		httpserver.WithAddress(fmt.Sprintf(":%d", cfg.HTTPPort)),
	))
}

func NewClusterAdapter(cluster *raft.Cluster) httphandler.ClusterClient {
	return &clusterAdapter{Cluster: cluster}
}

func NewGRPCServer(cfg *config.Config, logger logging.Logger) (*grpcserver.Server, error) {
	_, raftPort, err := net.SplitHostPort(cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid bind address format: %w", err)
	}
	grpcPort, err := strconv.Atoi(raftPort)
	if err != nil {
		return nil, fmt.Errorf("invalid port in bind address: %w", err)
	}

	return grpcserver.NewServer(grpcPort, logger), nil
}

func NewSnapshotClient(cluster *raft.Cluster) service.SnapshotClient {
	return cluster
}

func NewSystemServiceServer(logger logging.Logger, snapshotClient service.SnapshotClient) service.SystemServiceServer {
	return service.NewSystemServiceServer(logger, snapshotClient)
}

func NewLedgerServiceServer(logger logging.Logger, ledgerService service.Ledger) service.LedgerServiceServer {
	return service.NewLedgerServiceServer(logger, ledgerService)
}

func RegisterRaftTransportServiceHook(grpcServer *grpcserver.Server, transport *raft.Transport) error {
	raft.RegisterRaftTransportService(grpcServer.GetServer(), transport)
	return nil
}

func RegisterSystemServiceHook(grpcServer *grpcserver.Server, systemServiceServer service.SystemServiceServer) error {
	service.RegisterSystemService(grpcServer.GetServer(), systemServiceServer)
	return nil
}

func RegisterLedgerServiceHook(grpcServer *grpcserver.Server, ledgerServiceServer service.LedgerServiceServer) error {
	service.RegisterLedgerService(grpcServer.GetServer(), ledgerServiceServer)
	return nil
}

func StartGRPCServerHook(lc fx.Lifecycle, grpcServer *grpcserver.Server, logger logging.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				if err := grpcServer.Start(); err != nil {
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
