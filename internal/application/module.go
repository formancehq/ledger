package application

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"

	"github.com/formancehq/go-libs/v3/httpserver"
	"github.com/formancehq/go-libs/v3/logging"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	grpcserver "github.com/formancehq/ledger-v3-poc/internal/grpc"
	httphandler "github.com/formancehq/ledger-v3-poc/internal/http"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/raft/system"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/formancehq/ledger-v3-poc/internal/transport"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Options(
		transport.Module(),
		fx.Provide(
			raft.NewTransport,
			system.NewNode,
			func(cfg Config) system.Config {
				return cfg.RaftConfig
			},
			func(systemNode *system.Node, pool *transport.ConnectionPool, logger logging.Logger) service.MasterCluster {
				return &systemNodeAdapter{
					systemNode:     systemNode,
					connectionPool: pool,
					logger:         logger,
				}
			},
			func(cfg system.Config, logger logging.Logger) (*grpcserver.Server, error) {
				_, raftPort, err := net.SplitHostPort(cfg.BindAddr)
				if err != nil {
					return nil, fmt.Errorf("invalid bind address format: %w", err)
				}
				grpcPort, err := strconv.Atoi(raftPort)
				if err != nil {
					return nil, fmt.Errorf("invalid port in bind address: %w", err)
				}

				return grpcserver.NewServer(grpcPort, logger), nil
			},
			service.NewSystemServiceServer,
			service.NewBucketServiceServer,
			httphandler.NewServer,
			httphandler.NewHandler,
		),
		fx.Invoke(
			func(lc fx.Lifecycle, systemNode *system.Node, logger logging.Logger) (*system.Node, error) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						if err := systemNode.Start(); err != nil {
							return fmt.Errorf("starting raft cluster: %w", err)
						}
						logger.Infof("Raft cluster started successfully")
						return nil
					},
					OnStop: func(ctx context.Context) error {
						logger.Infof("Shutting down raft cluster")
						if err := systemNode.Stop(ctx); err != nil {
							return fmt.Errorf("shutting down raft cluster: %w", err)
						}
						logger.Infof("Raft cluster stopped successfully")
						return nil
					},
				})

				return systemNode, nil
			},
			func(grpcServer *grpcserver.Server, transport *raft.GRPCTransport) error {
				raft.RegisterRaftTransportService(grpcServer.GetServer(), transport)
				return nil
			},
			func(grpcServer *grpcserver.Server, systemServiceServer service.SystemServiceServer) error {
				service.RegisterSystemService(grpcServer.GetServer(), systemServiceServer)
				return nil
			},
			func(grpcServer *grpcserver.Server, bucketServiceServer service.BucketServiceServer) error {
				service.RegisterBucketService(grpcServer.GetServer(), bucketServiceServer)
				return nil
			},
			func(lc fx.Lifecycle, grpcServer *grpcserver.Server, logger logging.Logger) {
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
			},
			func(lc fx.Lifecycle, cfg Config, handler http.Handler) {
				lc.Append(httpserver.NewHook(handler,
					httpserver.WithAddress(fmt.Sprintf(":%d", cfg.HTTPPort)),
				))
			},
		),
	)
}

type systemNodeAdapter struct {
	systemNode     *system.Node
	logger         logging.Logger
	connectionPool *transport.ConnectionPool
}

func (adapter *systemNodeAdapter) IsHealthy() bool {
	return adapter.systemNode.IsHealthy()
}

func (adapter *systemNodeAdapter) GetAllBucketsInfo(ctx context.Context) map[string]ledger.BucketInfo {
	mainCluster, err := adapter.getMainCluster()
	if err != nil {
		// todo: return error
		panic(err)
	}
	return mainCluster.GetAllBucketsInfo(ctx)
}

func (adapter *systemNodeAdapter) GetBucketInfo(ctx context.Context, name string) (*ledger.BucketInfo, error) {
	mainCluster, err := adapter.getMainCluster()
	if err != nil {
		// todo: return error
		panic(err)
	}
	return mainCluster.GetBucketInfo(ctx, name)
}

func (adapter *systemNodeAdapter) GetClusterState(ctx context.Context) (*ledger.ClusterState[ledger.SystemState], error) {
	return adapter.systemNode.GetClusterState(ctx)
}

func (adapter *systemNodeAdapter) GetBucketCluster(ctx context.Context, name string) (service.BucketCluster, error) {
	return adapter.systemNode.GetBucketCluster(ctx, name)
}

func (adapter *systemNodeAdapter) getMainCluster() (interface {
	service.Cluster
	service.System
	service.LeaderOnly
}, error) {
	if adapter.systemNode.IsLeader() {
		return adapter.systemNode, nil
	}
	if adapter.systemNode.GetLeader() == 0 {
		return nil, ledger.ErrNoLeader
	}

	grpcConn := adapter.connectionPool.GetConnection(adapter.systemNode.GetLeader())
	grpcClient := service.NewGrpcSystemClient(service.NewSystemServiceClient(grpcConn))
	return struct {
		service.Cluster
		service.System
		service.LeaderOnly
	}{
		Cluster:    adapter.systemNode,
		System:     grpcClient,
		LeaderOnly: grpcClient,
	}, nil
}

func (adapter *systemNodeAdapter) ResolveLedger(ctx context.Context, ledgerName string) (string, uint64, error) {
	cluster, err := adapter.getMainCluster()
	if err != nil {
		return "", 0, err
	}
	return cluster.ResolveLedger(ctx, ledgerName)
}

func (adapter *systemNodeAdapter) CreateBucket(ctx context.Context, name, driver string, config map[string]interface{}, snapshotThreshold *uint64) (*ledger.BucketInfo, error) {
	cluster, err := adapter.getMainCluster()
	if err != nil {
		return nil, err
	}
	return cluster.CreateBucket(ctx, name, driver, config, snapshotThreshold)
}

func (adapter *systemNodeAdapter) DeleteBucket(ctx context.Context, name string) error {
	cluster, err := adapter.getMainCluster()
	if err != nil {
		return err
	}
	return cluster.DeleteBucket(ctx, name)
}

func (adapter *systemNodeAdapter) Snapshot(ctx context.Context) error {
	cluster, err := adapter.getMainCluster()
	if err != nil {
		return err
	}
	return cluster.Snapshot(ctx)
}

func (adapter *systemNodeAdapter) GetBucketClusterLocal(ctx context.Context, name string) (service.BucketCluster, error) {
	return adapter.systemNode.GetBucketCluster(ctx, name)
}

var _ service.MasterCluster = (*systemNodeAdapter)(nil)
