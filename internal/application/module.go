package application

import (
	"context"
	"errors"
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
	ledgerraft "github.com/formancehq/ledger-v3-poc/internal/raft/ledger"
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
			NewSystemServiceServer,
			NewLedgerServiceServer,
			httphandler.NewServer,
			httphandler.NewHandler,
		),
		fx.Invoke(
			func(lc fx.Lifecycle, systemNode *system.Node, logger logging.Logger) (*system.Node, error) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						if err := systemNode.Start(ctx); err != nil {
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
				RegisterSystemService(grpcServer.GetServer(), systemServiceServer)
				return nil
			},
			func(grpcServer *grpcserver.Server, ledgerServiceServer service.LedgerServiceServer) error {
				RegisterLedgerService(grpcServer.GetServer(), ledgerServiceServer)
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

func (adapter *systemNodeAdapter) ResolveLedgerLeader(ctx context.Context, ledgerName string) (uint64, error) {
	ledgerNode, err := adapter.systemNode.GetLedgerNode(ctx, ledgerName)
	if err != nil {
		return 0, err
	}
	return ledgerNode.GetLeader(), nil
}

func (adapter *systemNodeAdapter) GetLeader() uint64 {
	return adapter.systemNode.GetLeader()
}

func (adapter *systemNodeAdapter) getMainCluster() (service.System, error) {
	if adapter.systemNode.IsLeader() {
		return adapter.systemNode, nil
	}
	if adapter.systemNode.GetLeader() == 0 {
		return nil, ledger.ErrNoLeader
	}

	grpcConn := adapter.connectionPool.GetConnection(adapter.systemNode.GetLeader())
	grpcClient := service.NewGrpcSystemClient(service.NewSystemServiceClient(grpcConn))

	return grpcClient, nil
}

func (adapter *systemNodeAdapter) IsHealthy() bool {
	return adapter.systemNode.IsHealthy()
}

func (adapter *systemNodeAdapter) GetAllLedgersInfo(ctx context.Context) map[string]ledger.LedgerInfo {
	mainCluster, err := adapter.getMainCluster()
	if err != nil {
		// todo: return error
		panic(err)
	}
	return mainCluster.GetAllLedgersInfo(ctx)
}

func (adapter *systemNodeAdapter) GetLedgerInfo(ctx context.Context, name string) (*ledger.LedgerInfo, error) {
	mainCluster, err := adapter.getMainCluster()
	if err != nil {
		return nil, err
	}
	return mainCluster.GetLedgerInfo(ctx, name)
}

func (adapter *systemNodeAdapter) GetClusterState(ctx context.Context) (*ledger.ClusterState[ledger.SystemState], error) {
	return adapter.systemNode.GetClusterState(ctx)
}

func (adapter *systemNodeAdapter) GetLedgerCluster(ctx context.Context, name string) (service.LedgerCluster, error) {
	ledgerNode, err := adapter.systemNode.GetLedgerNode(ctx, name)
	if err != nil && !errors.Is(err, &ledger.NotFoundError{}) {
		return nil, fmt.Errorf("resolving local ledger node: %w", err)
	}

	if err != nil || !ledgerNode.IsLeader() {
		mainCluster, err := adapter.getMainCluster()
		if err != nil {
			return nil, err
		}

		ledgerLeader, err := mainCluster.ResolveLedgerLeader(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("resolving ledger leader: %w", err)
		}

		grpcConn := adapter.connectionPool.GetConnection(ledgerLeader & 0xFFFF) // todo: move that
		if grpcConn == nil {
			panic(fmt.Sprintf("no connection to ledger leader: %d", ledgerLeader))
		}
		ledgerClusterClient := service.NewLedgerGrpcClient(name, service.NewLedgerServiceClient(grpcConn))

		return &ledgerClusterRouter{
			localNode:     ledgerNode,
			ledgerCluster: ledgerClusterClient,
			logger:        adapter.logger,
		}, nil
	}

	return ledgerNode, nil
}

func (adapter *systemNodeAdapter) ResolveLedger(ctx context.Context, ledgerName string) (string, uint64, error) {
	cluster, err := adapter.getMainCluster()
	if err != nil {
		return "", 0, fmt.Errorf("resolving main cluster: %w", err)
	}
	ledgerNameResolved, ledgerID, err := cluster.ResolveLedger(ctx, ledgerName)
	if err != nil {
		return "", 0, fmt.Errorf("resolving ledger: %w", err)
	}

	return ledgerNameResolved, ledgerID, nil
}

func (adapter *systemNodeAdapter) CreateLedger(ctx context.Context, name, driver string, config map[string]interface{}, metadata map[string]string, snapshotThreshold *uint64) (*ledger.LedgerInfo, error) {
	cluster, err := adapter.getMainCluster()
	if err != nil {
		return nil, err
	}
	return cluster.CreateLedger(ctx, name, driver, config, metadata, snapshotThreshold)
}

func (adapter *systemNodeAdapter) DeleteLedger(ctx context.Context, name string) error {
	cluster, err := adapter.getMainCluster()
	if err != nil {
		return err
	}
	return cluster.DeleteLedger(ctx, name)
}

func (adapter *systemNodeAdapter) Snapshot(ctx context.Context) error {
	cluster, err := adapter.getMainCluster()
	if err != nil {
		return err
	}
	return cluster.Snapshot(ctx)
}

func (adapter *systemNodeAdapter) GetLedgerClusterLocal(ctx context.Context, name string) (service.LedgerCluster, error) {
	return adapter.systemNode.GetLedgerNode(ctx, name)
}

var _ service.MasterCluster = (*systemNodeAdapter)(nil)

type ledgerClusterRouter struct {
	localNode     *ledgerraft.Node
	ledgerCluster *service.LedgerGrpcClient
	logger        logging.Logger
}

func (l ledgerClusterRouter) Snapshot(ctx context.Context) error {
	return l.ledgerCluster.Snapshot(ctx)
}

func (l ledgerClusterRouter) IsHealthy() bool {
	return l.localNode.IsHealthy()
}

func (l ledgerClusterRouter) GetLeader() uint64 {
	return l.localNode.GetLeader()
}

func (l ledgerClusterRouter) CreateTransaction(ctx context.Context, ledgerName string, parameters service.Parameters[service.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	return l.ledgerCluster.CreateTransaction(ctx, ledgerName, parameters)
}

func (l ledgerClusterRouter) RevertTransaction(ctx context.Context, ledgerName string, parameters service.Parameters[service.RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	return l.ledgerCluster.RevertTransaction(ctx, ledgerName, parameters)
}

func (l ledgerClusterRouter) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[service.SaveTransactionMetadata]) (*ledger.Log, error) {
	return l.ledgerCluster.SaveTransactionMetadata(ctx, ledgerName, parameters)
}

func (l ledgerClusterRouter) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[service.SaveAccountMetadata]) (*ledger.Log, error) {
	return l.ledgerCluster.SaveAccountMetadata(ctx, ledgerName, parameters)
}

func (l ledgerClusterRouter) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[service.DeleteTransactionMetadata]) (*ledger.Log, error) {
	return l.ledgerCluster.DeleteTransactionMetadata(ctx, ledgerName, parameters)
}

func (l ledgerClusterRouter) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[service.DeleteAccountMetadata]) (*ledger.Log, error) {
	return l.ledgerCluster.DeleteAccountMetadata(ctx, ledgerName, parameters)
}

func (l ledgerClusterRouter) Import(ctx context.Context, ledgerName string, stream chan ledger.Log) error {
	return l.ledgerCluster.Import(ctx, ledgerName, stream)
}

func (l ledgerClusterRouter) Export(ctx context.Context, ledgerName string, w service.ExportWriter) error {
	return l.ledgerCluster.Export(ctx, ledgerName, w)
}

func (l ledgerClusterRouter) GetAllLogs(ctx context.Context, from uint64, to uint64) (service.Cursor[ledger.Log], error) {
	return l.ledgerCluster.GetAllLogs(ctx, from, to)
}

func (l ledgerClusterRouter) GetClusterState(ctx context.Context) (*ledger.ClusterState[ledger.LedgerState], error) {
	return l.localNode.GetClusterState(ctx)
}

var _ service.LedgerCluster = (*ledgerClusterRouter)(nil)
