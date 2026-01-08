package application

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"

	"github.com/formancehq/go-libs/v3/httpserver"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"
	grpcserver "github.com/formancehq/ledger-v3-poc/internal/grpc"
	httphandler "github.com/formancehq/ledger-v3-poc/internal/http"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	ledgerraft "github.com/formancehq/ledger-v3-poc/internal/raft/ledger"
	"github.com/formancehq/ledger-v3-poc/internal/raft/system"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/formancehq/ledger-v3-poc/internal/transport"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Options(
		transport.Module(),
		fx.Provide(
			raft.NewTransport,
			func(
				params struct {
				fx.In
				Config        system.Config
				Logger        logging.Logger
				Transport     *raft.GRPCTransport
				MeterProvider metric.MeterProvider
			},
			) (*system.Node, error) {
				return system.NewNode(params.Config, params.Logger, params.Transport, params.MeterProvider)
			},
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
		fx.Decorate(func(
			params struct {
			fx.In
			Handler       http.Handler
			MeterProvider *sdkmetric.MeterProvider      `optional:"true"`
			Exporter      *otlpmetrics.InMemoryExporter `optional:"true"`
		},
		) http.Handler {
			// If InMemoryExporter is available, wrap handler to add metrics endpoint
			if params.Exporter != nil && params.MeterProvider != nil {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/metrics" {
						otlpmetrics.NewInMemoryExporterHandler(params.MeterProvider, params.Exporter)(w, r)
						return
					}
					params.Handler.ServeHTTP(w, r)
				})
			}
			return params.Handler
		}),
		fx.Invoke(
			func(grpcServer *grpcserver.Server, transport *raft.GRPCTransport) error {
				raft.RegisterRaftTransportService(grpcServer.GetServer(), transport)
				return nil
			},
			func(grpcServer *grpcserver.Server, systemServiceServer service.SystemServiceServer) error {
				RegisterSystemService(grpcServer.GetServer(), systemServiceServer)
				return nil
			},
			func(grpcServer *grpcserver.Server, ledgerServiceServer ledgerpb.LedgerServiceServer) error {
				RegisterLedgerService(grpcServer.GetServer(), ledgerServiceServer)
				return nil
			},
			func(lc fx.Lifecycle, grpcServer *grpcserver.Server, logger logging.Logger) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						otlplogs.Go(func() {
							if err := grpcServer.Start(); err != nil {
								panic(err)
							}
						}, logger)
						return nil
					},
					OnStop: func(ctx context.Context) error {
						return grpcServer.Stop()
					},
				})
			},
			func(lc fx.Lifecycle, raftTransport *raft.GRPCTransport, logger logging.Logger) {
				lc.Append(fx.Hook{
					OnStop: raftTransport.Stop,
				})
			},
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
		return nil, ledgerpb.ErrNoLeader
	}

	grpcConn := adapter.connectionPool.GetConnection(adapter.systemNode.GetLeader())
	grpcClient := service.NewGrpcSystemClient(service.NewSystemServiceClient(grpcConn))

	return grpcClient, nil
}

func (adapter *systemNodeAdapter) IsHealthy() bool {
	return adapter.systemNode.IsHealthy()
}

func (adapter *systemNodeAdapter) GetAllLedgersInfo(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error) {
	mainCluster, err := adapter.getMainCluster()
	if err != nil {
		return nil, err
	}
	return mainCluster.GetAllLedgersInfo(ctx)
}

func (adapter *systemNodeAdapter) GetLedgerInfo(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	mainCluster, err := adapter.getMainCluster()
	if err != nil {
		return nil, err
	}
	return mainCluster.GetLedgerInfo(ctx, name)
}

func (adapter *systemNodeAdapter) GetClusterState(ctx context.Context) (*ledgerpb.ClusterState[ledgerpb.SystemState], error) {
	return adapter.systemNode.GetClusterState(ctx)
}

func (adapter *systemNodeAdapter) GetLedgerCluster(ctx context.Context, name string) (service.LedgerCluster, error) {
	mainCluster, err := adapter.getMainCluster()
	if err != nil {
		return nil, err
	}
	ledgerLeader, err := mainCluster.ResolveLedgerLeader(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("resolving ledger leader: %w", err)
	}
	ledgerLeaderNode := ledgerLeader & 0xFFFF // todo: should not be here

	if ledgerLeaderNode == adapter.systemNode.Node.Status().ID {
		return adapter.systemNode.GetLedgerNode(ctx, name)
	}

	grpcConn := adapter.connectionPool.GetConnection(ledgerLeaderNode)
	if grpcConn == nil {
		return nil, fmt.Errorf("no connection to ledger leader: %d", ledgerLeaderNode)
	}
	ledgerClusterClient := service.NewLedgerGrpcClient(name, ledgerpb.NewLedgerServiceClient(grpcConn))

	localNode, err := adapter.systemNode.GetLedgerNode(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("resolving local ledger node: %w", err)
	}

	return &ledgerClusterRouter{
		localNode:     localNode,
		ledgerCluster: ledgerClusterClient,
		logger:        adapter.logger,
	}, nil
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

func (adapter *systemNodeAdapter) CreateLedger(ctx context.Context, name string, logStoreConfig, runtimeStoreConfig map[string]interface{}, metadata map[string]string, snapshotThreshold *uint64, logStoreDriver, runtimeStoreDriver string) (*ledgerpb.LedgerInfo, error) {
	cluster, err := adapter.getMainCluster()
	if err != nil {
		return nil, err
	}
	return cluster.CreateLedger(ctx, name, logStoreConfig, runtimeStoreConfig, metadata, snapshotThreshold, logStoreDriver, runtimeStoreDriver)
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

func (l ledgerClusterRouter) CreateTransaction(ctx context.Context, ledgerName string, parameters service.Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, *ledgerpb.CreatedTransaction, error) {
	return l.ledgerCluster.CreateTransaction(ctx, ledgerName, parameters)
}

func (l ledgerClusterRouter) RevertTransaction(ctx context.Context, ledgerName string, parameters service.Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, *ledgerpb.RevertedTransaction, error) {
	return l.ledgerCluster.RevertTransaction(ctx, ledgerName, parameters)
}

func (l ledgerClusterRouter) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return l.ledgerCluster.SaveTransactionMetadata(ctx, ledgerName, parameters)
}

func (l ledgerClusterRouter) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return l.ledgerCluster.SaveAccountMetadata(ctx, ledgerName, parameters)
}

func (l ledgerClusterRouter) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return l.ledgerCluster.DeleteTransactionMetadata(ctx, ledgerName, parameters)
}

func (l ledgerClusterRouter) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return l.ledgerCluster.DeleteAccountMetadata(ctx, ledgerName, parameters)
}

func (l ledgerClusterRouter) Import(ctx context.Context, ledgerName string, stream chan *ledgerpb.Log) error {
	return l.ledgerCluster.Import(ctx, ledgerName, stream)
}

func (l ledgerClusterRouter) Export(ctx context.Context, ledgerName string, w service.ExportWriter) error {
	return l.ledgerCluster.Export(ctx, ledgerName, w)
}

func (l ledgerClusterRouter) GetAllLogs(ctx context.Context, from uint64, to uint64) (service.Cursor[*ledgerpb.Log], error) {
	return l.ledgerCluster.GetAllLogs(ctx, from, to)
}

func (l ledgerClusterRouter) GetClusterState(ctx context.Context) (*ledgerpb.ClusterState[ledgerpb.LedgerState], error) {
	return l.localNode.GetClusterState(ctx)
}

var _ service.LedgerCluster = (*ledgerClusterRouter)(nil)
