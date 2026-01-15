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
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/formancehq/ledger-v3-poc/internal/store/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/store/sqlite"
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
			func(cfg Config, meterProvider metric.MeterProvider, logger logging.Logger) (store.Store, error) {
				switch cfg.StorageType {
				case "pebble":
					return pebble.NewStore(
						cfg.DataDir,
						logger,
						meterProvider.Meter("peeble.runtime_store"),
					)
				case "sqlite-mattn":
					return sqlite.NewMattnStore(cfg.DataDir, logger)
				case "sqlite-modernc":
					return sqlite.NewModernStore(cfg.DataDir, logger)
				default:
					return nil, fmt.Errorf("invalid storage type: %s", cfg.StorageType)
				}
			},
			func(
				params struct {
					fx.In
					Config        raft.NodeConfig
					Logger        logging.Logger
					Transport     *raft.GRPCTransport
					MeterProvider metric.MeterProvider
					RuntimeStore  store.Store
				},
			) (*raft.Node, error) {
				return raft.NewNode(
					params.Config,
					params.Transport,
					params.RuntimeStore,
					params.Logger,
					params.MeterProvider.Meter("raft.node"),
				)
			},
			func(cfg Config) raft.NodeConfig {
				return cfg.RaftConfig
			},
			func(cfg Config) raft.TransportConfig {
				return cfg.TransportConfig
			},
			func(cfg raft.NodeConfig, logger logging.Logger) (*grpcserver.Server, error) {
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
			NewLedgerServiceServer,
			httphandler.NewServer,
			httphandler.NewHandler,
			func(node *raft.Node, connectionPool *transport.ConnectionPool, cfg raft.NodeConfig) httphandler.Backend {
				return httphandler.NewDefaultBackend(node, connectionPool, cfg.NodeID)
			},
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
			func(lc fx.Lifecycle, runtime store.Store) {
				lc.Append(fx.Hook{
					OnStop: runtime.Close,
				})
			},
			func(grpcServer *grpcserver.Server, transport *raft.GRPCTransport) error {
				raft.RegisterRaftTransportService(grpcServer.GetServer(), transport)
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
			func(lc fx.Lifecycle, systemNode *raft.Node, logger logging.Logger) (*raft.Node, error) {
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
