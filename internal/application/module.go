package application

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
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
	"github.com/formancehq/ledger-v3-poc/internal/wal"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
	health "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func Module() fx.Option {
	return fx.Options(
		transport.Module(),
		fx.Provide(
			func(
				cfg Config,
				logger logging.Logger,
				connectionPool *transport.ConnectionPool,
				meterProvider metric.MeterProvider,
			) *raft.DefaultTransport {
				return raft.NewTransport(
					logger,
					connectionPool,
					meterProvider,
					cfg.RaftConfig.NodeID,
					cfg.TransportConfig,
				)
			},
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
			func(cfg Config, logger logging.Logger, meterProvider metric.MeterProvider) (*wal.WAL, error) {
				return wal.New(cfg.RaftConfig.WalDir, logger.WithFields(map[string]any{
					"cmp": "wal",
				}), meterProvider.Meter("wal"))
			},
			func(cfg Config) (*raft.DefaultSpool, error) {
				return raft.NewDefaultSpool(raft.DefaultSpoolConfig{
					Dir: filepath.Join(cfg.RaftConfig.WalDir, "spool"),
				})
			},
			func(transport *raft.DefaultTransport) raft.LogStreamerProvider {
				return raft.GRPCLogStreamerProvider(transport)
			},
			func(
				params struct {
					fx.In
					Config            raft.NodeConfig
					Logger            logging.Logger
					Transport         *raft.DefaultTransport
					MeterProvider     metric.MeterProvider
					Store             store.Store
					WAL               *wal.WAL
					Spool             *raft.DefaultSpool
					LogReaderProvider raft.LogStreamerProvider
				},
			) (*raft.Node, error) {
				return raft.NewNode(
					params.Config,
					params.Transport,
					params.Store,
					params.Logger,
					params.MeterProvider.Meter("raft.node"),
					params.Spool,
					params.WAL,
					params.LogReaderProvider,
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
			func(
				lc fx.Lifecycle,
				runtime store.Store,
				wal *wal.WAL,
				logger logging.Logger,
			) {
				lc.Append(fx.Hook{
					OnStop: func(ctx context.Context) error {
						return wal.Close()
					},
				})
				lc.Append(fx.Hook{
					OnStop: runtime.Close,
				})
			},
			func(
				lc fx.Lifecycle,
				t *raft.DefaultTransport,
				logger logging.Logger,
			) {
				lc.Append(fx.Hook{
					OnStop: func(ctx context.Context) error {
						logger.Infof("Stopping raft transport")
						return t.Stop(ctx)
					},
					OnStart: func(ctx context.Context) error {
						otlplogs.Go(func() {
							t.Start(context.WithoutCancel(ctx))
						}, logger)
						return nil
					},
				})
			},
			func(grpcServer *grpcserver.Server, transport *raft.DefaultTransport) error {
				hs := health.NewServer()
				healthpb.RegisterHealthServer(grpcServer.GetServer(), hs)
				raft.RegisterRaftTransportService(grpcServer.GetServer(), transport)
				hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
				return nil
			},
			func(grpcServer *grpcserver.Server, ledgerServiceServer ledgerpb.LedgerServiceServer) error {
				RegisterLedgerService(grpcServer.GetServer(), ledgerServiceServer)
				return nil
			},
			func(
				lc fx.Lifecycle,
				grpcServer *grpcserver.Server,
				logger logging.Logger,
				defaultTransport *raft.DefaultTransport,
				cfg raft.NodeConfig,
			) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						logger.Infof("Starting GRPC server")
						listening := make(chan struct{})
						otlplogs.Go(func() {
							if err := grpcServer.Start(listening); err != nil {
								panic(err)
							}
						}, logger)

						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-listening:
						}

						logger.Infof("GRPC server started successfully")
						for _, peerEntry := range cfg.Peers {
							logger := logger.WithFields(map[string]any{"peer": peerEntry})
							logger.Debugf("Adding peer to transport")
							defaultTransport.AddPeer(peerEntry.ID, peerEntry.Address)
						}

						return nil
					},
					OnStop: func(ctx context.Context) error {
						logger.Infof("Stopping GRPC server")
						return grpcServer.Stop()
					},
				})
			},
			func(lc fx.Lifecycle, node *raft.Node, logger logging.Logger) (*raft.Node, error) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						otlplogs.Go(func() {
							if err := node.Run(context.WithoutCancel(ctx)); err != nil {
								panic(err)
							}
						}, logger)
						logger.Infof("Raft cluster started successfully")
						return nil
					},
					OnStop: func(ctx context.Context) error {
						logger.Infof("Shutting down raft cluster")
						if err := node.Stop(ctx); err != nil {
							return fmt.Errorf("shutting down raft cluster: %w", err)
						}
						logger.Infof("Raft cluster stopped successfully")
						return nil
					},
				})

				return node, nil
			},
			func(lc fx.Lifecycle, cfg Config, handler http.Handler) {
				lc.Append(httpserver.NewHook(handler,
					httpserver.WithAddress(fmt.Sprintf(":%d", cfg.HTTPPort)),
				))
			},
		),
	)
}
