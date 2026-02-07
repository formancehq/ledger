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
	http2 "github.com/formancehq/ledger-v3-poc/internal/compat/http"
	"github.com/formancehq/ledger-v3-poc/internal/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/monitoring/otlplogs"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/admission"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/service/state"
	"github.com/formancehq/ledger-v3-poc/internal/service/transport"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/formancehq/ledger-v3-poc/internal/storage/spool"
	"github.com/formancehq/ledger-v3-poc/internal/storage/wal"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
	health "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func Module() fx.Option {
	return fx.Options(
		transport.Module(),
		attributes.Module(),
		fx.Provide(
			func(
				cfg Config,
				logger logging.Logger,
				connectionPool *transport.ConnectionPool,
				meterProvider metric.MeterProvider,
			) *node.DefaultTransport {
				return node.NewTransport(
					logger,
					connectionPool,
					meterProvider,
					cfg.RaftConfig.NodeID,
					cfg.TransportConfig,
				)
			},
			func(cfg Config, meterProvider metric.MeterProvider, logger logging.Logger) (*data.Store, error) {
				return data.NewStore(
					cfg.DataDir,
					logger,
					meterProvider.Meter("pebble.runtime_store"),
					cfg.PebbleConfig,
				)
			},
			func(cfg Config, logger logging.Logger, meterProvider metric.MeterProvider) (*wal.DefaultWAL, error) {
				return wal.New(cfg.RaftConfig.WalDir, logger.WithFields(map[string]any{
					"cmp": "wal",
				}), meterProvider.Meter("wal"))
			},
			func(cfg Config) (*spool.Default, error) {
				return spool.NewDefault(spool.DefaultSpoolConfig{
					Dir: filepath.Join(cfg.RaftConfig.WalDir, "spool"),
				})
			},
			func(transport *node.DefaultTransport) state.SnapshotFetcherProvider {
				return ctrl.GRPCSnapshotFetcherProvider(transport)
			},
			func(cfg node.NodeConfig, meterProvider metric.MeterProvider) (*cache.Cache, error) {
				return cache.New(cfg.RotationThreshold, meterProvider.Meter("cache"))
			},
			func(
				params struct {
					fx.In
					Config                  node.NodeConfig
					Logger                  logging.Logger
					Transport               *node.DefaultTransport
					MeterProvider           metric.MeterProvider
					Store                   *data.Store
					WAL                     *wal.DefaultWAL
					Spool                   *spool.Default
					SnapshotFetcherProvider state.SnapshotFetcherProvider
					Cache                   *cache.Cache
					Attrs                   *attributes.Attributes
				},
			) (*node.Node, error) {
				return node.NewNode(
					params.Config,
					params.Transport,
					params.Store,
					params.Logger,
					params.MeterProvider.Meter("raft.node"),
					params.Spool,
					params.WAL,
					params.SnapshotFetcherProvider,
					params.Cache,
					params.Attrs,
				)
			},
			func(cfg Config) node.NodeConfig {
				cfg.RaftConfig.SetDefaults()
				return cfg.RaftConfig
			},
			func(cfg Config) node.TransportConfig {
				return cfg.TransportConfig
			},
			func(cfg Config, logger logging.Logger) (*Server, error) {
				_, raftPort, err := net.SplitHostPort(cfg.RaftConfig.BindAddr)
				if err != nil {
					return nil, fmt.Errorf("invalid bind address format: %w", err)
				}
				grpcPort, err := strconv.Atoi(raftPort)
				if err != nil {
					return nil, fmt.Errorf("invalid port in bind address: %w", err)
				}

				return NewServer(grpcPort, logger, cfg.Debug), nil
			},
			func(logger logging.Logger, ctrl ctrl.Controller, s *data.Store) servicepb.BucketServiceServer {
				return NewBucketServiceServer(logger, ctrl, s)
			},
			func(logger logging.Logger, s *data.Store) snapshotpb.SnapshotServiceServer {
				return NewSnapshotServiceServer(logger, s)
			},
			func(node *node.Node) clusterpb.ClusterServiceServer {
				return NewClusterServiceServer(node)
			},
			http2.NewServer,
			http2.NewHandler,
			func(node *node.Node, ctrl ctrl.Controller) http2.Backend {
				return http2.NewDefaultBackend(node, ctrl)
			},
			func(
				node *node.Node,
				cache *cache.Cache,
				store *data.Store,
				logger logging.Logger,
				attrs *attributes.Attributes,
				meterProvider metric.MeterProvider,
			) ctrl.Admission {
				return admission.NewAdmission(
					cache,
					store,
					logger,
					node,
					attrs,
					meterProvider,
				)
			},
			func(
				raftNode *node.Node,
				connectionPool *transport.ConnectionPool,
				admission ctrl.Admission,
				store *data.Store,
				logger logging.Logger,
				attrs *attributes.Attributes,
			) ctrl.Controller {
				return NewRoutedController(
					ctrl.NewDefaultController(admission, store, logger, attrs),
					raftNode,
					connectionPool,
				)
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
				runtime *data.Store,
				wal *wal.DefaultWAL,
				logger logging.Logger,
			) {
				lc.Append(fx.Hook{
					OnStop: func(ctx context.Context) error {
						return wal.Close()
					},
				})
				lc.Append(fx.Hook{
					OnStop: func(_ context.Context) error {
						return runtime.Close()
					},
				})
			},
			func(
				lc fx.Lifecycle,
				t *node.DefaultTransport,
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
			func(grpcServer *Server, transport *node.DefaultTransport) error {
				hs := health.NewServer()
				healthpb.RegisterHealthServer(grpcServer.GetServer(), hs)
				node.RegisterRaftTransportService(grpcServer.GetServer(), transport)
				hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
				return nil
			},
			func(grpcServer *Server, bucketServiceServer servicepb.BucketServiceServer) error {
				RegisterBucketService(grpcServer.GetServer(), bucketServiceServer)
				return nil
			},
			func(grpcServer *Server, snapshotServiceServer snapshotpb.SnapshotServiceServer) error {
				RegisterSnapshotService(grpcServer.GetServer(), snapshotServiceServer)
				return nil
			},
			func(grpcServer *Server, clusterServiceServer clusterpb.ClusterServiceServer) error {
				RegisterClusterService(grpcServer.GetServer(), clusterServiceServer)
				return nil
			},
			func(
				lc fx.Lifecycle,
				grpcServer *Server,
				logger logging.Logger,
				defaultTransport *node.DefaultTransport,
				cfg node.NodeConfig,
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
			func(lc fx.Lifecycle, node *node.Node, logger logging.Logger) (*node.Node, error) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						ready := make(chan struct{})
						otlplogs.Go(func() {
							if err := node.Run(context.WithoutCancel(ctx), ready); err != nil {
								panic(err)
							}
						}, logger)
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-ready:
							logger.Infof("Raft cluster started successfully")
							return nil
						}
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
