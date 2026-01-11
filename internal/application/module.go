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
	"github.com/formancehq/ledger-v3-poc/internal/raft/system"
	"github.com/formancehq/ledger-v3-poc/internal/systempb"
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
					Config        system.NodeConfig
					Logger        logging.Logger
					Transport     *raft.GRPCTransport
					MeterProvider metric.MeterProvider
				},
			) (*system.Node, error) {
				return system.NewNode(params.Config, params.Logger, params.Transport, params.MeterProvider)
			},
			func(cfg Config) system.NodeConfig {
				return cfg.RaftConfig
			},
			func(cfg Config) raft.TransportConfig {
				return cfg.TransportConfig
			},
			func(cfg system.NodeConfig, logger logging.Logger) (*grpcserver.Server, error) {
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
			func(node *system.Node, connectionPool *transport.ConnectionPool, cfg system.NodeConfig) httphandler.Backend {
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
			func(grpcServer *grpcserver.Server, transport *raft.GRPCTransport) error {
				raft.RegisterRaftTransportService(grpcServer.GetServer(), transport)
				return nil
			},
			func(grpcServer *grpcserver.Server, systemServiceServer systempb.SystemServiceServer) error {
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
