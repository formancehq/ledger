package bootstrap

import (
	"context"
	"fmt"
	"net/http"

	"go.uber.org/fx"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/formancehq/go-libs/v5/pkg/fx/transportfx"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/transport/httpserver"

	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/infra/monitoring/otlplogs"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// RestoreModule returns a minimal fx module for restore mode.
// It only starts a gRPC server with the RestoreService and a health endpoint.
// No Raft, WAL, transport, or other production services are started.
//
// The restore RPCs are not authenticated. To avoid exposing destructive
// operations on the public network, this module binds gRPC + HTTP to
// cfg.RestoreListen (defaults to 127.0.0.1). An operator can opt out with
// --restore-listen=0.0.0.0 (or a specific interface), but this should only
// be combined with TLS and upstream firewalling.
func RestoreModule() fx.Option {
	return fx.Options(
		fx.Provide(
			func(cfg Config, logger logging.Logger) (*grpcadp.ServiceServer, error) {
				tlsCfg, err := ServerTLSConfig(cfg.TLSConfig)
				if err != nil {
					return nil, fmt.Errorf("loading TLS config for restore server: %w", err)
				}

				host := cfg.EffectiveRestoreListen()

				if host != "127.0.0.1" && host != "localhost" && host != "::1" {
					logger.WithFields(map[string]any{"restoreListen": host}).
						Errorf("WARNING: restore mode bound to a non-loopback address (%s). The restore RPCs are not authenticated; ensure TLS and upstream firewalling are in place.", host)
				}

				return grpcadp.NewServiceServer(host, cfg.GRPCPort, logger, cfg.Debug, cfg.GRPCSlowThreshold, tlsCfg, cfg.TLSConfig.Mode.AllowsPlaintext())
			},
			func(cfg Config, logger logging.Logger) *grpcadp.RestoreServiceServerImpl {
				return grpcadp.NewRestoreServiceServer(cfg.DataDir, cfg.ClusterID, cfg.RestoreDownloadParallelism, logger)
			},
		),
		fx.Invoke(
			// Validate that the data directory is fresh (no existing checkpoints)
			func(cfg Config) error {
				_, hasCheckpoint, err := dal.ScanLatestCheckpointID(cfg.DataDir)
				if err != nil {
					return fmt.Errorf("scanning data directory: %w", err)
				}

				if hasCheckpoint {
					return fmt.Errorf("restore mode requires a fresh data directory; checkpoints already exist in %s", cfg.DataDir)
				}

				return nil
			},
			// Register health service on ServiceServer
			func(serviceServer *grpcadp.ServiceServer) {
				hs := health.NewServer()
				healthpb.RegisterHealthServer(serviceServer.GetServer(), hs)
				hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
			},
			// Register RestoreService on ServiceServer
			func(serviceServer *grpcadp.ServiceServer, restoreServer *grpcadp.RestoreServiceServerImpl) {
				grpcadp.RegisterRestoreService(serviceServer.GetServer(), restoreServer)
			},
			// Start ServiceServer
			func(
				lc fx.Lifecycle,
				serviceServer *grpcadp.ServiceServer,
				logger logging.Logger,
			) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						logger.Infof("Starting restore-mode gRPC server")

						listening := make(chan struct{})

						otlplogs.Go(func() {
							err := serviceServer.Start(listening)
							if err != nil {
								panic(err)
							}
						}, logger)

						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-listening:
						}

						logger.Infof("Restore-mode gRPC server started successfully")

						return nil
					},
					OnStop: func(_ context.Context) error {
						logger.Infof("Stopping restore-mode gRPC server")

						return serviceServer.Stop()
					},
				})
			},
			// Start minimal HTTP server with /health only, bound to the same
			// host as the gRPC restore server (see comment on RestoreModule).
			func(lc fx.Lifecycle, cfg Config) {
				mux := http.NewServeMux()
				mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"status":"restore_mode"}`))
				})

				lc.Append(transportfx.FXHook(httpserver.NewHook(mux,
					httpserver.WithAddress(fmt.Sprintf("%s:%d", cfg.EffectiveRestoreListen(), cfg.HTTPPort)),
				)))
			},
		),
	)
}
