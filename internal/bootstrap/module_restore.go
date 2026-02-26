package bootstrap

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/formancehq/go-libs/v3/httpserver"
	"github.com/formancehq/go-libs/v3/logging"
	grpcadp "github.com/formancehq/ledger-v3-poc/internal/adapter/grpc"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/otlplogs"
	"go.uber.org/fx"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)



// RestoreModule returns a minimal fx module for restore mode.
// It only starts a gRPC server with the RestoreService and a health endpoint.
// No Raft, WAL, transport, or other production services are started.
func RestoreModule() fx.Option {
	return fx.Options(
		fx.Provide(
			func(cfg Config, logger logging.Logger) (*grpcadp.ServiceServer, error) {
				tlsOpt, err := ServerCredentials(cfg.TLSConfig)
				if err != nil {
					return nil, fmt.Errorf("loading TLS credentials for restore server: %w", err)
				}
				return grpcadp.NewServiceServer(cfg.GRPCPort, logger, cfg.Debug, tlsOpt), nil
			},
			func(cfg Config, logger logging.Logger) *grpcadp.RestoreServiceServerImpl {
				return grpcadp.NewRestoreServiceServer(cfg.DataDir, logger)
			},
		),
		fx.Invoke(
			// Validate that the data directory is fresh (no CURRENT_CHECKPOINT)
			func(cfg Config) error {
				cpPath := filepath.Join(cfg.DataDir, "CURRENT_CHECKPOINT")
				if _, err := os.Stat(cpPath); err == nil {
					return fmt.Errorf("restore mode requires a fresh data directory; %s already exists", cpPath)
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
							if err := serviceServer.Start(listening); err != nil {
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
			// Start minimal HTTP server with /health only
			func(lc fx.Lifecycle, cfg Config) {
				mux := http.NewServeMux()
				mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"status":"restore_mode"}`))
				})
				lc.Append(httpserver.NewHook(mux,
					httpserver.WithAddress(fmt.Sprintf(":%d", cfg.HTTPPort)),
				))
			},
		),
	)
}
