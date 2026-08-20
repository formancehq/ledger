package bootstrap

import (
	"fmt"
	"net/http"

	"go.uber.org/fx"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/formancehq/go-libs/v5/pkg/fx/transportfx"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/transport/httpserver"

	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/pkg/network"
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
			func(cfg Config, lc fx.Lifecycle, logger logging.Logger, bindings network.Bindings) (*grpcadp.ServiceServer, error) {
				tlsCfg, reloader, err := ServerTLSConfig(cfg.TLSConfig)
				if err != nil {
					return nil, fmt.Errorf("loading TLS config for restore server: %w", err)
				}

				RegisterCertReloaderLifecycle(lc, reloader, logger)

				// An injected bindings.Service listener decides the bind
				// address instead of host. Only tests inject one, and they bind
				// loopback; production supplies the zero value, so
				// --restore-listen stays the only lever in a deployment.
				host := cfg.EffectiveRestoreListen()

				if host != "127.0.0.1" && host != "localhost" && host != "::1" {
					logger.WithFields(map[string]any{"restoreListen": host}).
						Errorf("WARNING: restore mode bound to a non-loopback address (%s). The restore RPCs are not authenticated; ensure TLS and upstream firewalling are in place.", host)
				}

				return grpcadp.NewServiceServer(host, cfg.GRPCPort, logger, cfg.Debug, cfg.GRPCSlowThreshold, tlsCfg, cfg.TLSConfig.Mode.AllowsPlaintext(),
					listenerOptions(bindings.Service)...)
			},
			func(cfg Config, logger logging.Logger) *grpcadp.RestoreServiceServerImpl {
				return grpcadp.NewRestoreServiceServer(cfg.DataDir, cfg.ClusterID, cfg.RestoreDownloadParallelism, logger)
			},
		),
		fx.Invoke(
			// Registered first so it runs last on stop: every injected listener
			// is released once the servers that adopted them have stopped.
			func(lc fx.Lifecycle, bindings network.Bindings) {
				lc.Append(releaseBindingsHook(bindings))
			},
			// Validate that the data directory is fresh: no checkpoints, no
			// live/ database (normal startup prefers it over the restored
			// checkpoint, silently booting the stale store under the
			// marker's boundary), no leftover RESTORED marker.
			func(cfg Config) error {
				// Marker first: ValidateFreshRestoreTarget's reclaim of a
				// half-finalized checkpoint is only safe once the marker is
				// known absent.
				if marker, err := node.ReadRestoredMarker(cfg.DataDir); err != nil {
					return fmt.Errorf("checking for RESTORED marker: %w", err)
				} else if marker != nil {
					return fmt.Errorf("restore mode requires a fresh data directory: a RESTORED marker already exists in %s", cfg.DataDir)
				}

				return dal.ValidateFreshRestoreTarget(cfg.DataDir)
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
				shutdowner fx.Shutdowner,
			) {
				lc.Append(grpcServerHook(grpcServerHookConfig{
					Server:          serviceServer,
					Name:            "restore-mode gRPC server",
					Logger:          logger,
					RequestShutdown: shutdownRequester(shutdowner),
				}))
			},
			// Start minimal HTTP server with /health only, bound to the same
			// host as the gRPC restore server (see comment on RestoreModule).
			func(lc fx.Lifecycle, cfg Config, bindings network.Bindings) {
				mux := http.NewServeMux()
				mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"status":"restore_mode"}`))
				})

				lc.Append(transportfx.FXHook(httpserver.NewHook(mux,
					httpListenerOption(bindings.HTTP, fmt.Sprintf("%s:%d", cfg.EffectiveRestoreListen(), cfg.HTTPPort)),
				)))
			},
		),
	)
}
