package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/cloud/aws/iam"
	"github.com/formancehq/go-libs/v5/pkg/fx/authnfx"
	"github.com/formancehq/go-libs/v5/pkg/fx/messagingfx"
	"github.com/formancehq/go-libs/v5/pkg/fx/observefx"
	"github.com/formancehq/go-libs/v5/pkg/fx/storagefx"
	"github.com/formancehq/go-libs/v5/pkg/fx/transportfx"
	"github.com/formancehq/go-libs/v5/pkg/messaging/publish"
	"github.com/formancehq/go-libs/v5/pkg/observe"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/observe/metrics"
	"github.com/formancehq/go-libs/v5/pkg/observe/traces"
	"github.com/formancehq/go-libs/v5/pkg/service"
	"github.com/formancehq/go-libs/v5/pkg/service/health"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"
	apilib "github.com/formancehq/go-libs/v5/pkg/transport/api"
	"github.com/formancehq/go-libs/v5/pkg/transport/httpserver"

	"github.com/formancehq/ledger/internal/api"
	"github.com/formancehq/ledger/internal/bus"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/replication"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/replication/drivers/alldrivers"
	"github.com/formancehq/ledger/internal/storage"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/internal/tracing"
	"github.com/formancehq/ledger/internal/worker"
)

type ServeCommandConfig struct {
	commonConfig        `mapstructure:",squash"`
	WorkerConfiguration `mapstructure:",squash"`

	Bind                   string `mapstructure:"bind"`
	BallastSizeInBytes     uint   `mapstructure:"ballast-size"`
	NumscriptCacheMaxCount uint   `mapstructure:"numscript-cache-max-count"`
	AutoUpgrade            bool   `mapstructure:"auto-upgrade"`
	BulkMaxSize            int    `mapstructure:"bulk-max-size"`
	BulkParallel           int    `mapstructure:"bulk-parallel"`
	DefaultPageSize        uint64 `mapstructure:"default-page-size"`
	MaxPageSize            uint64 `mapstructure:"max-page-size"`
	WorkerEnabled          bool   `mapstructure:"worker"`
	WorkerAddress          string `mapstructure:"worker-grpc-address"`
}

const (
	BindFlag                   = "bind"
	BallastSizeInBytesFlag     = "ballast-size"
	NumscriptCacheMaxCountFlag = "numscript-cache-max-count"
	AutoUpgradeFlag            = "auto-upgrade"
	BulkMaxSizeFlag            = "bulk-max-size"
	BulkParallelFlag           = "bulk-parallel"

	DefaultPageSizeFlag   = "default-page-size"
	MaxPageSizeFlag       = "max-page-size"
	WorkerEnabledFlag     = "worker"
	SemconvMetricsNames   = "semconv-metrics-names"
	SchemaEnforcementMode = "schema-enforcement-mode"
)

func NewServeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "serve",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {

			cfg, err := LoadConfig[ServeCommandConfig](cmd)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			if err := cfg.Validate(); err != nil {
				return err
			}

			connectionOptions, err := connect.ConnectionOptionsFromFlags(cmd.Flags(), cmd.Context())
			if err != nil {
				return err
			}

			options := []fx.Option{
				fx.NopLogger,
				otlpModule(cmd, cfg.commonConfig),
				messagingfx.PublishModuleFromFlags(cmd, service.IsDebug(cmd)),
				authnfx.JWTModuleFromFlags(cmd),
				fx.Supply(connectionOptions),
				storagefx.BunConnectModule(*connectionOptions, service.IsDebug(cmd)),
				storage.NewFXModule(storage.ModuleConfig{
					AutoUpgrade: cfg.AutoUpgrade,
				}),
				drivers.NewFXModule(),
				fx.Invoke(alldrivers.Register),
				systemcontroller.NewFXModule(systemcontroller.ModuleConfiguration{
					NumscriptInterpreter:      cfg.NumscriptInterpreter,
					NumscriptInterpreterFlags: cfg.NumscriptInterpreterFlags,
					NSCacheConfiguration: ledgercontroller.CacheConfiguration{
						MaxCount: cfg.NumscriptCacheMaxCount,
					},
					DatabaseRetryConfiguration: systemcontroller.DatabaseRetryConfiguration{
						MaxRetry: 10,
						Delay:    time.Millisecond * 100,
					},
					EnableFeatures:        cfg.ExperimentalFeaturesEnabled,
					SchemaEnforcementMode: cfg.commonConfig.SchemaEnforcementMode,
				}),
				bus.NewFxModule(),
				ballastModule(cfg.BallastSizeInBytes),
				api.Module(api.Config{
					Version: Version,
					Debug:   service.IsDebug(cmd),
					Bulk: api.BulkConfig{
						MaxSize:  cfg.BulkMaxSize,
						Parallel: cfg.BulkParallel,
					},
					Pagination: storagecommon.PaginationConfig{
						MaxPageSize:     cfg.MaxPageSize,
						DefaultPageSize: cfg.DefaultPageSize,
					},
					Exporters: cfg.ExperimentalExporters,
				}),
				fx.Decorate(func(
					params struct {
						fx.In

						Handler          chi.Router
						HealthController *health.HealthController
						Logger           logging.Logger

						MeterProvider *metric.MeterProvider     `optional:"true"`
						Exporter      *metrics.InMemoryExporter `optional:"true"`
					},
				) chi.Router {
					return assembleFinalRouter(
						service.IsDebug(cmd),
						params.MeterProvider,
						params.Exporter,
						params.HealthController,
						params.Logger,
						params.Handler,
					)
				}),
				fx.Invoke(func(lc fx.Lifecycle, h chi.Router) {
					lc.Append(transportfx.FXHook(httpserver.NewHook(h, httpserver.WithAddress(cfg.Bind))))
				}),
			}

			if cfg.WorkerEnabled {
				options = append(options,
					newWorkerModule(cfg.WorkerConfiguration),
					replication.NewFXEmbeddedClientModule(),
				)
			} else {
				options = append(options,
					worker.NewGRPCClientFxModule(
						cfg.WorkerAddress,
						grpc.WithTransportCredentials(insecure.NewCredentials()),
					),
					replication.NewFXGRPCClientModule(),
				)
			}

			return service.New(cmd.OutOrStdout(), options...).Run(cmd)
		},
	}
	cmd.Flags().Uint(BallastSizeInBytesFlag, 0, "Ballast size in bytes, default to 0")
	cmd.Flags().Uint(NumscriptCacheMaxCountFlag, 1024, "Numscript cache max count")
	cmd.Flags().Bool(AutoUpgradeFlag, false, "Automatically upgrade all schemas")
	cmd.Flags().String(BindFlag, "0.0.0.0:3068", "API bind address")
	cmd.Flags().Int(BulkMaxSizeFlag, api.DefaultBulkMaxSize, "Bulk max size (default 100)")
	cmd.Flags().Int(BulkParallelFlag, 10, "Bulk max parallelism")
	cmd.Flags().Uint64(MaxPageSizeFlag, 100, "Max page size")
	cmd.Flags().Uint64(DefaultPageSizeFlag, 15, "Default page size")
	cmd.Flags().Bool(WorkerEnabledFlag, false, "Enable worker")
	cmd.Flags().Bool(ExperimentalFeaturesFlag, false, "Enable features configurability")
	cmd.Flags().Bool(NumscriptInterpreterFlag, false, "Enable experimental numscript rewrite")
	cmd.Flags().StringSlice(NumscriptInterpreterFlagsToPass, nil, "Feature flags to pass to the experimental numscript interpreter")
	cmd.Flags().String(WorkerGRPCAddressFlag, "localhost:8081", "GRPC address")
	cmd.Flags().Bool(SemconvMetricsNames, false, "Use semconv metrics names (recommended)")
	cmd.Flags().String(SchemaEnforcementMode, "audit", "Schema enforcement mode. Values: `audit`, `strict`")

	addWorkerFlags(cmd)
	connect.AddFlags(cmd.Flags())
	observe.AddFlags(cmd.Flags())
	metrics.AddFlags(cmd.Flags())
	traces.AddFlags(cmd.Flags())
	jwt.AddFlags(cmd.Flags())
	publish.AddFlags(ServiceName, cmd.Flags(), func(cd *publish.ConfigDefault) {
		cd.PublisherCircuitBreakerSchema = systemstore.SchemaSystem
	})
	iam.AddFlags(cmd.Flags())

	return cmd
}

func assembleFinalRouter(
	exportPProf bool,
	meterProvider *metric.MeterProvider,
	exporter *metrics.InMemoryExporter,
	healthController *health.HealthController,
	logger logging.Logger,
	handler http.Handler,
) *chi.Mux {
	wrappedRouter := chi.NewRouter()
	wrappedRouter.Use(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			r = r.WithContext(logging.ContextWithLogger(r.Context(), logger))

			handler.ServeHTTP(w, r)
		})
	})
	wrappedRouter.Route("/_/", func(r chi.Router) {
		if exporter != nil {
			r.Handle("/metrics", metrics.NewInMemoryExporterHandler(
				meterProvider,
				exporter,
			))
		}
		if exportPProf {
			r.Handle("/debug/pprof/*", http.StripPrefix(
				"/_",
				http.HandlerFunc(pprof.Index),
			))
		}
		r.Handle("/healthcheck", http.HandlerFunc(healthController.Check))
		r.Get("/info", func(w http.ResponseWriter, r *http.Request) {
			apilib.RawOk(w, struct {
				Server  string `json:"server"`
				Version string `json:"version"`
			}{
				Server:  "ledger",
				Version: Version,
			})
		})
	})
	wrappedRouter.Get("/_healthcheck", healthController.Check)
	wrappedRouter.Mount("/", handler)

	return wrappedRouter
}

func ballastModule(sizeInBytes uint) fx.Option {
	if sizeInBytes == 0 {
		return fx.Options()
	}
	return fx.Invoke(func(lc fx.Lifecycle) {
		var ballast []byte
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				ballast = make([]byte, 0, sizeInBytes)
				_ = ballast
				return nil
			},
			OnStop: func(ctx context.Context) error {
				ballast = nil
				return nil
			},
		})
	})
}

func otlpModule(cmd *cobra.Command, cfg commonConfig) fx.Option {
	return fx.Options(
		observefx.ResourceModuleFromFlags(cmd, observe.WithServiceVersion(Version)),
		observefx.TracesModuleFromFlags(cmd),
		observefx.ProvideMetricsProviderOption(func() metric.Option {
			return metric.WithView(func(instrument metric.Instrument) (metric.Stream, bool) {
				if cfg.SemconvMetricsNames {
					return metric.Stream{}, false
				}
				return metric.Stream{
					Name:        tracing.LegacyMetricsName(instrument.Name),
					Description: instrument.Description,
					Unit:        instrument.Unit,
				}, true
			})
		}),
		observefx.MetricsModuleFromFlags(cmd),
	)
}
