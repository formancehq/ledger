package cmd

import (
	"fmt"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/replication"
	drivers "github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/replication/drivers/alldrivers"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net/http"
	"net/http/pprof"
	"time"

	apilib "github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/health"
	"github.com/formancehq/go-libs/v3/httpserver"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/sdk/metric"

	"github.com/formancehq/ledger/internal/bus"

	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/aws/iam"
	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/go-libs/v3/publish"
	"github.com/formancehq/ledger/internal/api"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/storage"

	"github.com/formancehq/go-libs/v3/ballast"
	"github.com/formancehq/go-libs/v3/service"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
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

	DefaultPageSizeFlag = "default-page-size"
	MaxPageSizeFlag     = "max-page-size"
	WorkerEnabledFlag   = "worker"
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

			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
			if err != nil {
				return err
			}

			options := []fx.Option{
				fx.NopLogger,
				otlp.FXModuleFromFlags(cmd, otlp.WithServiceVersion(Version)),
				otlptraces.FXModuleFromFlags(cmd),
				otlpmetrics.FXModuleFromFlags(cmd),
				publish.FXModuleFromFlags(cmd, service.IsDebug(cmd)),
				auth.FXModuleFromFlags(cmd),
				bunconnect.Module(*connectionOptions, service.IsDebug(cmd)),
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
					EnableFeatures: cfg.ExperimentalFeaturesEnabled,
				}),
				bus.NewFxModule(),
				ballast.Module(cfg.BallastSizeInBytes),
				api.Module(api.Config{
					Version: Version,
					Debug:   service.IsDebug(cmd),
					Bulk: api.BulkConfig{
						MaxSize:  cfg.BulkMaxSize,
						Parallel: cfg.BulkParallel,
					},
					Pagination: common.PaginationConfig{
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

						MeterProvider *metric.MeterProvider         `optional:"true"`
						Exporter      *otlpmetrics.InMemoryExporter `optional:"true"`
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
					lc.Append(httpserver.NewHook(h, httpserver.WithAddress(cfg.Bind)))
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
	cmd.Flags().String(NumscriptInterpreterFlagsToPass, "", "Feature flags to pass to the experimental numscript interpreter")
	cmd.Flags().String(WorkerGRPCAddressFlag, "localhost:8081", "GRPC address")

	addWorkerFlags(cmd)
	bunconnect.AddFlags(cmd.Flags())
	otlp.AddFlags(cmd.Flags())
	otlpmetrics.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())
	auth.AddFlags(cmd.Flags())
	publish.AddFlags(ServiceName, cmd.Flags(), func(cd *publish.ConfigDefault) {
		cd.PublisherCircuitBreakerSchema = systemstore.SchemaSystem
	})
	iam.AddFlags(cmd.Flags())

	return cmd
}

func assembleFinalRouter(
	exportPProf bool,
	meterProvider *metric.MeterProvider,
	exporter *otlpmetrics.InMemoryExporter,
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
			r.Handle("/metrics", otlpmetrics.NewInMemoryExporterHandler(
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
