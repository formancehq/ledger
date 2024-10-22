package cmd

import (
	apilib "github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/health"
	"github.com/formancehq/go-libs/v2/httpserver"
	"github.com/formancehq/go-libs/v2/otlp"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/sdk/metric"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/aws/iam"
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v2/otlp/otlptraces"
	"github.com/formancehq/go-libs/v2/publish"
	"github.com/formancehq/ledger/internal/api"
	"github.com/formancehq/ledger/internal/bus"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/storage"

	"github.com/formancehq/go-libs/v2/ballast"
	"github.com/formancehq/go-libs/v2/service"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

const (
	BindFlag                         = "bind"
	BallastSizeInBytesFlag           = "ballast-size"
	NumscriptCacheMaxCountFlag       = "numscript-cache-max-count"
	AutoUpgradeFlag                  = "auto-upgrade"
	APIResponseTimeoutDelayFlag      = "api-response-timeout-delay"
	APIResponseTimeoutStatusCodeFlag = "api-response-timeout-status-code"
	ExperimentalFeaturesFlag         = "experimental-features"
)

func NewServeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "serve",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			serveConfiguration := discoverServeConfiguration(cmd)

			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
			if err != nil {
				return err
			}

			experimentalFeatures, err := cmd.Flags().GetBool(ExperimentalFeaturesFlag)
			if err != nil {
				return err
			}

			apiResponseTimeoutDelay, err := cmd.Flags().GetDuration(APIResponseTimeoutDelayFlag)
			if err != nil {
				return err
			}

			apiResponseTimeoutStatusCode, err := cmd.Flags().GetInt(APIResponseTimeoutStatusCodeFlag)
			if err != nil {
				return err
			}

			options := []fx.Option{
				fx.NopLogger,
				otlp.FXModuleFromFlags(cmd),
				otlptraces.FXModuleFromFlags(cmd),
				otlpmetrics.FXModuleFromFlags(cmd),
				publish.FXModuleFromFlags(cmd, service.IsDebug(cmd)),
				auth.FXModuleFromFlags(cmd),
				bunconnect.Module(*connectionOptions, service.IsDebug(cmd)),
				storage.NewFXModule(serveConfiguration.autoUpgrade),
				systemcontroller.NewFXModule(systemcontroller.ModuleConfiguration{
					NSCacheConfiguration: ledgercontroller.CacheConfiguration{
						MaxCount: serveConfiguration.numscriptCacheMaxCount,
					},
					DatabaseRetryConfiguration: systemcontroller.DatabaseRetryConfiguration{
						MaxRetry: 10,
						Delay:    time.Millisecond * 100,
					},
					EnableFeatures: experimentalFeatures,
				}),
				bus.NewFxModule(),
				ballast.Module(serveConfiguration.ballastSize),
				api.Module(api.Config{
					Version: Version,
					Debug:   service.IsDebug(cmd),
					Timeout: common.TimeoutConfiguration{
						Timeout:    apiResponseTimeoutDelay,
						StatusCode: apiResponseTimeoutStatusCode,
					},
				}),
				fx.Decorate(func(
					params struct {
						fx.In

						Handler          chi.Router
						HealthController *health.HealthController

						MeterProvider *metric.MeterProvider         `optional:"true"`
						Exporter      *otlpmetrics.InMemoryExporter `optional:"true"`
					},
				) chi.Router {
					return assembleFinalRouter(
						service.IsDebug(cmd),
						params.MeterProvider,
						params.Exporter,
						params.HealthController,
						params.Handler,
					)
				}),
				fx.Invoke(func(lc fx.Lifecycle, h chi.Router) {
					lc.Append(httpserver.NewHook(h, httpserver.WithAddress(serveConfiguration.bind)))
				}),
			}

			return service.New(cmd.OutOrStdout(), options...).Run(cmd)
		},
	}
	cmd.Flags().Uint(BallastSizeInBytesFlag, 0, "Ballast size in bytes, default to 0")
	cmd.Flags().Uint(NumscriptCacheMaxCountFlag, 1024, "Numscript cache max count")
	cmd.Flags().Bool(AutoUpgradeFlag, false, "Automatically upgrade all schemas")
	cmd.Flags().String(BindFlag, "0.0.0.0:3068", "API bind address")
	cmd.Flags().Bool(ExperimentalFeaturesFlag, false, "Enable features configurability")
	cmd.Flags().Duration(APIResponseTimeoutDelayFlag, 0, "API response timeout delay")
	cmd.Flags().Int(APIResponseTimeoutStatusCodeFlag, http.StatusGatewayTimeout, "API response timeout status code")

	service.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())
	otlpmetrics.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())
	auth.AddFlags(cmd.Flags())
	publish.AddFlags(ServiceName, cmd.Flags(), func(cd *publish.ConfigDefault) {
		cd.PublisherCircuitBreakerSchema = driver.SchemaSystem
	})
	iam.AddFlags(cmd.Flags())

	return cmd
}

type serveConfiguration struct {
	ballastSize            uint
	numscriptCacheMaxCount uint
	autoUpgrade            bool
	bind                   string
}

func discoverServeConfiguration(cmd *cobra.Command) serveConfiguration {
	ret := serveConfiguration{}
	ret.ballastSize, _ = cmd.Flags().GetUint(BallastSizeInBytesFlag)
	ret.numscriptCacheMaxCount, _ = cmd.Flags().GetUint(NumscriptCacheMaxCountFlag)
	ret.autoUpgrade, _ = cmd.Flags().GetBool(AutoUpgradeFlag)
	ret.bind, _ = cmd.Flags().GetString(BindFlag)

	return ret
}

func assembleFinalRouter(
	exportPProf bool,
	meterProvider *metric.MeterProvider,
	exporter *otlpmetrics.InMemoryExporter,
	healthController *health.HealthController,
	handler http.Handler,
) *chi.Mux {
	wrappedRouter := chi.NewRouter()
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
