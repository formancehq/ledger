package cmd

import (
	"context"
	"net/http"

	"github.com/formancehq/ledger/internal/bus"
	otelpyroscope "github.com/grafana/otel-profiling-go"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/aws/iam"
	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/otlp/otlptraces"
	"github.com/formancehq/go-libs/publish"
	"github.com/formancehq/ledger/internal/api"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/storage"
	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/ballast"
	"github.com/formancehq/go-libs/service"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/spf13/cobra"
	"go.uber.org/fx"

	_ "github.com/grafana/pyroscope-go/godeltaprof/http/pprof"
	//nolint:gosec
	_ "net/http/pprof"
)

const (
	BindFlag                   = "bind"
	BallastSizeInBytesFlag     = "ballast-size"
	NumscriptCacheMaxCountFlag = "numscript-cache-max-count"
	AutoUpgradeFlag            = "auto-upgrade"
	EnablePProfFlag            = "enable-pprof"
)

func NewServeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "serve",
		RunE: func(cmd *cobra.Command, args []string) error {
			serveConfiguration := discoverServeConfiguration(cmd)

			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
			if err != nil {
				return err
			}

			enablePProf, _ := cmd.Flags().GetBool(EnablePProfFlag)
			options := []fx.Option{
				fx.NopLogger,
				otlptraces.FXModuleFromFlags(cmd),
			}
			// todo: move in dedicated shared package
			if enablePProf {
				logging.FromContext(cmd.Context()).Info("Enabling pprof...")
				options = append(options,
					fx.Decorate(func(provider trace.TracerProvider) trace.TracerProvider {
						return otelpyroscope.NewTracerProvider(provider)
					}),
					fx.Invoke(func(lc fx.Lifecycle) {
						lc.Append(fx.Hook{
							OnStart: func(ctx context.Context) error {
								go func() {
									if err := http.ListenAndServe(":3000", nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
										panic(err)
									}
								}()
								return nil
							},
						})
					}),
				)
			}

			options = append(options,
				publish.FXModuleFromFlags(cmd, service.IsDebug(cmd)),
				otlpmetrics.FXModuleFromFlags(cmd),
				auth.FXModuleFromFlags(cmd),
				bunconnect.Module(*connectionOptions, service.IsDebug(cmd)),
				storage.NewFXModule(serveConfiguration.autoUpgrade),
				systemcontroller.NewFXModule(systemcontroller.ModuleConfiguration{
					NSCacheConfiguration: ledgercontroller.CacheConfiguration{
						MaxCount: serveConfiguration.numscriptCacheMaxCount,
					},
				}),
				bus.NewFxModule(),
				ballast.Module(serveConfiguration.ballastSize),
				api.Module(api.Config{
					Version: Version,
					Debug:   service.IsDebug(cmd),
					Bind:    serveConfiguration.bind,
				}),
			)

			return service.New(cmd.OutOrStdout(), options...).Run(cmd)
		},
	}
	cmd.Flags().Uint(BallastSizeInBytesFlag, 0, "Ballast size in bytes, default to 0")
	cmd.Flags().Uint(NumscriptCacheMaxCountFlag, 1024, "Numscript cache max count")
	cmd.Flags().Bool(AutoUpgradeFlag, false, "Automatically upgrade all schemas")
	cmd.Flags().String(BindFlag, "0.0.0.0:3068", "API bind address")
	cmd.Flags().Bool(EnablePProfFlag, false, "Enable pprof")

	service.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())
	otlpmetrics.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())
	auth.AddFlags(cmd.Flags())
	publish.AddFlags(ServiceName, cmd.Flags(), func(cd *publish.ConfigDefault) {
		cd.PublisherCircuitBreakerSchema = systemstore.Schema
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
