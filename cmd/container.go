package cmd

import (
	"io"

	"github.com/formancehq/ledger/cmd/internal"
	"github.com/formancehq/ledger/internal/engine"
	driver "github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/stack/libs/go-libs/auth"
	"github.com/formancehq/stack/libs/go-libs/otlp/otlpmetrics"
	"github.com/formancehq/stack/libs/go-libs/otlp/otlptraces"
	"github.com/formancehq/stack/libs/go-libs/publish"
	"github.com/formancehq/stack/libs/go-libs/service"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const ServiceName = "ledger"

func resolveOptions(output io.Writer, userOptions ...fx.Option) []fx.Option {
	options := make([]fx.Option, 0)
	options = append(options, fx.NopLogger)

	v := viper.GetViper()
	debug := v.GetBool(service.DebugFlag)

	options = append(options,
		publish.CLIPublisherModule(v, ServiceName),
		otlptraces.CLITracesModule(v),
		otlpmetrics.CLIMetricsModule(v),
		auth.CLIAuthModule(v),
		driver.CLIModule(v, output, debug),
		internal.NewAnalyticsModule(v, Version),
		engine.Module(engine.Configuration{
			NumscriptCache: engine.NumscriptCacheConfiguration{
				MaxCount: v.GetInt(numscriptCacheMaxCountFlag),
			},
		}),
	)

	return append(options, userOptions...)
}
