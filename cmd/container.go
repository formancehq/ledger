package cmd

import (
	"github.com/formancehq/ledger/cmd/internal"
	"github.com/formancehq/ledger/pkg/api"
	"github.com/formancehq/ledger/pkg/bus"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"github.com/formancehq/stack/libs/go-libs/otlp/otlpmetrics"
	"github.com/formancehq/stack/libs/go-libs/otlp/otlptraces"
	"github.com/formancehq/stack/libs/go-libs/publish"
	"github.com/formancehq/stack/libs/go-libs/service"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const ServiceName = "ledger"

func resolveOptions(v *viper.Viper, userOptions ...fx.Option) []fx.Option {

	options := make([]fx.Option, 0)
	options = append(options, fx.NopLogger)

	debug := v.GetBool(service.DebugFlag)
	if debug {
		sqlstorage.InstrumentalizeSQLDriver()
	}

	options = append(options,
		publish.CLIPublisherModule(v, ServiceName),
		bus.LedgerMonitorModule(),
		otlptraces.CLITracesModule(v),
		otlpmetrics.CLIMetricsModule(v),
		api.Module(api.Config{
			Version: Version,
		}),
		sqlstorage.DriverModule(sqlstorage.ModuleConfig{
			PostgresConfig: func() *sqlstorage.PostgresConfig {
				return &sqlstorage.PostgresConfig{
					ConnString: v.GetString(storagePostgresConnectionStringFlag),
				}
			}(),
		}),
		internal.NewAnalyticsModule(v, Version),
		ledger.Module(v.GetString(commitPolicyFlag) == "allow-past-timestamps"),
	)

	return append(options, userOptions...)
}
