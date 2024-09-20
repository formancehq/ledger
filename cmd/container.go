package cmd

import (
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/otlp/otlptraces"
	"github.com/formancehq/go-libs/publish"
	"github.com/formancehq/go-libs/service"
	"github.com/formancehq/ledger/internal/engine"
	driver "github.com/formancehq/ledger/internal/storage/driver"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

const ServiceName = "ledger"

func resolveOptions(cmd *cobra.Command, userOptions ...fx.Option) []fx.Option {
	options := make([]fx.Option, 0)
	options = append(options, fx.NopLogger)

	numscriptCacheMaxCountFlag, _ := cmd.Flags().GetInt(NumscriptCacheMaxCountFlag)
	ledgerBatchSizeFlag, _ := cmd.Flags().GetInt(ledgerBatchSizeFlag)

	options = append(options,
		publish.FXModuleFromFlags(cmd, service.IsDebug(cmd)),
		otlptraces.FXModuleFromFlags(cmd),
		otlpmetrics.FXModuleFromFlags(cmd),
		auth.FXModuleFromFlags(cmd),
		driver.FXModuleFromFlags(cmd),
		engine.Module(engine.Configuration{
			NumscriptCache: engine.NumscriptCacheConfiguration{
				MaxCount: numscriptCacheMaxCountFlag,
			},
			LedgerBatchSize: ledgerBatchSizeFlag,
		}),
	)

	return append(options, userOptions...)
}
