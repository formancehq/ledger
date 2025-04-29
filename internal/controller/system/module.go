package system

import (
	"time"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

type DatabaseRetryConfiguration struct {
	MaxRetry int
	Delay    time.Duration
}

type ModuleConfiguration struct {
	NSCacheConfiguration       ledgercontroller.CacheConfiguration
	DatabaseRetryConfiguration DatabaseRetryConfiguration
	EnableFeatures             bool
	NumscriptInterpreter       bool
	// Ignored whenever NumscriptInterpreter is set to false
	NumscriptInterpreterFlags []string
}

func NewFXModule(configuration ModuleConfiguration) fx.Option {
	return fx.Options(
		fx.Provide(func(controller *DefaultController) Controller {
			return controller
		}),
		fx.Provide(func(
			store Store,
			listener ledgercontroller.Listener,
			meterProvider metric.MeterProvider,
			tracerProvider trace.TracerProvider,
		) *DefaultController {
			var (
				machineParser     ledgercontroller.NumscriptParser = ledgercontroller.NewDefaultNumscriptParser()
				interpreterParser ledgercontroller.NumscriptParser = ledgercontroller.NewInterpreterNumscriptParser(configuration.NumscriptInterpreterFlags)
			)

			if configuration.NSCacheConfiguration.MaxCount != 0 {
				machineParser = ledgercontroller.NewCachedNumscriptParser(machineParser, ledgercontroller.CacheConfiguration{
					MaxCount: configuration.NSCacheConfiguration.MaxCount,
				})
				interpreterParser = ledgercontroller.NewCachedNumscriptParser(interpreterParser, ledgercontroller.CacheConfiguration{
					MaxCount: configuration.NSCacheConfiguration.MaxCount,
				})
			}

			parser := machineParser
			if configuration.NumscriptInterpreter {
				parser = interpreterParser
			}

			return NewDefaultController(
				store,
				listener,
				WithParser(parser, machineParser, interpreterParser),
				WithDatabaseRetryConfiguration(configuration.DatabaseRetryConfiguration),
				WithMeterProvider(meterProvider),
				WithTracerProvider(tracerProvider),
				WithEnableFeatures(configuration.EnableFeatures),
			)
		}),
	)
}
