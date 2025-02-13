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
	NumscriptInterpreterFlags map[string]struct{}
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
			var parser ledgercontroller.NumscriptParser = ledgercontroller.NewDefaultNumscriptParser()
			if configuration.NumscriptInterpreter {
				parser = ledgercontroller.NewInterpreterNumscriptParser(configuration.NumscriptInterpreterFlags)
			}

			if configuration.NSCacheConfiguration.MaxCount != 0 {
				parser = ledgercontroller.NewCachedNumscriptParser(parser, ledgercontroller.CacheConfiguration{
					MaxCount: configuration.NSCacheConfiguration.MaxCount,
				})
			}

			return NewDefaultController(
				store,
				listener,
				WithParser(parser),
				WithDatabaseRetryConfiguration(configuration.DatabaseRetryConfiguration),
				WithMeterProvider(meterProvider),
				WithTracerProvider(tracerProvider),
				WithEnableFeatures(configuration.EnableFeatures),
			)
		}),
	)
}
