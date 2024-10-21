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
			options := make([]Option, 0)

			if configuration.NumscriptInterpreter {
				options = append(options, WithUpdateParser(func(ledgercontroller.NumscriptParser) ledgercontroller.NumscriptParser {
					return ledgercontroller.NewInterpreterNumscriptParser()
				}))
			}

			// TEMP enable again
			if false && configuration.NSCacheConfiguration.MaxCount != 0 {
				options = append(options, WithUpdateParser(func(oldParser ledgercontroller.NumscriptParser) ledgercontroller.NumscriptParser {
					if oldParser == nil {
						oldParser = ledgercontroller.NewDefaultNumscriptParser()
					}
					return ledgercontroller.NewCachedNumscriptParser(
						oldParser,
						configuration.NSCacheConfiguration,
					)

				}))
			}

			return NewDefaultController(
				store,
				listener,
				append(options,
					WithDatabaseRetryConfiguration(configuration.DatabaseRetryConfiguration),
					WithMeter(meterProvider.Meter("core")),
					WithTracer(tracerProvider.Tracer("core")),
					WithEnableFeatures(configuration.EnableFeatures),
				)...,
			)
		}),
	)
}
