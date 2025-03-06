package system

import (
	"github.com/formancehq/ledger/internal/replication/drivers"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
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
		fx.Provide(func(store *systemstore.DefaultStore) Store {
			return store
		}),
		fx.Provide(fx.Annotate(NewControllerStorageDriverAdapter, fx.As(new(Driver)))),
		fx.Provide(func(driversRegistry *drivers.Registry) ConfigValidator {
			return driversRegistry
		}),
		fx.Provide(func(
			driver Driver,
			listener ledgercontroller.Listener,
			validator ConfigValidator,
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

			return NewDefaultController(driver, listener, validator, WithParser(parser, machineParser, interpreterParser), WithDatabaseRetryConfiguration(configuration.DatabaseRetryConfiguration), WithMeterProvider(meterProvider), WithTracerProvider(tracerProvider), WithEnableFeatures(configuration.EnableFeatures))
		}),
	)
}
