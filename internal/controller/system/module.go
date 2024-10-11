package system

import (
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"go.uber.org/fx"
	"time"
)

type DatabaseRetryConfiguration struct {
	MaxRetry int
	Delay time.Duration
}

type ModuleConfiguration struct {
	NSCacheConfiguration ledgercontroller.CacheConfiguration
	DatabaseRetryConfiguration DatabaseRetryConfiguration
}

func NewFXModule(configuration ModuleConfiguration) fx.Option {
	return fx.Options(
		fx.Provide(func(controller *DefaultController) Controller {
			return controller
		}),
		fx.Provide(func(
			store Store,
			listener ledgercontroller.Listener,
		) *DefaultController {
			options := make([]Option, 0)
			if configuration.NSCacheConfiguration.MaxCount != 0 {
				options = append(options, WithCompiler(ledgercontroller.NewCachedCompiler(
					ledgercontroller.NewDefaultCompiler(),
					configuration.NSCacheConfiguration,
				)))
			}
			options = append(options, WithDatabaseRetryConfiguration(configuration.DatabaseRetryConfiguration))

			return NewDefaultController(store, listener, options...)
		}),
	)
}
