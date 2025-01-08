package runner

import (
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/replication/leadership"
	"github.com/formancehq/ledger/internal/utils"
	"go.uber.org/fx"
)

// NewFXModule create a new fx module
func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(drivers.NewRegistry),
		fx.Provide(NewStarter),
		fx.Provide(func(leadership *leadership.Leadership) Leadership {
			return leadership
		}),
		fx.Provide(func(registry *drivers.Registry) drivers.Factory {
			return registry
		}),
		// decorate the original Factory (implemented by *Registry)
		// to abstract the fact we want to batch logs
		fx.Decorate(fx.Annotate(
			drivers.NewWithBatchingConnectorFactory,
			fx.As(new(drivers.Factory)),
		)),
		fx.Provide(NewRunner),
		fx.Invoke(utils.StartRunner[*Starter]()),
	)
}
