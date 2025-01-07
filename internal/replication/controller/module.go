package controller

import (
	"github.com/formancehq/ledger/internal/replication/drivers"
	"go.uber.org/fx"
)

func NewModule() fx.Option {
	return fx.Options(
		fx.Provide(New),
		fx.Provide(NewDefaultRunner),
		fx.Provide(func(registry *drivers.Registry) ConfigValidator {
			return registry
		}),
	)
}
