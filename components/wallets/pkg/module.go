package wallet

import (
	"go.uber.org/fx"
)

func Module(ledgerName, chartPrefix string) fx.Option {
	return fx.Module(
		"wallet",
		fx.Provide(fx.Annotate(NewDefaultLedger, fx.As(new(Ledger)))),
		fx.Provide(func() *Chart {
			return NewChart(chartPrefix)
		}),
		fx.Provide(func(ledger Ledger, chart *Chart) *Manager {
			return NewManager(ledgerName, ledger, chart)
		}),
	)
}
