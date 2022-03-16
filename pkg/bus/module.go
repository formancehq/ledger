package bus

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/numary/ledger/pkg/ledger"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Options(
		fx.Provide(NewLedgerMonitor),
		fx.Provide(fx.Annotate(NewInternalPublisher, fx.As(new(message.Publisher)), fx.As(new(message.Subscriber)))),
		ledger.ProvideResolverOption(func(monitor *ledgerMonitor) ledger.ResolveOptionFn {
			return ledger.WithMonitor(monitor)
		}),
	)
}
