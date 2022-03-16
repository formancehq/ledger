package bus

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/numary/ledger/pkg/ledger"
	"go.uber.org/fx"
)

func NewInternalPublisher(logger watermill.LoggerAdapter) *gochannel.GoChannel {
	return gochannel.NewGoChannel(
		gochannel.Config{
			BlockPublishUntilSubscriberAck: true,
		},
		logger,
	)
}

func Module() fx.Option {
	return fx.Options(
		fx.Supply(fx.Annotate(watermill.NopLogger{}, fx.As(new(watermill.LoggerAdapter)))),
		fx.Provide(NewLedgerMonitor),
		fx.Provide(NewRouter),
		fx.Provide(fx.Annotate(NewInternalPublisher, fx.As(new(message.Publisher)), fx.As(new(message.Subscriber)))),
		ledger.ProvideResolverOption(func(monitor *ledgerMonitor) ledger.ResolveOptionFn {
			return ledger.WithMonitor(monitor)
		}),
	)
}
