package bus

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
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

func DefaultPublisherModule() fx.Option {
	return fx.Provide(fx.Annotate(NewInternalPublisher, fx.As(new(message.Publisher)), fx.As(new(message.Subscriber))))
}

func Module() fx.Option {
	return fx.Options(
		LedgerMonitorModule(),
		LoggingModule(),
		RouterModule(),
		DefaultPublisherModule(),
	)
}
