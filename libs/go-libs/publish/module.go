package publish

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
	return fx.Options(
		fx.Provide(NewInternalPublisher),
		fx.Provide(func(ch *gochannel.GoChannel) message.Publisher {
			return ch
		}),
	)
}

func Module() fx.Option {
	return fx.Options(
		DefaultLoggingModule(),
		DefaultPublisherModule(),
	)
}
