package publish

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	wNats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"go.uber.org/fx"
)

func newNatsPublisher(logger watermill.LoggerAdapter, config wNats.PublisherConfig) (*wNats.Publisher, error) {
	return wNats.NewPublisher(config, logger)
}

func newNatsSubscriber(logger watermill.LoggerAdapter, config wNats.SubscriberConfig) (*wNats.Subscriber, error) {
	return wNats.NewSubscriber(config, logger)
}

func natsModule(clientID, url, serviceName string) fx.Option {
	jetStreamConfig := wNats.JetStreamConfig{
		AutoProvision: true,
		DurablePrefix: serviceName,
	}
	natsOptions := []nats.Option{
		nats.Name(clientID),
	}
	return fx.Options(
		fx.Provide(newNatsPublisher),
		fx.Provide(newNatsSubscriber),
		fx.Supply(wNats.PublisherConfig{
			NatsOptions: natsOptions,
			URL:         url,
			Marshaler:   &wNats.NATSMarshaler{},
			JetStream:   jetStreamConfig,
		}),
		fx.Supply(wNats.SubscriberConfig{
			NatsOptions:      natsOptions,
			Unmarshaler:      &wNats.NATSMarshaler{},
			URL:              url,
			QueueGroupPrefix: serviceName,
			JetStream:        jetStreamConfig,
		}),
		fx.Provide(func(publisher *wNats.Publisher) message.Publisher {
			return publisher
		}),
		fx.Provide(func(subscriber *wNats.Subscriber, lc fx.Lifecycle) message.Subscriber {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return subscriber.Close()
				},
			})
			return subscriber
		}),
	)
}
