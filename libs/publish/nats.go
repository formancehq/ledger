package publish

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	wNats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"go.uber.org/fx"
)

func newNatsConn(config wNats.PublisherConfig) (*nats.Conn, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	conn, err := nats.Connect(config.URL, config.NatsOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to nats-core")
	}

	return conn, nil
}

func newNatsPublisherWithConn(conn *nats.Conn, logger watermill.LoggerAdapter, config wNats.PublisherConfig) (*wNats.Publisher, error) {
	return wNats.NewPublisherWithNatsConn(conn, config.GetPublisherPublishConfig(), logger)
}

func newNatsSubscriberWithConn(conn *nats.Conn, logger watermill.LoggerAdapter, config wNats.SubscriberConfig) (*wNats.Subscriber, error) {
	return wNats.NewSubscriberWithNatsConn(conn, config.GetSubscriberSubscriptionConfig(), logger)
}

func NatsModule(clientID, url, serviceName string) fx.Option {
	jetStreamConfig := wNats.JetStreamConfig{
		AutoProvision: true,
		DurablePrefix: serviceName,
	}
	natsOptions := []nats.Option{
		nats.Name(clientID),
	}
	return fx.Options(
		fx.Provide(newNatsConn),
		fx.Provide(newNatsPublisherWithConn),
		fx.Provide(newNatsSubscriberWithConn),
		fx.Supply(wNats.PublisherConfig{
			NatsOptions:       natsOptions,
			URL:               url,
			Marshaler:         &wNats.NATSMarshaler{},
			JetStream:         jetStreamConfig,
			SubjectCalculator: wNats.DefaultSubjectCalculator,
		}),
		fx.Supply(wNats.SubscriberConfig{
			NatsOptions:       natsOptions,
			Unmarshaler:       &wNats.NATSMarshaler{},
			URL:               url,
			QueueGroupPrefix:  serviceName,
			JetStream:         jetStreamConfig,
			SubjectCalculator: wNats.DefaultSubjectCalculator,
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
