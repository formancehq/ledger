package publish

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"go.uber.org/fx"
)

func newGoChannel(logger watermill.LoggerAdapter) *gochannel.GoChannel {
	return gochannel.NewGoChannel(
		gochannel.Config{
			BlockPublishUntilSubscriberAck: true,
		},
		logger,
	)
}

func GoChannelModule() fx.Option {
	return fx.Options(
		fx.Provide(newGoChannel),
		fx.Provide(func(ch *gochannel.GoChannel) message.Subscriber {
			return ch
		}),
		fx.Provide(func(ch *gochannel.GoChannel) message.Publisher {
			return ch
		}),
	)
}

func Module(topics map[string]string) fx.Option {
	options := fx.Options(
		defaultLoggingModule(),
		fx.Supply(message.RouterConfig{}),
		fx.Provide(message.NewRouter),
		fx.Invoke(func(router *message.Router, lc fx.Lifecycle) error {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						if err := router.Run(context.Background()); err != nil {
							panic(err)
						}
					}()
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-router.Running():
					}
					return nil
				},
				OnStop: func(ctx context.Context) error {
					return router.Close()
				},
			})
			return nil
		}),
		fx.Decorate(func(publisher message.Publisher) message.Publisher {
			return NewTopicMapperPublisherDecorator(publisher, topics)
		}),
	)
	return options
}

type topicMapperPublisherDecorator struct {
	message.Publisher
	topics map[string]string
}

func (p topicMapperPublisherDecorator) Publish(topic string, messages ...*message.Message) error {
	mappedTopic, ok := p.topics[topic]
	if ok {
		return p.Publisher.Publish(mappedTopic, messages...)
	}
	mappedTopic, ok = p.topics["*"]
	if ok {
		return p.Publisher.Publish(mappedTopic, messages...)
	}

	return p.Publisher.Publish(topic, messages...)
}

var _ message.Publisher = &topicMapperPublisherDecorator{}

func NewTopicMapperPublisherDecorator(publisher message.Publisher, topics map[string]string) *topicMapperPublisherDecorator {
	return &topicMapperPublisherDecorator{
		Publisher: publisher,
		topics:    topics,
	}
}
