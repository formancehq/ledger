package publish

import (
	"context"
	"crypto/tls"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"go.uber.org/fx"
)

type SaramaOption interface {
	Apply(config *sarama.Config)
}
type SaramaOptionFn func(config *sarama.Config)

func (fn SaramaOptionFn) Apply(config *sarama.Config) {
	fn(config)
}

func WithConsumerOffsetsInitial(v int64) SaramaOptionFn {
	return func(config *sarama.Config) {
		config.Consumer.Offsets.Initial = v
	}
}

func WithConsumerReturnErrors() SaramaOptionFn {
	return func(config *sarama.Config) {
		config.Consumer.Return.Errors = true
	}
}

func WithProducerReturnSuccess() SaramaOptionFn {
	return func(config *sarama.Config) {
		config.Producer.Return.Successes = true
	}
}

func WithSASLEnabled() SaramaOptionFn {
	return func(config *sarama.Config) {
		config.Net.SASL.Enable = true
	}
}

func WithSASLMechanism(mechanism sarama.SASLMechanism) SaramaOptionFn {
	return func(config *sarama.Config) {
		config.Net.SASL.Mechanism = mechanism
	}
}

func WithSASLScramClient(fn func() sarama.SCRAMClient) SaramaOptionFn {
	return func(config *sarama.Config) {
		config.Net.SASL.SCRAMClientGeneratorFunc = fn
	}
}

func WithSASLCredentials(user, pwd string) SaramaOptionFn {
	return func(config *sarama.Config) {
		config.Net.SASL.User = user
		config.Net.SASL.Password = pwd
	}
}

func WithTLS() SaramaOptionFn {
	return func(config *sarama.Config) {
		config.Net.TLS = struct {
			Enable bool
			Config *tls.Config
		}{
			Enable: true,
			Config: &tls.Config{},
		}
	}
}

func ProvideSaramaOption(options ...SaramaOption) fx.Option {
	fxOptions := make([]fx.Option, 0)
	for _, opt := range options {
		opt := opt
		fxOptions = append(fxOptions, fx.Provide(fx.Annotate(func() SaramaOption {
			return opt
		}, fx.ResultTags(`group:"saramaOptions"`), fx.As(new(SaramaOption)))))
	}
	return fx.Options(fxOptions...)
}

type clientId string

func newSaramaConfig(clientId clientId, version sarama.KafkaVersion, options ...SaramaOption) *sarama.Config {
	config := sarama.NewConfig()
	config.ClientID = string(clientId)
	config.Version = version

	for _, opt := range options {
		opt.Apply(config)
	}

	return config
}

func newKafkaPublisher(logger watermill.LoggerAdapter, config *sarama.Config, marshaller kafka.Marshaler, brokers ...string) (*kafka.Publisher, error) {
	return kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:               brokers,
		Marshaler:             marshaller,
		OverwriteSaramaConfig: config,
		OTELEnabled:           true,
	}, logger)
}

func newKafkaSubscriber(logger watermill.LoggerAdapter, config *sarama.Config,
	unmarshaler kafka.Unmarshaler, consumerGroup string, brokers ...string) (*kafka.Subscriber, error) {
	return kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:               brokers,
		OverwriteSaramaConfig: config,
		Unmarshaler:           unmarshaler,
		OTELEnabled:           true,
		ConsumerGroup:         consumerGroup,
	}, logger)
}

func kafkaModule(clientId clientId, consumerGroup string, brokers ...string) fx.Option {
	return fx.Options(
		fx.Supply(clientId),
		fx.Supply(sarama.V1_0_0_0),
		fx.Supply(fx.Annotate(kafka.DefaultMarshaler{}, fx.As(new(kafka.Marshaler)))),
		fx.Supply(fx.Annotate(kafka.DefaultMarshaler{}, fx.As(new(kafka.Unmarshaler)))),
		fx.Provide(fx.Annotate(newSaramaConfig, fx.ParamTags(``, ``, `group:"saramaOptions"`))),
		fx.Provide(func(lc fx.Lifecycle, logger watermill.LoggerAdapter, marshaller kafka.Marshaler, config *sarama.Config) (*kafka.Publisher, error) {
			ret, err := newKafkaPublisher(logger, config, marshaller, brokers...)
			if err != nil {
				return nil, err
			}
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return ret.Close()
				},
			})
			return ret, nil
		}),
		fx.Provide(func(lc fx.Lifecycle, logger watermill.LoggerAdapter, unmarshaler kafka.Unmarshaler, config *sarama.Config) (*kafka.Subscriber, error) {
			ret, err := newKafkaSubscriber(logger, config, unmarshaler, consumerGroup, brokers...)
			if err != nil {
				return nil, err
			}
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return ret.Close()
				},
			})
			return ret, nil
		}),
		fx.Provide(func(kafkaPublisher *kafka.Publisher) message.Publisher {
			return kafkaPublisher
		}),
		fx.Provide(func(kafkaSubscriber *kafka.Subscriber) message.Subscriber {
			return kafkaSubscriber
		}),
	)
}
