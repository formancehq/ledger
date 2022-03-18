package kafkabus

import (
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"go.uber.org/fx"
)

func NewSaramaConfig(clientId string, version sarama.KafkaVersion) *sarama.Config {
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.ClientID = clientId
	return config
}

func NewKafkaPublisher(logger watermill.LoggerAdapter, config *sarama.Config, brokers []string) (*kafka.Publisher, error) {
	publisherConfig := kafka.PublisherConfig{
		Brokers:               brokers,
		Marshaler:             kafka.DefaultMarshaler{},
		OverwriteSaramaConfig: config,
	}
	return kafka.NewPublisher(publisherConfig, logger)
}

func Module(clientId string, brokers ...string) fx.Option {
	return fx.Options(
		fx.Supply(sarama.V1_0_0_0),
		fx.Provide(func(version sarama.KafkaVersion) *sarama.Config {
			return NewSaramaConfig(clientId, version)
		}),
		fx.Provide(func(logger watermill.LoggerAdapter, config *sarama.Config) (*kafka.Publisher, error) {
			return NewKafkaPublisher(logger, config, brokers)
		}),
		fx.Decorate(
			func(kafkaPublisher *kafka.Publisher) message.Publisher {
				return kafkaPublisher
			},
		),
	)
}
