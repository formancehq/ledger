package kafkabus

import (
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"go.uber.org/fx"
	"log"
	"os"
)

func NewSaramaConfig(clientId string, version sarama.KafkaVersion) *sarama.Config {
	sarama.Logger = log.New(os.Stdout, "sarama: ", log.LstdFlags)
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

func NewKafkaSubscriber(logger watermill.LoggerAdapter, config *sarama.Config, brokers []string) (*kafka.Subscriber, error) {
	subscriberConfig := kafka.SubscriberConfig{
		Brokers:               brokers,
		Unmarshaler:           &kafka.DefaultMarshaler{},
		OverwriteSaramaConfig: config,
	}
	return kafka.NewSubscriber(subscriberConfig, logger)
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
		fx.Provide(func(logger watermill.LoggerAdapter, config *sarama.Config) (*kafka.Subscriber, error) {
			return NewKafkaSubscriber(logger, config, brokers)
		}),
		fx.Decorate(
			func(kafkaPublisher *kafka.Publisher) message.Publisher {
				return kafkaPublisher
			},
		),
		fx.Decorate(
			func(kafkaSubscriber *kafka.Subscriber) message.Subscriber {
				return kafkaSubscriber
			},
		),
	)
}
