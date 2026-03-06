//go:build kafka

package events_test

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

var sharedKafkaBrokers []string

func init() {
	var kafkaContainer *kafka.KafkaContainer

	registerTestSetup(
		func(ctx context.Context) error {
			var err error

			kafkaContainer, err = kafka.Run(ctx, "confluentinc/confluent-local:7.6.1")
			if err != nil {
				return fmt.Errorf("failed to start Kafka container: %w", err)
			}

			sharedKafkaBrokers, err = kafkaContainer.Brokers(ctx)
			if err != nil {
				_ = kafkaContainer.Terminate(ctx)

				return fmt.Errorf("failed to get Kafka brokers: %w", err)
			}

			return nil
		},
		func(ctx context.Context) {
			if kafkaContainer != nil {
				_ = kafkaContainer.Terminate(ctx)
			}
		},
	)
}
