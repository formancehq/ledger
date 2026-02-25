package events_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	chmodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

var (
	sharedKafkaBrokers  []string
	sharedClickHouseDSN string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	kafkaContainer, err := kafka.Run(ctx, "confluentinc/confluent-local:7.6.1")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start Kafka container: %v\n", err)
		os.Exit(1)
	}

	sharedKafkaBrokers, err = kafkaContainer.Brokers(ctx)
	if err != nil {
		_ = kafkaContainer.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get Kafka brokers: %v\n", err)
		os.Exit(1)
	}

	chContainer, err := chmodule.Run(ctx, "clickhouse/clickhouse-server:24-alpine")
	if err != nil {
		_ = kafkaContainer.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to start ClickHouse container: %v\n", err)
		os.Exit(1)
	}

	sharedClickHouseDSN, err = chContainer.ConnectionString(ctx)
	if err != nil {
		_ = chContainer.Terminate(ctx)
		_ = kafkaContainer.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get ClickHouse DSN: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	_ = chContainer.Terminate(ctx)
	_ = kafkaContainer.Terminate(ctx)

	os.Exit(code)
}
