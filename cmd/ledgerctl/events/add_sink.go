package events

import (
	"fmt"
	"strings"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewAddSinkCommand creates the events add-sink command.
func NewAddSinkCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "add-sink",
		Aliases: []string{"add", "upsert"},
		Short:   "Add or update an event sink configuration",
		Long: `Add or update a named event sink configuration.

If a sink with the same name already exists, it is replaced (upsert).
The sink configuration is replicated via Raft consensus.

Currently supported sink types: NATS JetStream, ClickHouse, Kafka, HTTP.

Examples:
  # Add a NATS sink with default settings
  ledgerctl events add-sink --name primary --nats-url nats://localhost:4222 --nats-topic ledger.events

  # Add a NATS sink with custom batch settings
  ledgerctl events add-sink --name primary --nats-url nats://localhost:4222 --nats-topic ledger.events \
    --format protobuf --batch-size 128 --batch-delay-ms 50

  # Add a ClickHouse sink
  ledgerctl events add-sink --name analytics --ch-dsn clickhouse://user:pass@localhost:9000/db --ch-table ledger_events

  # Add a Kafka sink
  ledgerctl events add-sink --name streaming --kafka-brokers localhost:9092 --kafka-topic ledger-events

  # Add a Kafka sink with SASL authentication
  ledgerctl events add-sink --name streaming --kafka-brokers broker1:9092,broker2:9092 --kafka-topic ledger-events \
    --kafka-tls --kafka-sasl-mechanism SCRAM-SHA-256 --kafka-sasl-username user --kafka-sasl-password pass

  # Add an HTTP webhook sink
  ledgerctl events add-sink --name webhook --http-endpoint https://example.com/webhooks/ledger

  # Add an HTTP webhook sink with HMAC signature
  ledgerctl events add-sink --name webhook --http-endpoint https://example.com/webhooks/ledger --http-secret my-secret`,
		Args: cobra.NoArgs,
		RunE: runAddSink,
	}

	cmd.Flags().String("name", "", "Unique name for this sink (required)")
	cmd.Flags().String("nats-url", "", "NATS server URL")
	cmd.Flags().String("nats-topic", "", "NATS topic/subject for events")
	cmd.Flags().String("ch-dsn", "", "ClickHouse DSN (e.g. clickhouse://user:pass@host:9000/db)")
	cmd.Flags().String("ch-table", "ledger_events", "ClickHouse table name")
	cmd.Flags().String("kafka-brokers", "", "Kafka broker addresses (comma-separated, e.g. localhost:9092)")
	cmd.Flags().String("kafka-topic", "", "Kafka topic name")
	cmd.Flags().Bool("kafka-tls", false, "Enable TLS for Kafka connection")
	cmd.Flags().String("kafka-sasl-mechanism", "", "Kafka SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)")
	cmd.Flags().String("kafka-sasl-username", "", "Kafka SASL username")
	cmd.Flags().String("kafka-sasl-password", "", "Kafka SASL password")
	cmd.Flags().String("http-endpoint", "", "HTTP webhook endpoint URL")
	cmd.Flags().String("http-secret", "", "HMAC-SHA256 secret for X-Webhook-Signature header")
	cmd.Flags().String("format", "json", "Event serialization format (json or protobuf)")
	cmd.Flags().Int32("batch-size", 0, "Max events per batch (default: 64)")
	cmd.Flags().Int64("batch-delay-ms", 0, "Max delay before flush in ms (default: 10)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runAddSink(cmd *cobra.Command, _ []string) error {
	name, _ := cmd.Flags().GetString("name")
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	var (
		natsURL, _         = cmd.Flags().GetString("nats-url")
		natsTopic, _       = cmd.Flags().GetString("nats-topic")
		chDSN, _           = cmd.Flags().GetString("ch-dsn")
		chTable, _         = cmd.Flags().GetString("ch-table")
		kafkaBrokersStr, _ = cmd.Flags().GetString("kafka-brokers")
		kafkaTopic, _      = cmd.Flags().GetString("kafka-topic")
		kafkaTLS, _        = cmd.Flags().GetBool("kafka-tls")
		kafkaSASL, _       = cmd.Flags().GetString("kafka-sasl-mechanism")
		kafkaUser, _       = cmd.Flags().GetString("kafka-sasl-username")
		kafkaPass, _       = cmd.Flags().GetString("kafka-sasl-password")
		httpEndpoint, _    = cmd.Flags().GetString("http-endpoint")
		httpSecret, _      = cmd.Flags().GetString("http-secret")
	)

	hasNATS := natsURL != "" || natsTopic != ""
	hasCH := chDSN != ""
	hasKafka := kafkaBrokersStr != "" || kafkaTopic != ""
	hasHTTP := httpEndpoint != ""

	sinkCount := 0
	if hasNATS {
		sinkCount++
	}
	if hasCH {
		sinkCount++
	}
	if hasKafka {
		sinkCount++
	}
	if hasHTTP {
		sinkCount++
	}

	if sinkCount > 1 {
		return fmt.Errorf("cannot specify multiple sink types; choose one of: NATS (--nats-url), ClickHouse (--ch-dsn), Kafka (--kafka-brokers), or HTTP (--http-endpoint)")
	}
	if sinkCount == 0 {
		return fmt.Errorf("must specify a sink type: NATS (--nats-url and --nats-topic), ClickHouse (--ch-dsn), Kafka (--kafka-brokers and --kafka-topic), or HTTP (--http-endpoint)")
	}

	var (
		format, _       = cmd.Flags().GetString("format")
		batchSize, _    = cmd.Flags().GetInt32("batch-size")
		batchDelayMs, _ = cmd.Flags().GetInt64("batch-delay-ms")
	)

	config := &commonpb.SinkConfig{
		Name:         name,
		Format:       format,
		BatchSize:    batchSize,
		BatchDelayMs: batchDelayMs,
	}

	var sinkType string
	switch {
	case hasNATS:
		if natsURL == "" || natsTopic == "" {
			return fmt.Errorf("--nats-url and --nats-topic are both required for NATS sinks")
		}
		config.Type = &commonpb.SinkConfig_Nats{
			Nats: &commonpb.NatsSinkConfig{
				Url:   natsURL,
				Topic: natsTopic,
			},
		}
		sinkType = "NATS"
	case hasCH:
		config.Type = &commonpb.SinkConfig_Clickhouse{
			Clickhouse: &commonpb.ClickHouseSinkConfig{
				Dsn:   chDSN,
				Table: chTable,
			},
		}
		sinkType = "ClickHouse"
	case hasKafka:
		if kafkaBrokersStr == "" || kafkaTopic == "" {
			return fmt.Errorf("--kafka-brokers and --kafka-topic are both required for Kafka sinks")
		}
		brokers := strings.Split(kafkaBrokersStr, ",")
		config.Type = &commonpb.SinkConfig_Kafka{
			Kafka: &commonpb.KafkaSinkConfig{
				Brokers:       brokers,
				Topic:         kafkaTopic,
				Tls:           kafkaTLS,
				SaslMechanism: kafkaSASL,
				SaslUsername:   kafkaUser,
				SaslPassword:  kafkaPass,
			},
		}
		sinkType = "Kafka"
	case hasHTTP:
		config.Type = &commonpb.SinkConfig_Http{
			Http: &commonpb.HttpSinkConfig{
				Endpoint: httpEndpoint,
				Secret:   httpSecret,
			},
		}
		sinkType = "HTTP"
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Adding event sink %s...", name))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_AddEventsSink{
				AddEventsSink: &servicepb.AddEventsSinkRequest{
					Config: config,
				},
			},
		},
	}

	if err := cmdutil.SignRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")
		return err
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		spinner.Fail("Failed to add event sink")
		return cmdutil.FormatGRPCError("failed to add event sink", err)
	}

	spinner.Success("Added")

	pterm.Println()
	pterm.Printf("Sink:   %s\n", pterm.Cyan(name))
	pterm.Printf("Type:   %s\n", sinkType)
	switch {
	case hasNATS:
		pterm.Printf("URL:    %s\n", natsURL)
		pterm.Printf("Topic:  %s\n", natsTopic)
	case hasCH:
		pterm.Printf("DSN:    %s\n", chDSN)
		pterm.Printf("Table:  %s\n", chTable)
	case hasKafka:
		pterm.Printf("Brokers: %s\n", kafkaBrokersStr)
		pterm.Printf("Topic:   %s\n", kafkaTopic)
	case hasHTTP:
		pterm.Printf("Endpoint: %s\n", httpEndpoint)
		if httpSecret != "" {
			pterm.Printf("Secret:   (set)\n")
		}
	}
	pterm.Printf("Format: %s\n", format)

	return nil
}
