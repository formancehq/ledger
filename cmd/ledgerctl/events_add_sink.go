package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newEventsAddSinkCommand creates the events add-sink command.
func newEventsAddSinkCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "add-sink",
		Aliases: []string{"add", "upsert"},
		Short:   "Add or update an event sink configuration",
		Long: `Add or update a named event sink configuration.

If a sink with the same name already exists, it is replaced (upsert).
The sink configuration is replicated via Raft consensus.

Currently supported sink types: NATS JetStream, ClickHouse.

Examples:
  # Add a NATS sink with default settings
  ledgerctl events add-sink --name primary --nats-url nats://localhost:4222 --nats-topic ledger.events

  # Add a NATS sink with custom batch settings
  ledgerctl events add-sink --name primary --nats-url nats://localhost:4222 --nats-topic ledger.events \
    --format protobuf --batch-size 128 --batch-delay-ms 50

  # Add a ClickHouse sink
  ledgerctl events add-sink --name analytics --ch-dsn clickhouse://user:pass@localhost:9000/db --ch-table ledger_events`,
		Args: cobra.NoArgs,
		RunE: runEventsAddSink,
	}

	cmd.Flags().String("name", "", "Unique name for this sink (required)")
	cmd.Flags().String("nats-url", "", "NATS server URL")
	cmd.Flags().String("nats-topic", "", "NATS topic/subject for events")
	cmd.Flags().String("ch-dsn", "", "ClickHouse DSN (e.g. clickhouse://user:pass@host:9000/db)")
	cmd.Flags().String("ch-table", "ledger_events", "ClickHouse table name")
	cmd.Flags().String("format", "json", "Event serialization format (json or protobuf)")
	cmd.Flags().Int32("batch-size", 0, "Max events per batch (default: 64)")
	cmd.Flags().Int64("batch-delay-ms", 0, "Max delay before flush in ms (default: 10)")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runEventsAddSink(cmd *cobra.Command, _ []string) error {
	name, _ := cmd.Flags().GetString("name")
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	var (
		natsURL, _   = cmd.Flags().GetString("nats-url")
		natsTopic, _ = cmd.Flags().GetString("nats-topic")
		chDSN, _     = cmd.Flags().GetString("ch-dsn")
		chTable, _   = cmd.Flags().GetString("ch-table")
	)

	hasNATS := natsURL != "" || natsTopic != ""
	hasCH := chDSN != ""

	if hasNATS && hasCH {
		return fmt.Errorf("cannot specify both NATS (--nats-url/--nats-topic) and ClickHouse (--ch-dsn) flags")
	}
	if !hasNATS && !hasCH {
		return fmt.Errorf("must specify either NATS (--nats-url and --nats-topic) or ClickHouse (--ch-dsn) flags")
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
	if hasNATS {
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
	} else {
		config.Type = &commonpb.SinkConfig_Clickhouse{
			Clickhouse: &commonpb.ClickHouseSinkConfig{
				Dsn:   chDSN,
				Table: chTable,
			},
		}
		sinkType = "ClickHouse"
	}

	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
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

	if err := signRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")
		return err
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		spinner.Fail("Failed to add event sink")
		return formatGRPCError("failed to add event sink", err)
	}

	spinner.Success("Added")

	pterm.Println()
	pterm.Printf("Sink:   %s\n", pterm.Cyan(name))
	pterm.Printf("Type:   %s\n", sinkType)
	if hasNATS {
		pterm.Printf("URL:    %s\n", natsURL)
		pterm.Printf("Topic:  %s\n", natsTopic)
	} else {
		pterm.Printf("DSN:    %s\n", chDSN)
		pterm.Printf("Table:  %s\n", chTable)
	}
	pterm.Printf("Format: %s\n", format)

	return nil
}
