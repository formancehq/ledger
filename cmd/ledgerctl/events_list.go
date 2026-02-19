package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newEventsListCommand creates the events list command.
func newEventsListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "sinks"},
		Short:   "List all event sink configurations and statuses",
		Long: `List all configured event sinks and their current status.

Shows each sink's configuration (name, format, batch settings, sink type)
and its runtime status (cursor position, any active errors).

Examples:
  ledgerctl events list`,
		Args: cobra.NoArgs,
		RunE: runEventsList,
	}

	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runEventsList(cmd *cobra.Command, _ []string) error {
	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	resp, err := client.GetEventsSinks(ctx, &servicepb.GetEventsSinksRequest{})
	if err != nil {
		return formatGRPCError("failed to get event sinks", err)
	}

	if len(resp.Sinks) == 0 {
		pterm.Info.Println("No event sinks configured.")
		pterm.Println(pterm.Gray("Hint: Add a sink using:"))
		pterm.FgCyan.Println("  ledgerctl events add-sink --name <name> --nats-url <url> --nats-topic <topic>")
		return nil
	}

	// Build status lookup by sink name
	statusBySink := make(map[string]struct {
		cursor uint64
		err    string
	}, len(resp.SinkStatuses))
	for _, s := range resp.SinkStatuses {
		entry := statusBySink[s.SinkName]
		entry.cursor = s.Cursor
		if s.Error != nil {
			entry.err = s.Error.Message
		}
		statusBySink[s.SinkName] = entry
	}

	// Display sinks
	for _, sink := range resp.Sinks {
		pterm.DefaultSection.Println(fmt.Sprintf("Sink: %s", sink.Name))

		// Config
		format := sink.Format
		if format == "" {
			format = "json"
		}
		batchSize := sink.BatchSize
		if batchSize == 0 {
			batchSize = 64
		}
		batchDelayMs := sink.BatchDelayMs
		if batchDelayMs == 0 {
			batchDelayMs = 10
		}

		data := [][]string{
			{"Format", format},
			{"Batch Size", fmt.Sprintf("%d", batchSize)},
			{"Batch Delay", fmt.Sprintf("%dms", batchDelayMs)},
		}

		// Sink type
		switch s := sink.GetType().(type) {
		case *commonpb.SinkConfig_Nats:
			data = append(data,
				[]string{"Type", "NATS"},
				[]string{"URL", s.Nats.Url},
				[]string{"Topic", s.Nats.Topic},
			)
		default:
			data = append(data, []string{"Type", fmt.Sprintf("unknown (%T)", s)})
		}

		// Status
		if status, ok := statusBySink[sink.Name]; ok {
			data = append(data, []string{"Cursor", fmt.Sprintf("%d", status.cursor)})
			if status.err != "" {
				data = append(data, []string{"Error", pterm.Red(status.err)})
			} else {
				data = append(data, []string{"Status", pterm.Green("healthy")})
			}
		}

		if err := pterm.DefaultTable.WithData(data).Render(); err != nil {
			return fmt.Errorf("rendering table: %w", err)
		}
		pterm.Println()
	}

	return nil
}
