package events

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewListCommand creates the events list command.
func NewListCommand() *cobra.Command {
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
		RunE: runList,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runList(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	resp, err := client.GetEventsSinks(ctx, &servicepb.GetEventsSinksRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to get event sinks", err)
	}

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(resp)
	}

	if len(resp.Sinks) == 0 {
		pterm.Info.Println("No event sinks found.")
		pterm.Println(pterm.Gray("Add one with: ledgerctl events add-sink --name <name> --nats-url <url> --nats-topic <topic>"))
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
			{"Event Types", formatEventTypes(sink.EventTypes)},
		}

		// Sink type
		switch s := sink.GetType().(type) {
		case *commonpb.SinkConfig_Nats:
			data = append(data,
				[]string{"Type", "NATS"},
				[]string{"URL", s.Nats.Url},
				[]string{"Topic", s.Nats.Topic},
			)
		case *commonpb.SinkConfig_Clickhouse:
			data = append(data,
				[]string{"Type", "ClickHouse"},
				[]string{"DSN", cmdutil.ObfuscateDSN(s.Clickhouse.Dsn)},
				[]string{"Table", s.Clickhouse.Table},
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
