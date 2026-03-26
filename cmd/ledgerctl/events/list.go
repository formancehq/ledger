package events

import (
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
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

	cmdutil.AddOutputFlags(cmd)
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

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	if len(resp.GetSinks()) == 0 {
		pterm.Info.Println("No event sinks found.")
		pterm.Println(pterm.Gray("Add one with: ledgerctl events add-sink --name <name> --nats-url <url> --nats-topic <topic>"))

		return nil
	}

	// Build status lookup by sink name
	statusBySink := make(map[string]struct {
		cursor uint64
		err    string
	}, len(resp.GetSinkStatuses()))
	for _, s := range resp.GetSinkStatuses() {
		entry := statusBySink[s.GetSinkName()]

		entry.cursor = s.GetCursor()
		if s.GetError() != nil {
			entry.err = s.GetError().GetMessage()
		}

		statusBySink[s.GetSinkName()] = entry
	}

	// Display sinks
	for _, sink := range resp.GetSinks() {
		pterm.DefaultSection.Println("Sink: " + sink.GetName())

		// Config
		format := sink.GetFormat()
		if format == "" {
			format = "json"
		}

		batchSize := sink.GetBatchSize()
		if batchSize == 0 {
			batchSize = 64
		}

		batchDelayMs := sink.GetBatchDelayMs()
		if batchDelayMs == 0 {
			batchDelayMs = 10
		}

		data := [][]string{
			{"Format", format},
			{"Batch Size", strconv.Itoa(int(batchSize))},
			{"Batch Delay", fmt.Sprintf("%dms", batchDelayMs)},
			{"Event Types", formatEventTypes(sink.GetEventTypes())},
		}

		// Sink type
		switch s := sink.GetType().(type) {
		case *commonpb.SinkConfig_Nats:
			data = append(data,
				[]string{"Type", "NATS"},
				[]string{"URL", s.Nats.GetUrl()},
				[]string{"Topic", s.Nats.GetTopic()},
			)
		case *commonpb.SinkConfig_Http:
			secret := "(none)"
			if s.Http.GetSecret() != "" {
				secret = "(set)"
			}
			data = append(data,
				[]string{"Type", "HTTP"},
				[]string{"Endpoint", s.Http.GetEndpoint()},
				[]string{"Secret", secret},
			)
		case *commonpb.SinkConfig_Clickhouse:
			data = append(data,
				[]string{"Type", "ClickHouse"},
				[]string{"DSN", cmdutil.ObfuscateDSN(s.Clickhouse.GetDsn())},
				[]string{"Table", s.Clickhouse.GetTable()},
			)
		default:
			data = append(data, []string{"Type", fmt.Sprintf("unknown (%T)", s)})
		}

		// Status
		if status, ok := statusBySink[sink.GetName()]; ok {
			data = append(data, []string{"Cursor", strconv.FormatUint(status.cursor, 10)})
			if status.err != "" {
				data = append(data, []string{"Error", pterm.Red(status.err)})
			} else {
				data = append(data, []string{"Status", pterm.Green("healthy")})
			}
		}

		err := pterm.DefaultTable.WithData(data).Render()
		if err != nil {
			return fmt.Errorf("rendering table: %w", err)
		}

		pterm.Println()
	}

	return nil
}
