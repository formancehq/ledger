package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newEventsRemoveSinkCommand creates the events remove-sink command.
func newEventsRemoveSinkCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove-sink",
		Aliases: []string{"rm", "delete-sink"},
		Short:   "Remove an event sink configuration",
		Long: `Remove a named event sink configuration.

The sink is removed from the Raft-replicated configuration.
If this was the last sink, event emission is implicitly disabled.

Examples:
  ledgerctl events remove-sink --name primary`,
		Args: cobra.NoArgs,
		RunE: runEventsRemoveSink,
	}

	cmd.Flags().String("name", "", "Name of the sink to remove (required)")
	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runEventsRemoveSink(cmd *cobra.Command, _ []string) error {
	name, _ := cmd.Flags().GetString("name")
	if name == "" {
		return fmt.Errorf("--name is required")
	}

	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Removing event sink %s...", name))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_RemoveEventsSink{
				RemoveEventsSink: &servicepb.RemoveEventsSinkRequest{
					Name: name,
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
		spinner.Fail("Failed to remove event sink")
		return formatGRPCError("failed to remove event sink", err)
	}

	spinner.Success("Removed")

	pterm.Println()
	pterm.Printf("Sink: %s (removed)\n", pterm.Gray(name))

	return nil
}
