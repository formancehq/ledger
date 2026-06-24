package events

import (
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewRemoveSinkCommand creates the events remove-sink command.
func NewRemoveSinkCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove-sink",
		Aliases: []string{"rm", "delete-sink"},
		Short:   "Remove an event sink configuration",
		Long: `Remove a named event sink configuration.

The sink is removed from the Raft-replicated configuration.
If this was the last sink, event emission is implicitly disabled.

Examples:
  ledgerctl events remove-sink --name primary`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runRemoveSink,
	}

	cmd.Flags().String("name", "", "Name of the sink to remove (required)")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runRemoveSink(cmd *cobra.Command, _ []string) error {
	name, _ := cmd.Flags().GetString("name")
	if name == "" {
		return errors.New("--name is required")
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
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

	applyReq, err := cmdutil.BuildApplyRequest(cmd, requests...)
	if err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	_, err = client.Apply(ctx, applyReq)
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to remove event sink", err)
	}

	spinner.Success("Removed")

	if handled, err := cmdutil.EncodeStructured(cmd, map[string]any{"name": name, "removed": true}); handled || err != nil {
		return err
	}

	pterm.Println()
	pterm.Printf("Sink: %s (removed)\n", pterm.Gray(name))

	return nil
}
