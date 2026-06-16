package periods

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewDeleteScheduleCommand creates the periods delete-schedule command.
func NewDeleteScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete-schedule",
		Short: "Delete automatic period rotation schedule",
		Long:  `Remove the cron schedule for automatic period rotation, disabling automatic rotation.`,
		Args:  cobra.NoArgs,
		RunE:  runDeleteSchedule,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runDeleteSchedule(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Deleting period schedule...")

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_DeletePeriodSchedule{
				DeletePeriodSchedule: &servicepb.DeletePeriodScheduleRequest{},
			},
		},
	}

	envelopes, err := cmdutil.BuildEnvelopes(cmd, requests)
	if err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{Envelopes: envelopes})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to delete period schedule", err)
	}

	spinner.Success("Period schedule deleted")

	if handled, err := cmdutil.EncodeStructured(cmd, map[string]any{"deleted": true}); handled || err != nil {
		return err
	}

	return nil
}
