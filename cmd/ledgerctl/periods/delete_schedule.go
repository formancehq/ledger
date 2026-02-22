package periods

import (
	"encoding/json"
	"os"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
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

	cmd.Flags().Bool("json", false, "Output as JSON")
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

	if err := cmdutil.SignRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")
		return err
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		spinner.Fail("Failed to delete period schedule")
		return cmdutil.FormatGRPCError("failed to delete period schedule", err)
	}

	spinner.Success("Period schedule deleted")

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(map[string]any{"deleted": true})
	}

	return nil
}
