package periods

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewSetScheduleCommand creates the periods set-schedule command.
func NewSetScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-schedule <cron-expression>",
		Short: "Set automatic period rotation schedule",
		Long: `Set a cron schedule for automatic period rotation.

The cron expression uses the standard 5-field format (minute hour day-of-month month day-of-week)
or the extended 6-field format with an optional leading seconds field.

Examples:
  # Rotate every day at midnight
  ledgerctl periods set-schedule "0 0 * * *"

  # Rotate on the 1st of every month at midnight
  ledgerctl periods set-schedule "0 0 1 * *"

  # Rotate every 30 seconds (6-field format with seconds)
  ledgerctl periods set-schedule "*/30 * * * * *"`,
		Args: cobra.ExactArgs(1),
		RunE: runSetSchedule,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runSetSchedule(cmd *cobra.Command, args []string) error {
	cronExpr := args[0]

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Setting period schedule to %q...", cronExpr))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_SetPeriodSchedule{
				SetPeriodSchedule: &servicepb.SetPeriodScheduleRequest{
					Cron: cronExpr,
				},
			},
		},
	}

	if err := cmdutil.SignRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to set period schedule", err)
	}

	spinner.Success(fmt.Sprintf("Period schedule set to %q", cronExpr))

	if handled, err := cmdutil.EncodeStructured(cmd, map[string]any{"cron": cronExpr}); handled || err != nil {
		return err
	}

	return nil
}
