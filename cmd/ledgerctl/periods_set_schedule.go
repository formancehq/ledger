package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newPeriodsSetScheduleCommand creates the periods set-schedule command.
func newPeriodsSetScheduleCommand() *cobra.Command {
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
		RunE: runPeriodsSetSchedule,
	}

	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runPeriodsSetSchedule(cmd *cobra.Command, args []string) error {
	cronExpr := args[0]

	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
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

	if err := signRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")
		return err
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		spinner.Fail("Failed to set period schedule")
		return formatGRPCError("failed to set period schedule", err)
	}

	spinner.Success(fmt.Sprintf("Period schedule set to %q", cronExpr))

	return nil
}
