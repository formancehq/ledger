package main

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newPeriodsGetScheduleCommand creates the periods get-schedule command.
func newPeriodsGetScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-schedule",
		Short: "Show automatic period rotation schedule",
		Long:  `Display the current cron schedule for automatic period rotation, if any.`,
		Args:  cobra.NoArgs,
		RunE:  runPeriodsGetSchedule,
	}

	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runPeriodsGetSchedule(cmd *cobra.Command, _ []string) error {
	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Fetching period schedule...")

	resp, err := client.GetPeriodSchedule(ctx, &servicepb.GetPeriodScheduleRequest{})
	if err != nil {
		spinner.Fail("Failed to get period schedule")
		return formatGRPCError("failed to get period schedule", err)
	}

	if resp.Cron == "" {
		spinner.Success("No period schedule configured (automatic rotation disabled)")
	} else {
		spinner.Success("Period schedule: " + resp.Cron)
	}

	return nil
}
