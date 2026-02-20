package main

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newPeriodsDeleteScheduleCommand creates the periods delete-schedule command.
func newPeriodsDeleteScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete-schedule",
		Short: "Delete automatic period rotation schedule",
		Long:  `Remove the cron schedule for automatic period rotation, disabling automatic rotation.`,
		Args:  cobra.NoArgs,
		RunE:  runPeriodsDeleteSchedule,
	}

	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runPeriodsDeleteSchedule(cmd *cobra.Command, _ []string) error {
	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Deleting period schedule...")

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_DeletePeriodSchedule{
				DeletePeriodSchedule: &servicepb.DeletePeriodScheduleRequest{},
			},
		},
	}

	if err := signRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")
		return err
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		spinner.Fail("Failed to delete period schedule")
		return formatGRPCError("failed to delete period schedule", err)
	}

	spinner.Success("Period schedule deleted")

	return nil
}
