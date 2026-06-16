package querycheckpoint

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// newSetScheduleCommand creates the query-checkpoint set-schedule command.
func newSetScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-schedule <cron-expression>",
		Short: "Set automatic query checkpoint creation schedule",
		Long: `Set a cron schedule for automatic query checkpoint creation.

The cron expression uses the standard 5-field format (minute hour day-of-month month day-of-week)
or the extended 6-field format with an optional leading seconds field.

Examples:
  # Create a checkpoint every day at midnight
  ledgerctl query-checkpoint set-schedule "0 0 * * *"

  # Create a checkpoint every hour
  ledgerctl query-checkpoint set-schedule "0 * * * *"

  # Create a checkpoint every 30 seconds (6-field format with seconds)
  ledgerctl query-checkpoint set-schedule "*/30 * * * * *"`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runSetSchedule,
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

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Setting query checkpoint schedule to %q...", cronExpr))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_SetQueryCheckpointSchedule{
				SetQueryCheckpointSchedule: &servicepb.SetQueryCheckpointScheduleRequest{
					Cron: cronExpr,
				},
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

		return cmdutil.FormatGRPCError("failed to set query checkpoint schedule", err)
	}

	spinner.Success(fmt.Sprintf("Query checkpoint schedule set to %q", cronExpr))

	if handled, err := cmdutil.EncodeStructured(cmd, map[string]any{"cron": cronExpr}); handled || err != nil {
		return err
	}

	return nil
}
