package periods

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewGetScheduleCommand creates the periods get-schedule command.
func NewGetScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "get-schedule",
		Short:             "Show automatic period rotation schedule",
		Long:              `Display the current cron schedule for automatic period rotation, if any.`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runGetSchedule,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGetSchedule(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Fetching period schedule...")

	resp, err := client.GetPeriodSchedule(ctx, &servicepb.GetPeriodScheduleRequest{})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get period schedule", err)
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	if resp.GetCron() == "" {
		pterm.Success.Println("No period schedule configured (automatic rotation disabled)")
	} else {
		pterm.Success.Println("Period schedule: " + resp.GetCron())
	}

	return nil
}
