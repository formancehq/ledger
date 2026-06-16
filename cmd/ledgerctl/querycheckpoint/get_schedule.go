package querycheckpoint

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// newGetScheduleCommand creates the query-checkpoint get-schedule command.
func newGetScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "get-schedule",
		Short:             "Show automatic query checkpoint creation schedule",
		Long:              `Display the current cron schedule for automatic query checkpoint creation, if any.`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runGetSchedule,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGetSchedule(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Fetching query checkpoint schedule...")

	resp, err := client.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get query checkpoint schedule", err)
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	if resp.GetCron() == "" {
		pterm.Success.Println("No query checkpoint schedule configured (automatic creation disabled)")
	} else {
		pterm.Success.Println("Query checkpoint schedule: " + resp.GetCron())
	}

	return nil
}
