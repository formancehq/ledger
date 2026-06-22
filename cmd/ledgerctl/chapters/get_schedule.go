package chapters

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewGetScheduleCommand creates the chapters get-schedule command.
func NewGetScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "get-schedule",
		Short:             "Show automatic chapter rotation schedule",
		Long:              `Display the current cron schedule for automatic chapter rotation, if any.`,
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

	spinner, _ := pterm.DefaultSpinner.Start("Fetching chapter schedule...")

	resp, err := client.GetChapterSchedule(ctx, &servicepb.GetChapterScheduleRequest{})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get chapter schedule", err)
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, resp); handled || err != nil {
		return err
	}

	if resp.GetCron() == "" {
		pterm.Success.Println("No chapter schedule configured (automatic rotation disabled)")
	} else {
		pterm.Success.Println("Chapter schedule: " + resp.GetCron())
	}

	return nil
}
