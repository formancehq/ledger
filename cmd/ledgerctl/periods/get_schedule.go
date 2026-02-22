package periods

import (
	"encoding/json"
	"os"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewGetScheduleCommand creates the periods get-schedule command.
func NewGetScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-schedule",
		Short: "Show automatic period rotation schedule",
		Long:  `Display the current cron schedule for automatic period rotation, if any.`,
		Args:  cobra.NoArgs,
		RunE:  runGetSchedule,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
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
		spinner.Fail("Failed to get period schedule")
		return cmdutil.FormatGRPCError("failed to get period schedule", err)
	}

	_ = spinner.Stop()

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(resp)
	}

	if resp.Cron == "" {
		pterm.Success.Println("No period schedule configured (automatic rotation disabled)")
	} else {
		pterm.Success.Println("Period schedule: " + resp.Cron)
	}

	return nil
}
