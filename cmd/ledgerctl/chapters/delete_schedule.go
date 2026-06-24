package chapters

import (
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewDeleteScheduleCommand creates the chapters delete-schedule command.
func NewDeleteScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "delete-schedule",
		Short:             "Delete automatic chapter rotation schedule",
		Long:              `Remove the cron schedule for automatic chapter rotation, disabling automatic rotation.`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runDeleteSchedule,
	}

	cmdutil.AddOutputFlags(cmd)
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

	spinner, _ := pterm.DefaultSpinner.Start("Deleting chapter schedule...")

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_DeleteChapterSchedule{
				DeleteChapterSchedule: &servicepb.DeleteChapterScheduleRequest{},
			},
		},
	}

	applyReq, err := cmdutil.BuildApplyRequest(cmd, requests...)
	if err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	_, err = client.Apply(ctx, applyReq)
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to delete chapter schedule", err)
	}

	spinner.Success("Chapter schedule deleted")

	if handled, err := cmdutil.EncodeStructured(cmd, map[string]any{"deleted": true}); handled || err != nil {
		return err
	}

	return nil
}
