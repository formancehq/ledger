package querycheckpoint

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func newDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "delete <checkpoint-id>",
		Short:             "Delete a query checkpoint",
		Long:              "Delete a previously created query checkpoint by its ID.",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runDelete,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runDelete(cmd *cobra.Command, args []string) error {
	checkpointID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid checkpoint ID %q: %w", args[0], err)
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	structuredOutput := cmdutil.IsStructuredOutput(cmd)

	var spinner *cmdutil.Spinner
	if !structuredOutput {
		spinner = cmdutil.StartSpinner("Deleting query checkpoint...")
	}

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_DeleteQueryCheckpoint{
				DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{
					CheckpointId: checkpointID,
				},
			},
		},
	}

	applyReq, err := cmdutil.BuildApplyRequest(cmd, requests...)
	if err != nil {
		if spinner != nil {
			spinner.Fail("Failed to sign request")
		}

		return cmdutil.Displayed(err)
	}

	_, err = client.Apply(ctx, applyReq)
	if err != nil {
		if spinner != nil {
			_ = spinner.Stop()
		}

		return cmdutil.FormatGRPCError("query checkpoint deletion failed", err)
	}

	if spinner != nil {
		spinner.Success(fmt.Sprintf("Query checkpoint %d deleted", checkpointID))
	}

	return nil
}
