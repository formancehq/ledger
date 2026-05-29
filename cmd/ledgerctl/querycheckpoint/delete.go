package querycheckpoint

import (
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func newDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <checkpoint-id>",
		Short: "Delete a query checkpoint",
		Long:  "Delete a previously created query checkpoint by its ID.",
		Args:  cobra.ExactArgs(1),
		RunE:  runDelete,
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

	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	structuredOutput := cmdutil.IsStructuredOutput(cmd)

	var spinner *pterm.SpinnerPrinter
	if !structuredOutput {
		spinner, _ = pterm.DefaultSpinner.Start("Deleting query checkpoint...")
	}

	_, err = client.DeleteQueryCheckpoint(ctx, &clusterpb.DeleteQueryCheckpointRequest{
		CheckpointId: checkpointID,
	})
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
