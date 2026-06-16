package logs

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewGetCommand creates the logs get command.
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "get <sequence>",
		Short:             "Get a single system log by sequence number",
		Long:              "Retrieve and display a single system log entry by its sequence number",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runGet,
	}

	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runGet(cmd *cobra.Command, args []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	sequence, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid sequence number %q: %w", args[0], err)
	}

	checkpointID, _ := cmd.Flags().GetUint64("checkpoint-id")

	log, err := client.GetLog(ctx, &servicepb.GetLogRequest{
		Sequence:     sequence,
		CheckpointId: checkpointID,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to get log", err)
	}

	if handled, err := cmdutil.EncodeStructured(cmd, log); handled || err != nil {
		return err
	}

	printLog(log, true)

	return nil
}
