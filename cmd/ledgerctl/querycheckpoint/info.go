package querycheckpoint

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func newInfoCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "info <checkpoint-id>",
		Short:             "Show detailed information about a query checkpoint",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runInfo,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runInfo(cmd *cobra.Command, args []string) error {
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

	info, err := client.GetQueryCheckpointInfo(ctx, &clusterpb.GetQueryCheckpointInfoRequest{
		CheckpointId: checkpointID,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("getting query checkpoint info failed", err)
	}

	if handled, err := cmdutil.EncodeStructured(cmd, info); handled || err != nil {
		return err
	}

	created := info.GetCreatedAt().AsTime().Format(time.RFC3339)

	tableData := pterm.TableData{
		{"FIELD", "VALUE"},
		{"Checkpoint ID", strconv.FormatUint(info.GetCheckpointId(), 10)},
		{"Created", created},
		{"Max Sequence", strconv.FormatUint(info.GetMaxSequence(), 10)},
	}

	if err := pterm.DefaultTable.WithHasHeader().WithData(tableData).Render(); err != nil {
		return fmt.Errorf("rendering table: %w", err)
	}

	return nil
}
