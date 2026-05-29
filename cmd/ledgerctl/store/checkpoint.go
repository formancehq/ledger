package store

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// NewCheckpointCommand creates the store checkpoint command.
func NewCheckpointCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "checkpoint",
		Aliases: []string{"cp"},
		Short:   "Create a Pebble checkpoint",
		Long:    "Create a Pebble checkpoint of the current live database state via gRPC. Useful after compaction to persist the compacted state across restarts.",
		RunE:    runCheckpoint,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runCheckpoint(cmd *cobra.Command, _ []string) error {
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
		spinner, _ = pterm.DefaultSpinner.Start("Creating checkpoint...")
	}

	resp, err := client.CreateCheckpoint(ctx, &clusterpb.CreateCheckpointRequest{})
	if err != nil {
		if spinner != nil {
			_ = spinner.Stop()
		}

		return cmdutil.FormatGRPCError("checkpoint creation failed", err)
	}

	if spinner != nil {
		spinner.Success(fmt.Sprintf("Checkpoint created (id=%d)", resp.GetCheckpointId()))
	}

	if handled, err := cmdutil.EncodeStructured(cmd, struct {
		CheckpointID uint64 `json:"checkpointId"`
	}{
		CheckpointID: resp.GetCheckpointId(),
	}); handled || err != nil {
		return err
	}

	return nil
}
