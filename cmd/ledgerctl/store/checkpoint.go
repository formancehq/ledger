package store

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
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

	cmd.Flags().Bool("json", false, "Output as JSON instead of formatted output")
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

	jsonOutput, _ := cmd.Flags().GetBool("json")

	var spinner *pterm.SpinnerPrinter
	if !jsonOutput {
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

	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")

		return encoder.Encode(struct {
			CheckpointID uint64 `json:"checkpointId"`
		}{
			CheckpointID: resp.GetCheckpointId(),
		})
	}

	return nil
}
