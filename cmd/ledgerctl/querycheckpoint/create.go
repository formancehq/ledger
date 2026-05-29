package querycheckpoint

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func newCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a query checkpoint via Raft consensus",
		Long:  "Create a query checkpoint that captures a physical Pebble snapshot of the current state. The checkpoint is replicated to all nodes and enables point-in-time queries.",
		RunE:  runCreate,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runCreate(cmd *cobra.Command, _ []string) error {
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
		spinner, _ = pterm.DefaultSpinner.Start("Creating query checkpoint...")
	}

	resp, err := client.CreateQueryCheckpoint(ctx, &clusterpb.CreateQueryCheckpointRequest{})
	if err != nil {
		if spinner != nil {
			_ = spinner.Stop()
		}

		return cmdutil.FormatGRPCError("query checkpoint creation failed", err)
	}

	if spinner != nil {
		spinner.Success(fmt.Sprintf("Query checkpoint created (id=%d)", resp.GetCheckpointId()))
	}

	if handled, err := cmdutil.EncodeStructured(cmd, struct {
		CheckpointID uint64 `json:"checkpointId"`
		MaxSequence  uint64 `json:"maxSequence"`
	}{
		CheckpointID: resp.GetCheckpointId(),
		MaxSequence:  resp.GetMaxSequence(),
	}); handled || err != nil {
		return err
	}

	return nil
}
