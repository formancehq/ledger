package store

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// NewPrimaryCompactCommand creates the store primary compact command.
func NewPrimaryCompactCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "compact",
		Aliases: []string{"gc"},
		Short:   "Compact the primary Pebble store",
		Long:    "Trigger a synchronous prefix-by-prefix compaction of the primary Pebble store via gRPC",
		RunE:    runPrimaryCompact,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", 5*cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runPrimaryCompact(cmd *cobra.Command, _ []string) error {
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
		spinner, _ = pterm.DefaultSpinner.Start("Compacting storage...")
	}

	resp, err := client.CompactPrimary(ctx, &clusterpb.CompactPrimaryRequest{})
	if err != nil {
		if spinner != nil {
			_ = spinner.Stop()
		}

		return cmdutil.FormatGRPCError("compaction failed", err)
	}

	if spinner != nil {
		spinner.Success(fmt.Sprintf("Compaction complete (%dms)", resp.GetDurationMs()))
	}

	if handled, err := cmdutil.EncodeStructured(cmd, struct {
		DurationMs int64 `json:"durationMs"`
	}{
		DurationMs: resp.GetDurationMs(),
	}); handled || err != nil {
		return err
	}

	return nil
}
