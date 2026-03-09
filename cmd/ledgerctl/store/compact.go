package store

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
)

// NewCompactCommand creates the store compact command.
func NewCompactCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "compact",
		Aliases: []string{"gc"},
		Short:   "Compact the Pebble store",
		Long:    "Trigger a synchronous prefix-by-prefix compaction of the local Pebble store via gRPC",
		RunE:    runCompact,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", 5*cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runCompact(cmd *cobra.Command, _ []string) error {
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

	resp, err := client.CompactStore(ctx, &clusterpb.CompactStoreRequest{})
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
