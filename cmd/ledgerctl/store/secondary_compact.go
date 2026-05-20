package store

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
)

// NewSecondaryCompactCommand creates the store secondary compact command.
func NewSecondaryCompactCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compact",
		Short: "Compact the secondary Pebble store",
		Long:  "Trigger an online compaction of the secondary (read index) Pebble store via gRPC",
		RunE:  runSecondaryCompact,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", 5*cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runSecondaryCompact(cmd *cobra.Command, _ []string) error {
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
		spinner, _ = pterm.DefaultSpinner.Start("Compacting read index...")
	}

	resp, err := client.CompactSecondary(ctx, &clusterpb.CompactSecondaryRequest{})
	if err != nil {
		if spinner != nil {
			_ = spinner.Stop()
		}

		return cmdutil.FormatGRPCError("read index compaction failed", err)
	}

	if spinner != nil {
		spinner.Success(fmt.Sprintf("Read index compaction complete (%dms, %d → %d bytes)", resp.GetDurationMs(), resp.GetSizeBeforeBytes(), resp.GetSizeAfterBytes()))
	}

	if handled, err := cmdutil.EncodeStructured(cmd, struct {
		DurationMs      int64  `json:"durationMs"`
		SizeBeforeBytes uint64 `json:"sizeBeforeBytes"`
		SizeAfterBytes  uint64 `json:"sizeAfterBytes"`
	}{
		DurationMs:      resp.GetDurationMs(),
		SizeBeforeBytes: resp.GetSizeBeforeBytes(),
		SizeAfterBytes:  resp.GetSizeAfterBytes(),
	}); handled || err != nil {
		return err
	}

	return nil
}
