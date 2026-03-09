package store

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
)

// NewCompactReadIndexCommand creates the store compact-read-index command.
func NewCompactReadIndexCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compact-read-index",
		Short: "Compact the bbolt read index",
		Long:  "Trigger an online compaction of the local bbolt read index via gRPC",
		RunE:  runCompactReadIndex,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", 5*cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runCompactReadIndex(cmd *cobra.Command, _ []string) error {
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

	resp, err := client.CompactReadIndex(ctx, &clusterpb.CompactReadIndexRequest{})
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
		DurationMs      int64 `json:"durationMs"`
		SizeBeforeBytes int64 `json:"sizeBeforeBytes"`
		SizeAfterBytes  int64 `json:"sizeAfterBytes"`
	}{
		DurationMs:      resp.GetDurationMs(),
		SizeBeforeBytes: resp.GetSizeBeforeBytes(),
		SizeAfterBytes:  resp.GetSizeAfterBytes(),
	}); handled || err != nil {
		return err
	}

	return nil
}
