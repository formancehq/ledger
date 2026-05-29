package restore

import (
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

// NewPreviewCommand creates the restore preview command.
func NewPreviewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "preview",
		Short: "Preview staged backup data",
		Long:  "Display a summary of the staged backup data (ledger count, indices, timestamps)",
		RunE:  runPreview,
	}

	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runPreview(cmd *cobra.Command, _ []string) error {
	client, conn, err := getRestoreClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	resp, err := client.PreviewRestore(ctx, &restorepb.PreviewRestoreRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to preview restore", err)
	}

	printRestorePreview(resp)

	return nil
}

func printRestorePreview(resp *restorepb.PreviewRestoreResponse) {
	// Format timestamp
	var timestampStr string

	if resp.GetLastAppliedTimestamp() > 0 {
		t := time.UnixMicro(int64(resp.GetLastAppliedTimestamp()))
		timestampStr = t.Format(time.RFC3339)
	} else {
		timestampStr = "N/A"
	}

	pterm.DefaultSection.Println("Restore Preview")

	data := [][]string{
		{"Last Applied Index", strconv.FormatUint(resp.GetLastAppliedIndex(), 10)},
		{"Last Applied Time", timestampStr},
		{"Last Log Sequence", strconv.FormatUint(resp.GetLastSequence(), 10)},
		{"Ledger Count", strconv.FormatUint(uint64(resp.GetLedgerCount()), 10)},
		{"Ledgers", strings.Join(resp.GetLedgerNames(), ", ")},
	}

	_ = pterm.DefaultTable.
		WithHasHeader(false).
		WithBoxed(true).
		WithData(data).
		Render()
}
