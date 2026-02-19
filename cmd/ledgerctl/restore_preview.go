package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/restorepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// newRestorePreviewCommand creates the restore preview command.
func newRestorePreviewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "preview",
		Short: "Preview staged backup data",
		Long:  "Display a summary of the staged backup data (ledger count, indices, timestamps)",
		RunE:  runRestorePreview,
	}

	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")

	return cmd
}

func runRestorePreview(cmd *cobra.Command, _ []string) error {
	client, conn, err := getRestoreClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := getContext(cmd)
	defer cancel()

	resp, err := client.PreviewRestore(ctx, &restorepb.PreviewRestoreRequest{})
	if err != nil {
		return formatGRPCError("failed to preview restore", err)
	}

	printRestorePreview(resp)

	return nil
}

func printRestorePreview(resp *restorepb.PreviewRestoreResponse) {
	// Format timestamp
	var timestampStr string
	if resp.LastAppliedTimestamp > 0 {
		t := time.UnixMicro(int64(resp.LastAppliedTimestamp))
		timestampStr = t.Format(time.RFC3339)
	} else {
		timestampStr = "N/A"
	}

	pterm.DefaultSection.Println("Restore Preview")

	data := [][]string{
		{"Last Applied Index", fmt.Sprintf("%d", resp.LastAppliedIndex)},
		{"Last Applied Time", timestampStr},
		{"Last Log Sequence", fmt.Sprintf("%d", resp.LastSequence)},
		{"Ledger Count", fmt.Sprintf("%d", resp.LedgerCount)},
		{"Ledgers", strings.Join(resp.LedgerNames, ", ")},
	}

	_ = pterm.DefaultTable.
		WithHasHeader(false).
		WithBoxed(true).
		WithData(data).
		Render()
}
