package ledgers

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewStatsCommand creates the ledgers stats command.
func NewStatsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "stats",
		Aliases: []string{"st"},
		Short:   "Get ledger statistics",
		Long:    "Get aggregate statistics (account count, transaction count) for a ledger via gRPC",
		RunE:    runStats,
	}

	cmd.Flags().String("ledger", "", "Ledger name (interactive selection if omitted)")
	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runStats(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ledgerFlag, _ := cmd.Flags().GetString("ledger")

	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Fetching stats for ledger %s...", ledgerName))

	checkpointID, _ := cmd.Flags().GetUint64("checkpoint-id")

	stats, err := client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{
		Ledger:       ledgerName,
		CheckpointId: checkpointID,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get ledger stats", err)
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, stats); handled || err != nil {
		return err
	}

	pterm.Println()
	pterm.Printf("Ledger: %s\n", pterm.Cyan(ledgerName))
	pterm.Println(pterm.Gray("─────────────────────────────────"))
	pterm.Printf("Transactions:       %d\n", stats.GetTransactionCount())
	pterm.Printf("Postings:           %d\n", stats.GetPostingCount())
	pterm.Printf("Logs:               %d\n", stats.GetLogCount())
	pterm.Printf("Volumes:            %d\n", stats.GetVolumeCount())
	pterm.Printf("Metadata:           %d\n", stats.GetMetadataCount())
	pterm.Printf("References:         %d\n", stats.GetReferenceCount())
	pterm.Printf("Reverts:            %d\n", stats.GetRevertCount())
	pterm.Printf("Numscript execs:    %d\n", stats.GetNumscriptExecutionCount())
	pterm.Printf("Ephemeral evicted:  %d\n", stats.GetEphemeralEvictedCount())
	pterm.Printf("Transient used:     %d\n", stats.GetTransientUsedCount())

	return nil
}
