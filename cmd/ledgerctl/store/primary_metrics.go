package store

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewPrimaryMetricsCommand creates the store primary metrics command.
func NewPrimaryMetricsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "metrics",
		Aliases:           []string{"m"},
		Short:             "Get primary store metrics",
		Long:              "Retrieve and display metrics from the primary RocksDB storage engine via gRPC",
		RunE:              runPrimaryMetrics,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmd.Flags().Uint32("node-id", 0, "Target node ID (0 = local node)")

	return cmd
}

func runPrimaryMetrics(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	nodeID, _ := cmd.Flags().GetUint32("node-id")

	spinner := cmdutil.StartSpinner("Fetching store metrics...")

	resp, err := client.GetPrimaryMetrics(ctx, &servicepb.GetPrimaryMetricsRequest{
		NodeId: nodeID,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get primary metrics", err)
	}

	if !resp.GetAvailable() {
		spinner.Warning("Primary store metrics are unavailable")

		return errors.New("primary store metrics not available")
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, resp.GetMetrics()); handled || err != nil {
		return err
	}

	pterm.Println()
	printFormattedMetrics(resp.GetMetrics())

	return nil
}

// printFormattedMetrics shows properties RocksDB actually reported. Optional
// fields are omitted when RocksDB did not expose the corresponding property.
func printFormattedMetrics(m *servicepb.StorageMetrics) {
	if m.BlockCacheUsageBytes != nil {
		pterm.DefaultSection.Println("Block Cache")
		_ = pterm.DefaultTable.WithHasHeader().WithData(pterm.TableData{
			{"METRIC", "VALUE"},
			{"Usage", cmdutil.FormatBytes(m.GetBlockCacheUsageBytes())},
		}).Render()
		pterm.Println()
	}

	if m.MemtableSizeBytes != nil {
		pterm.DefaultSection.Println("Memtables")
		_ = pterm.DefaultTable.WithHasHeader().WithData(pterm.TableData{
			{"METRIC", "VALUE"},
			{"Current Size", cmdutil.FormatBytes(m.GetMemtableSizeBytes())},
		}).Render()
		pterm.Println()
	}

	if m.PendingCompactionBytes != nil {
		pterm.DefaultSection.Println("Compaction")
		_ = pterm.DefaultTable.WithHasHeader().WithData(pterm.TableData{
			{"METRIC", "VALUE"},
			{"Estimated Pending Bytes", cmdutil.FormatBytes(m.GetPendingCompactionBytes())},
		}).Render()
		pterm.Println()
	}

	if m.SnapshotCount != nil {
		pterm.DefaultSection.Println("Snapshots")
		_ = pterm.DefaultTable.WithHasHeader().WithData(pterm.TableData{
			{"METRIC", "VALUE"},
			{"Count", strconv.FormatUint(m.GetSnapshotCount(), 10)},
		}).Render()
		pterm.Println()
	}

	if len(m.GetLevels()) > 0 {
		pterm.DefaultSection.Println("Live SST Files by Level")
		tableData := pterm.TableData{{"LEVEL", "FILES", "SIZE"}}
		for _, level := range m.GetLevels() {
			tableData = append(tableData, []string{
				fmt.Sprintf("L%d", level.GetLevel()),
				strconv.FormatUint(level.GetNumFiles(), 10),
				cmdutil.FormatBytes(level.GetSizeBytes()),
			})
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
	}
}
