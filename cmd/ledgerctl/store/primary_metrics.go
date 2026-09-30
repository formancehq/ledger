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

// printFormattedMetrics shows only values with direct RocksDB equivalents. The
// protobuf envelope still carries legacy fields for wire compatibility, but
// their zero values do not mean RocksDB measured zero.
func printFormattedMetrics(m *servicepb.PebbleMetrics) {
	if cache := m.GetBlockCache(); cache != nil {
		pterm.DefaultSection.Println("Block Cache")
		_ = pterm.DefaultTable.WithHasHeader().WithData(pterm.TableData{
			{"METRIC", "VALUE"},
			{"Usage", cmdutil.FormatBytes(uint64(cache.GetSize()))},
		}).Render()
		pterm.Println()
	}

	if memtable := m.GetMemTable(); memtable != nil {
		pterm.DefaultSection.Println("Memtables")
		_ = pterm.DefaultTable.WithHasHeader().WithData(pterm.TableData{
			{"METRIC", "VALUE"},
			{"Current Size", cmdutil.FormatBytes(memtable.GetSize())},
		}).Render()
		pterm.Println()
	}

	if compact := m.GetCompact(); compact != nil {
		pterm.DefaultSection.Println("Compaction")
		_ = pterm.DefaultTable.WithHasHeader().WithData(pterm.TableData{
			{"METRIC", "VALUE"},
			{"Estimated Pending Bytes", cmdutil.FormatBytes(compact.GetEstimatedDebt())},
		}).Render()
		pterm.Println()
	}

	if snapshots := m.GetSnapshots(); snapshots != nil {
		pterm.DefaultSection.Println("Snapshots")
		_ = pterm.DefaultTable.WithHasHeader().WithData(pterm.TableData{
			{"METRIC", "VALUE"},
			{"Count", strconv.Itoa(int(snapshots.GetCount()))},
		}).Render()
		pterm.Println()
	}

	if len(m.GetLevels()) > 0 {
		pterm.DefaultSection.Println("Live SST Files by Level")
		tableData := pterm.TableData{{"LEVEL", "FILES", "SIZE"}}
		for _, level := range m.GetLevels() {
			tableData = append(tableData, []string{
				fmt.Sprintf("L%d", level.GetLevel()),
				strconv.FormatInt(level.GetNumFiles(), 10),
				cmdutil.FormatBytes(uint64(level.GetSize())),
			})
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
	}
}
