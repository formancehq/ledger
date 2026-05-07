package store

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewPrimaryMetricsCommand creates the store primary metrics command.
func NewPrimaryMetricsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "metrics",
		Aliases: []string{"m"},
		Short:   "Get primary store metrics",
		Long:    "Retrieve and display metrics from the primary Pebble storage engine via gRPC",
		RunE:    runPrimaryMetrics,
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

	spinner, _ := pterm.DefaultSpinner.Start("Fetching store metrics...")

	resp, err := client.GetPrimaryMetrics(ctx, &servicepb.GetPrimaryMetricsRequest{
		NodeId: nodeID,
	})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to get primary metrics", err)
	}

	if !resp.GetAvailable() {
		spinner.Warning("Store metrics not available")
		pterm.Warning.Println("Storage type may not be Pebble")

		return errors.New("store metrics not available (storage type may not be Pebble)")
	}

	_ = spinner.Stop()

	if handled, err := cmdutil.EncodeStructured(cmd, resp.GetMetrics()); handled || err != nil {
		return err
	}

	pterm.Println()
	printFormattedMetrics(resp.GetMetrics())

	return nil
}

func printFormattedMetrics(m *servicepb.PebbleMetrics) {
	// General
	pterm.DefaultSection.Println("General")
	pterm.Printf("Disk Space Usage: %s\n\n", cmdutil.FormatBytes(m.GetDiskSpaceUsage()))

	// Block Cache
	if m.GetBlockCache() != nil {
		pterm.DefaultSection.Println("Block Cache")

		tableData := pterm.TableData{
			{"METRIC", "VALUE"},
			{"Size", cmdutil.FormatBytes(uint64(m.GetBlockCache().GetSize()))},
			{"Count", strconv.FormatInt(m.GetBlockCache().GetCount(), 10)},
			{"Hits", strconv.FormatInt(m.GetBlockCache().GetHits(), 10)},
			{"Misses", strconv.FormatInt(m.GetBlockCache().GetMisses(), 10)},
		}
		if m.GetBlockCache().GetHits()+m.GetBlockCache().GetMisses() > 0 {
			hitRate := float64(m.GetBlockCache().GetHits()) / float64(m.GetBlockCache().GetHits()+m.GetBlockCache().GetMisses()) * 100
			tableData = append(tableData, []string{"Hit Rate", fmt.Sprintf("%.2f%%", hitRate)})
		}

		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

		pterm.Println()
	}

	// Table Cache
	if m.GetTableCache() != nil {
		pterm.DefaultSection.Println("Table Cache")

		tableData := pterm.TableData{
			{"METRIC", "VALUE"},
			{"Size", strconv.FormatInt(m.GetTableCache().GetSize(), 10)},
			{"Count", strconv.FormatInt(m.GetTableCache().GetCount(), 10)},
			{"Hits", strconv.FormatInt(m.GetTableCache().GetHits(), 10)},
			{"Misses", strconv.FormatInt(m.GetTableCache().GetMisses(), 10)},
		}
		if m.GetTableCache().GetHits()+m.GetTableCache().GetMisses() > 0 {
			hitRate := float64(m.GetTableCache().GetHits()) / float64(m.GetTableCache().GetHits()+m.GetTableCache().GetMisses()) * 100
			tableData = append(tableData, []string{"Hit Rate", fmt.Sprintf("%.2f%%", hitRate)})
		}

		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

		pterm.Println()
	}

	// MemTable
	if m.GetMemTable() != nil {
		pterm.DefaultSection.Println("MemTable")

		tableData := pterm.TableData{
			{"METRIC", "VALUE"},
			{"Size", cmdutil.FormatBytes(m.GetMemTable().GetSize())},
			{"Count", strconv.FormatInt(m.GetMemTable().GetCount(), 10)},
			{"Zombie Size", cmdutil.FormatBytes(m.GetMemTable().GetZombieSize())},
			{"Zombie Count", strconv.FormatInt(m.GetMemTable().GetZombieCount(), 10)},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

		pterm.Println()
	}

	// WAL
	if m.GetWal() != nil {
		pterm.DefaultSection.Println("Write-Ahead Log (WAL)")

		tableData := pterm.TableData{
			{"METRIC", "VALUE"},
			{"Files", strconv.FormatInt(m.GetWal().GetFiles(), 10)},
			{"Obsolete Files", strconv.FormatInt(m.GetWal().GetObsoleteFiles(), 10)},
			{"Size", cmdutil.FormatBytes(m.GetWal().GetSize())},
			{"Bytes In", cmdutil.FormatBytes(m.GetWal().GetBytesIn())},
			{"Bytes Written", cmdutil.FormatBytes(m.GetWal().GetBytesWritten())},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

		pterm.Println()
	}

	// Compaction
	if m.GetCompact() != nil {
		pterm.DefaultSection.Println("Compaction")

		tableData := pterm.TableData{
			{"METRIC", "VALUE"},
			{"Total Count", strconv.FormatInt(m.GetCompact().GetCount(), 10)},
			{"Default", strconv.FormatInt(m.GetCompact().GetDefaultCount(), 10)},
			{"Del-Only", strconv.FormatInt(m.GetCompact().GetDeleteOnlyCount(), 10)},
			{"Elision-Only", strconv.FormatInt(m.GetCompact().GetElisionOnlyCount(), 10)},
			{"Move", strconv.FormatInt(m.GetCompact().GetMoveCount(), 10)},
			{"Read", strconv.FormatInt(m.GetCompact().GetReadCount(), 10)},
			{"Rewrite", strconv.FormatInt(m.GetCompact().GetRewriteCount(), 10)},
			{"Multi-Level", strconv.FormatInt(m.GetCompact().GetMultiLevelCount(), 10)},
			{"Estimated Debt", cmdutil.FormatBytes(m.GetCompact().GetEstimatedDebt())},
			{"In Progress Bytes", cmdutil.FormatBytes(uint64(m.GetCompact().GetInProgressBytes()))},
			{"Num In Progress", strconv.FormatInt(m.GetCompact().GetNumInProgress(), 10)},
			{"Marked Files", strconv.Itoa(int(m.GetCompact().GetMarkedFiles()))},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

		pterm.Println()
	}

	// Flush
	if m.GetFlush() != nil {
		pterm.DefaultSection.Println("Flush")

		tableData := pterm.TableData{
			{"METRIC", "VALUE"},
			{"Count", strconv.FormatInt(m.GetFlush().GetCount(), 10)},
			{"In Progress", strconv.FormatInt(m.GetFlush().GetNumInProgress(), 10)},
			{"As Ingest Count", strconv.FormatUint(m.GetFlush().GetAsIngestCount(), 10)},
			{"As Ingest Tables", strconv.FormatUint(m.GetFlush().GetAsIngestTableCount(), 10)},
			{"As Ingest Bytes", cmdutil.FormatBytes(m.GetFlush().GetAsIngestBytes())},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

		pterm.Println()
	}

	// Snapshots
	if m.GetSnapshots() != nil {
		pterm.DefaultSection.Println("Snapshots")

		tableData := pterm.TableData{
			{"METRIC", "VALUE"},
			{"Count", strconv.Itoa(int(m.GetSnapshots().GetCount()))},
			{"Earliest Seq Num", strconv.FormatUint(m.GetSnapshots().GetEarliestSeqNum(), 10)},
			{"Pinned Keys", strconv.FormatUint(m.GetSnapshots().GetPinnedKeys(), 10)},
			{"Pinned Size", cmdutil.FormatBytes(m.GetSnapshots().GetPinnedSize())},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

		pterm.Println()
	}

	// Tables
	if m.GetTable() != nil {
		pterm.DefaultSection.Println("Tables (SST)")

		tableData := pterm.TableData{
			{"METRIC", "VALUE"},
			{"Zombie Size", cmdutil.FormatBytes(m.GetTable().GetZombieSize())},
			{"Zombie Count", strconv.FormatInt(m.GetTable().GetZombieCount(), 10)},
			{"Backing Table Count", strconv.FormatUint(m.GetTable().GetBackingTableCount(), 10)},
			{"Backing Table Size", cmdutil.FormatBytes(m.GetTable().GetBackingTableSize())},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

		pterm.Println()
	}

	// Keys
	if m.GetKeys() != nil {
		pterm.DefaultSection.Println("Keys")

		tableData := pterm.TableData{
			{"METRIC", "VALUE"},
			{"Range Key Sets", strconv.FormatUint(m.GetKeys().GetRangeKeySetsCount(), 10)},
			{"Tombstones", strconv.FormatUint(m.GetKeys().GetTombstoneCount(), 10)},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

		pterm.Println()
	}

	// Levels
	if len(m.GetLevels()) > 0 {
		pterm.DefaultSection.Println("Levels")

		tableData := pterm.TableData{
			{"LEVEL", "FILES", "SIZE", "SCORE", "BYTES IN", "BYTES COMPACTED"},
		}
		for _, level := range m.GetLevels() {
			tableData = append(tableData, []string{
				fmt.Sprintf("L%d", level.GetLevel()),
				strconv.FormatInt(level.GetNumFiles(), 10),
				cmdutil.FormatBytes(uint64(level.GetSize())),
				fmt.Sprintf("%.2f", level.GetScore()),
				cmdutil.FormatBytes(level.GetBytesIn()),
				cmdutil.FormatBytes(level.GetBytesCompacted()),
			})
		}

		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
	}
}
