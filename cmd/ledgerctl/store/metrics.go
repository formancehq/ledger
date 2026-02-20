package store

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewMetricsCommand creates the store metrics command.
func NewMetricsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "metrics",
		Aliases: []string{"m", "stats"},
		Short:   "Get store metrics",
		Long:    "Retrieve and display metrics from the Pebble storage engine via gRPC",
		RunE:    runMetrics,
	}

	cmd.Flags().Bool("json", false, "Output as JSON instead of formatted table")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runMetrics(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Fetching store metrics...")

	resp, err := client.GetStoreMetrics(ctx, &servicepb.GetStoreMetricsRequest{})
	if err != nil {
		spinner.Fail("Failed to get store metrics")
		return cmdutil.FormatGRPCError("failed to get store metrics", err)
	}

	if !resp.Available {
		spinner.Warning("Store metrics not available")
		pterm.Warning.Println("Storage type may not be Pebble")
		return fmt.Errorf("store metrics not available (storage type may not be Pebble)")
	}

	_ = spinner.Stop()

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(resp.Metrics)
	}

	pterm.Println()
	printFormattedMetrics(resp.Metrics)
	return nil
}

func printFormattedMetrics(m *servicepb.PebbleMetrics) {
	// General
	pterm.Println("General")
	pterm.Println(pterm.Gray("─────────────────────────────────"))
	pterm.Printf("Disk Space Usage: %s\n\n", cmdutil.FormatBytes(m.DiskSpaceUsage))

	// Block Cache
	if m.BlockCache != nil {
		pterm.Println("Block Cache")
		pterm.Println(pterm.Gray("─────────────────────────────────"))
		tableData := pterm.TableData{
			{"Metric", "Value"},
			{"Size", cmdutil.FormatBytes(uint64(m.BlockCache.Size))},
			{"Count", fmt.Sprintf("%d", m.BlockCache.Count)},
			{"Hits", fmt.Sprintf("%d", m.BlockCache.Hits)},
			{"Misses", fmt.Sprintf("%d", m.BlockCache.Misses)},
		}
		if m.BlockCache.Hits+m.BlockCache.Misses > 0 {
			hitRate := float64(m.BlockCache.Hits) / float64(m.BlockCache.Hits+m.BlockCache.Misses) * 100
			tableData = append(tableData, []string{"Hit Rate", fmt.Sprintf("%.2f%%", hitRate)})
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
		pterm.Println()
	}

	// Table Cache
	if m.TableCache != nil {
		pterm.Println("Table Cache")
		pterm.Println(pterm.Gray("─────────────────────────────────"))
		tableData := pterm.TableData{
			{"Metric", "Value"},
			{"Size", fmt.Sprintf("%d", m.TableCache.Size)},
			{"Count", fmt.Sprintf("%d", m.TableCache.Count)},
			{"Hits", fmt.Sprintf("%d", m.TableCache.Hits)},
			{"Misses", fmt.Sprintf("%d", m.TableCache.Misses)},
		}
		if m.TableCache.Hits+m.TableCache.Misses > 0 {
			hitRate := float64(m.TableCache.Hits) / float64(m.TableCache.Hits+m.TableCache.Misses) * 100
			tableData = append(tableData, []string{"Hit Rate", fmt.Sprintf("%.2f%%", hitRate)})
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
		pterm.Println()
	}

	// MemTable
	if m.MemTable != nil {
		pterm.Println("MemTable")
		pterm.Println(pterm.Gray("─────────────────────────────────"))
		tableData := pterm.TableData{
			{"Metric", "Value"},
			{"Size", cmdutil.FormatBytes(m.MemTable.Size)},
			{"Count", fmt.Sprintf("%d", m.MemTable.Count)},
			{"Zombie Size", cmdutil.FormatBytes(m.MemTable.ZombieSize)},
			{"Zombie Count", fmt.Sprintf("%d", m.MemTable.ZombieCount)},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
		pterm.Println()
	}

	// WAL
	if m.Wal != nil {
		pterm.Println("Write-Ahead Log (WAL)")
		pterm.Println(pterm.Gray("─────────────────────────────────"))
		tableData := pterm.TableData{
			{"Metric", "Value"},
			{"Files", fmt.Sprintf("%d", m.Wal.Files)},
			{"Obsolete Files", fmt.Sprintf("%d", m.Wal.ObsoleteFiles)},
			{"Size", cmdutil.FormatBytes(m.Wal.Size)},
			{"Bytes In", cmdutil.FormatBytes(m.Wal.BytesIn)},
			{"Bytes Written", cmdutil.FormatBytes(m.Wal.BytesWritten)},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
		pterm.Println()
	}

	// Compaction
	if m.Compact != nil {
		pterm.Println("Compaction")
		pterm.Println(pterm.Gray("─────────────────────────────────"))
		tableData := pterm.TableData{
			{"Metric", "Value"},
			{"Total Count", fmt.Sprintf("%d", m.Compact.Count)},
			{"Default", fmt.Sprintf("%d", m.Compact.DefaultCount)},
			{"Del-Only", fmt.Sprintf("%d", m.Compact.DeleteOnlyCount)},
			{"Elision-Only", fmt.Sprintf("%d", m.Compact.ElisionOnlyCount)},
			{"Move", fmt.Sprintf("%d", m.Compact.MoveCount)},
			{"Read", fmt.Sprintf("%d", m.Compact.ReadCount)},
			{"Rewrite", fmt.Sprintf("%d", m.Compact.RewriteCount)},
			{"Multi-Level", fmt.Sprintf("%d", m.Compact.MultiLevelCount)},
			{"Estimated Debt", cmdutil.FormatBytes(m.Compact.EstimatedDebt)},
			{"In Progress Bytes", cmdutil.FormatBytes(uint64(m.Compact.InProgressBytes))},
			{"Num In Progress", fmt.Sprintf("%d", m.Compact.NumInProgress)},
			{"Marked Files", fmt.Sprintf("%d", m.Compact.MarkedFiles)},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
		pterm.Println()
	}

	// Flush
	if m.Flush != nil {
		pterm.Println("Flush")
		pterm.Println(pterm.Gray("─────────────────────────────────"))
		tableData := pterm.TableData{
			{"Metric", "Value"},
			{"Count", fmt.Sprintf("%d", m.Flush.Count)},
			{"In Progress", fmt.Sprintf("%d", m.Flush.NumInProgress)},
			{"As Ingest Count", fmt.Sprintf("%d", m.Flush.AsIngestCount)},
			{"As Ingest Tables", fmt.Sprintf("%d", m.Flush.AsIngestTableCount)},
			{"As Ingest Bytes", cmdutil.FormatBytes(m.Flush.AsIngestBytes)},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
		pterm.Println()
	}

	// Snapshots
	if m.Snapshots != nil {
		pterm.Println("Snapshots")
		pterm.Println(pterm.Gray("─────────────────────────────────"))
		tableData := pterm.TableData{
			{"Metric", "Value"},
			{"Count", fmt.Sprintf("%d", m.Snapshots.Count)},
			{"Earliest Seq Num", fmt.Sprintf("%d", m.Snapshots.EarliestSeqNum)},
			{"Pinned Keys", fmt.Sprintf("%d", m.Snapshots.PinnedKeys)},
			{"Pinned Size", cmdutil.FormatBytes(m.Snapshots.PinnedSize)},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
		pterm.Println()
	}

	// Tables
	if m.Table != nil {
		pterm.Println("Tables (SST)")
		pterm.Println(pterm.Gray("─────────────────────────────────"))
		tableData := pterm.TableData{
			{"Metric", "Value"},
			{"Zombie Size", cmdutil.FormatBytes(m.Table.ZombieSize)},
			{"Zombie Count", fmt.Sprintf("%d", m.Table.ZombieCount)},
			{"Backing Table Count", fmt.Sprintf("%d", m.Table.BackingTableCount)},
			{"Backing Table Size", cmdutil.FormatBytes(m.Table.BackingTableSize)},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
		pterm.Println()
	}

	// Keys
	if m.Keys != nil {
		pterm.Println("Keys")
		pterm.Println(pterm.Gray("─────────────────────────────────"))
		tableData := pterm.TableData{
			{"Metric", "Value"},
			{"Range Key Sets", fmt.Sprintf("%d", m.Keys.RangeKeySetsCount)},
			{"Tombstones", fmt.Sprintf("%d", m.Keys.TombstoneCount)},
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
		pterm.Println()
	}

	// Levels
	if len(m.Levels) > 0 {
		pterm.Println("Levels")
		pterm.Println(pterm.Gray("─────────────────────────────────"))
		tableData := pterm.TableData{
			{"Level", "Files", "Size", "Score", "Bytes In", "Bytes Compacted"},
		}
		for _, level := range m.Levels {
			tableData = append(tableData, []string{
				fmt.Sprintf("L%d", level.Level),
				fmt.Sprintf("%d", level.NumFiles),
				cmdutil.FormatBytes(uint64(level.Size)),
				fmt.Sprintf("%.2f", level.Score),
				cmdutil.FormatBytes(level.BytesIn),
				cmdutil.FormatBytes(level.BytesCompacted),
			})
		}
		_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
	}
}
