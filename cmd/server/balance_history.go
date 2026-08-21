package server

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/internal/bootstrap"
)

const (
	balanceHistoryDirFlag                 = "balance-history-dir"
	balanceHistoryBuilderBatchSizeFlag    = "balance-history-builder-batch-size"
	balanceHistoryCompactionThresholdFlag = "balance-history-segment-compaction-threshold"
	balanceHistoryMaintenanceIntervalFlag = "balance-history-maintenance-interval"
	balanceHistoryMaxCompactionsFlag      = "balance-history-max-compactions-per-pass"
	balanceHistoryBackfillYieldFlag       = "balance-history-backfill-yield"
	balanceHistoryWALSyncIntervalFlag     = "balance-history-wal-sync-interval"
)

// addBalanceHistoryFlags exposes only replica-local resource tuning. Ledger
// activation is a client command and therefore cannot drift across nodes.
func addBalanceHistoryFlags(cmd *cobra.Command) {
	defaults := bootstrap.DefaultBalanceHistoryConfig()
	cmd.Flags().String(balanceHistoryDirFlag, defaults.Dir, "Directory for the historical-balance peer store (default: <data-dir>/balance-history)")
	cmd.Flags().Int(balanceHistoryBuilderBatchSizeFlag, defaults.BuilderBatchSize, "Maximum complete audit proposals per historical-balance publication")
	cmd.Flags().Int(balanceHistoryCompactionThresholdFlag, defaults.SegmentCompactionThreshold, "Logical segments at one level before historical-balance compaction")
	cmd.Flags().Duration(balanceHistoryMaintenanceIntervalFlag, defaults.MaintenanceInterval, "Fallback interval for historical-balance maintenance when no store notification arrives")
	cmd.Flags().Int(balanceHistoryMaxCompactionsFlag, defaults.MaxCompactionsPerPass, "Historical-balance compactions per scheduling slice while work remains")
	cmd.Flags().Duration(balanceHistoryBackfillYieldFlag, defaults.BackfillYield, "Cooperative pause between historical-balance backfill batches")
	cmd.Flags().Duration(balanceHistoryWALSyncIntervalFlag, defaults.DurabilityInterval, "Maximum asynchronous historical-balance WAL durability interval")
}

func balanceHistoryConfigFromFlags(cmd *cobra.Command) bootstrap.BalanceHistoryConfig {
	defaults := bootstrap.DefaultBalanceHistoryConfig()
	if cmd.Flags().Lookup(balanceHistoryDirFlag) == nil {
		return defaults
	}

	dir, _ := cmd.Flags().GetString(balanceHistoryDirFlag)
	batchSize, _ := cmd.Flags().GetInt(balanceHistoryBuilderBatchSizeFlag)
	compactionThreshold, _ := cmd.Flags().GetInt(balanceHistoryCompactionThresholdFlag)
	maintenanceInterval, _ := cmd.Flags().GetDuration(balanceHistoryMaintenanceIntervalFlag)
	maxCompactionsPerPass, _ := cmd.Flags().GetInt(balanceHistoryMaxCompactionsFlag)
	backfillYield, _ := cmd.Flags().GetDuration(balanceHistoryBackfillYieldFlag)
	durabilityInterval, _ := cmd.Flags().GetDuration(balanceHistoryWALSyncIntervalFlag)

	return bootstrap.BalanceHistoryConfig{
		Dir:                        dir,
		BuilderBatchSize:           batchSize,
		SegmentCompactionThreshold: compactionThreshold,
		MaintenanceInterval:        maintenanceInterval,
		MaxCompactionsPerPass:      maxCompactionsPerPass,
		BackfillYield:              backfillYield,
		DurabilityInterval:         durabilityInterval,
	}
}
