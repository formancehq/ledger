package server

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/internal/bootstrap"
	"github.com/formancehq/ledger/v3/internal/pkg/bytesize"
)

const (
	balanceHistoryDirFlag                 = "balance-history-dir"
	balanceHistoryEnabledFlag             = "balance-history-enabled"
	balanceHistoryLedgersFlag             = "balance-history-ledgers"
	balanceHistoryBuilderBatchSizeFlag    = "balance-history-builder-batch-size"
	balanceHistoryCompactionThresholdFlag = "balance-history-compaction-threshold"
	balanceHistoryMaintenanceIntervalFlag = "balance-history-maintenance-interval"
	balanceHistoryMaxCompactionsFlag      = "balance-history-max-compactions-per-pass"
	balanceHistoryBackfillYieldFlag       = "balance-history-backfill-yield"
	balanceHistoryWALSyncIntervalFlag     = "balance-history-wal-sync-interval"
	balanceHistoryVerifierIntervalFlag    = "balance-history-verifier-interval"
	balanceHistoryVerifierReplayEveryFlag = "balance-history-verifier-replay-every"
	balanceHistoryColdTierFlag            = "balance-history-cold-tier"
	balanceHistoryRetainLocalRunsFlag     = "balance-history-retain-local-runs"
	balanceHistoryArchiveCacheBytesFlag   = "balance-history-archive-cache-max-bytes"
	balanceHistoryMaxSegmentBytesFlag     = "balance-history-max-segment-bytes"
	balanceHistoryMaxRunsPerTierPassFlag  = "balance-history-max-runs-per-tier-pass"
	balanceHistoryTierIntervalFlag        = "balance-history-tier-interval"
	balanceHistoryRemoteGCIntervalFlag    = "balance-history-remote-gc-interval"
	balanceHistoryRemoteGCGracePeriodFlag = "balance-history-remote-gc-grace-period"
	balanceHistoryRemoteGCScanLimitFlag   = "balance-history-remote-gc-scan-limit"
	balanceHistoryRemoteGCDeleteLimitFlag = "balance-history-remote-gc-delete-limit"
)

func addBalanceHistoryFlags(cmd *cobra.Command) {
	defaults := bootstrap.DefaultBalanceHistoryConfig()
	cmd.Flags().String(balanceHistoryDirFlag, defaults.Dir, "Directory for the balance-history peer store (default: <data-dir>/balance-history)")
	cmd.Flags().Bool(balanceHistoryEnabledFlag, defaults.Enabled, "Enable the asynchronous PIT balance-history projection")
	cmd.Flags().StringSlice(balanceHistoryLedgersFlag, defaults.Ledgers, "Exact ledger names allowed to use PIT reads (empty = all ledgers)")
	cmd.Flags().Int(balanceHistoryBuilderBatchSizeFlag, defaults.BuilderBatchSize, "Maximum complete audit proposals per balance-history publication")
	cmd.Flags().Int(balanceHistoryCompactionThresholdFlag, defaults.RunCompactionThreshold, "Logical runs at one level before balance-history compaction")
	cmd.Flags().Duration(balanceHistoryMaintenanceIntervalFlag, defaults.MaintenanceInterval, "Interval between bounded local balance-history maintenance passes")
	cmd.Flags().Int(balanceHistoryMaxCompactionsFlag, defaults.MaxCompactionsPerPass, "Maximum balance-history compactions performed by one maintenance pass")
	cmd.Flags().Duration(balanceHistoryBackfillYieldFlag, defaults.BackfillYield, "Cooperative pause between boot-time balance-history backfill batches")
	cmd.Flags().Duration(balanceHistoryWALSyncIntervalFlag, defaults.DurabilityInterval, "Maximum asynchronous balance-history WAL durability interval")
	cmd.Flags().Duration(balanceHistoryVerifierIntervalFlag, defaults.VerifierInterval, "Interval between balance-history integrity verification passes")
	cmd.Flags().Uint64(balanceHistoryVerifierReplayEveryFlag, defaults.VerifierReplayEvery, "Run a full balance-history source replay every N verifier passes")
	cmd.Flags().Bool(balanceHistoryColdTierFlag, defaults.ColdTierEnabled, "Archive sealed balance-history runs to configured cold storage")
	cmd.Flags().Int(balanceHistoryRetainLocalRunsFlag, defaults.RetainLocalRuns, "Archived balance-history runs to retain locally outside the byte-bounded cache")
	bytesize.ByteSizeVar(cmd, new(bytesize.ByteSize), balanceHistoryArchiveCacheBytesFlag, bytesize.ByteSize(defaults.ArchiveCacheMaxBytes), "Maximum bytes for hydrated cold balance-history runs")
	bytesize.ByteSizeVar(cmd, new(bytesize.ByteSize), balanceHistoryMaxSegmentBytesFlag, bytesize.ByteSize(defaults.MaxSegmentBytes), "Maximum encoded bytes in one cold balance-history segment")
	cmd.Flags().Int(balanceHistoryMaxRunsPerTierPassFlag, defaults.MaxRunsPerTierPass, "Maximum immutable runs uploaded by one balance-history tier pass")
	cmd.Flags().Duration(balanceHistoryTierIntervalFlag, defaults.TierInterval, "Interval between bounded balance-history cold-tier passes")
	cmd.Flags().Duration(balanceHistoryRemoteGCIntervalFlag, defaults.RemoteGCInterval, "Interval between bounded balance-history remote-GC passes")
	cmd.Flags().Duration(balanceHistoryRemoteGCGracePeriodFlag, defaults.RemoteGCGracePeriod, "Minimum age before an unreferenced balance-history archive may be deleted")
	cmd.Flags().Int(balanceHistoryRemoteGCScanLimitFlag, defaults.RemoteGCScanLimit, "Maximum remote balance-history objects listed by one GC pass")
	cmd.Flags().Int(balanceHistoryRemoteGCDeleteLimitFlag, defaults.RemoteGCDeleteLimit, "Maximum remote balance-history objects deleted by one GC pass")
}

func balanceHistoryConfigFromFlags(cmd *cobra.Command) bootstrap.BalanceHistoryConfig {
	defaults := bootstrap.DefaultBalanceHistoryConfig()
	if cmd.Flags().Lookup(balanceHistoryEnabledFlag) == nil {
		return defaults
	}

	dir, _ := cmd.Flags().GetString(balanceHistoryDirFlag)
	enabled, _ := cmd.Flags().GetBool(balanceHistoryEnabledFlag)
	ledgers, _ := cmd.Flags().GetStringSlice(balanceHistoryLedgersFlag)
	if len(ledgers) == 0 {
		ledgers = nil
	}
	batchSize, _ := cmd.Flags().GetInt(balanceHistoryBuilderBatchSizeFlag)
	compactionThreshold, _ := cmd.Flags().GetInt(balanceHistoryCompactionThresholdFlag)
	maintenanceInterval, _ := cmd.Flags().GetDuration(balanceHistoryMaintenanceIntervalFlag)
	maxCompactionsPerPass, _ := cmd.Flags().GetInt(balanceHistoryMaxCompactionsFlag)
	backfillYield, _ := cmd.Flags().GetDuration(balanceHistoryBackfillYieldFlag)
	durabilityInterval, _ := cmd.Flags().GetDuration(balanceHistoryWALSyncIntervalFlag)
	verifierInterval, _ := cmd.Flags().GetDuration(balanceHistoryVerifierIntervalFlag)
	verifierReplayEvery, _ := cmd.Flags().GetUint64(balanceHistoryVerifierReplayEveryFlag)
	coldTierEnabled, _ := cmd.Flags().GetBool(balanceHistoryColdTierFlag)
	retainLocalRuns, _ := cmd.Flags().GetInt(balanceHistoryRetainLocalRunsFlag)
	archiveCacheMaxBytes := bytesize.Get(cmd, balanceHistoryArchiveCacheBytesFlag).Int64()
	maxSegmentBytes := bytesize.Get(cmd, balanceHistoryMaxSegmentBytesFlag).Int64()
	maxRunsPerTierPass, _ := cmd.Flags().GetInt(balanceHistoryMaxRunsPerTierPassFlag)
	tierInterval, _ := cmd.Flags().GetDuration(balanceHistoryTierIntervalFlag)
	remoteGCInterval, _ := cmd.Flags().GetDuration(balanceHistoryRemoteGCIntervalFlag)
	remoteGCGracePeriod, _ := cmd.Flags().GetDuration(balanceHistoryRemoteGCGracePeriodFlag)
	remoteGCScanLimit, _ := cmd.Flags().GetInt(balanceHistoryRemoteGCScanLimitFlag)
	remoteGCDeleteLimit, _ := cmd.Flags().GetInt(balanceHistoryRemoteGCDeleteLimitFlag)

	return bootstrap.BalanceHistoryConfig{
		Dir:                    dir,
		Enabled:                enabled,
		Ledgers:                ledgers,
		BuilderBatchSize:       batchSize,
		RunCompactionThreshold: compactionThreshold,
		MaintenanceInterval:    maintenanceInterval,
		MaxCompactionsPerPass:  maxCompactionsPerPass,
		BackfillYield:          backfillYield,
		DurabilityInterval:     durabilityInterval,
		VerifierInterval:       verifierInterval,
		VerifierReplayEvery:    verifierReplayEvery,
		ColdTierEnabled:        coldTierEnabled,
		RetainLocalRuns:        retainLocalRuns,
		ArchiveCacheMaxBytes:   archiveCacheMaxBytes,
		MaxSegmentBytes:        maxSegmentBytes,
		MaxRunsPerTierPass:     maxRunsPerTierPass,
		TierInterval:           tierInterval,
		RemoteGCInterval:       remoteGCInterval,
		RemoteGCGracePeriod:    remoteGCGracePeriod,
		RemoteGCScanLimit:      remoteGCScanLimit,
		RemoteGCDeleteLimit:    remoteGCDeleteLimit,
	}
}
