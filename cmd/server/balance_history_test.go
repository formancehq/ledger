package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/internal/bootstrap"
)

func TestLoadBalanceHistoryConfigDefaults(t *testing.T) {
	t.Parallel()

	command := NewRunCommand()
	require.NoError(t, command.Flags().Set("node-id", "1"))

	config, err := LoadConfig(context.Background(), command)
	require.NoError(t, err)
	require.Equal(t, bootstrap.DefaultBalanceHistoryConfig(), config.BalanceHistoryConfig)
}

func TestLoadBalanceHistoryConfigOverrides(t *testing.T) {
	t.Parallel()

	command := NewRunCommand()
	require.NoError(t, command.Flags().Set("node-id", "1"))
	require.NoError(t, command.Flags().Set(balanceHistoryDirFlag, "/dedicated/history"))
	require.NoError(t, command.Flags().Set(balanceHistoryEnabledFlag, "true"))
	require.NoError(t, command.Flags().Set(balanceHistoryLedgersFlag, "canary,shadow"))
	require.NoError(t, command.Flags().Set(balanceHistoryBuilderBatchSizeFlag, "321"))
	require.NoError(t, command.Flags().Set(balanceHistoryCompactionThresholdFlag, "7"))
	require.NoError(t, command.Flags().Set(balanceHistoryMaintenanceIntervalFlag, "2s"))
	require.NoError(t, command.Flags().Set(balanceHistoryMaxCompactionsFlag, "6"))
	require.NoError(t, command.Flags().Set(balanceHistoryBackfillYieldFlag, "12ms"))
	require.NoError(t, command.Flags().Set(balanceHistoryWALSyncIntervalFlag, "3s"))
	require.NoError(t, command.Flags().Set(balanceHistoryVerifierIntervalFlag, "20m"))
	require.NoError(t, command.Flags().Set(balanceHistoryVerifierReplayEveryFlag, "9"))
	require.NoError(t, command.Flags().Set(balanceHistoryColdTierFlag, "true"))
	require.NoError(t, command.Flags().Set(balanceHistoryRetainLocalRunsFlag, "11"))
	require.NoError(t, command.Flags().Set(balanceHistoryArchiveCacheBytesFlag, "2Gi"))
	require.NoError(t, command.Flags().Set(balanceHistoryMaxSegmentBytesFlag, "64Mi"))
	require.NoError(t, command.Flags().Set(balanceHistoryMaxRunsPerTierPassFlag, "3"))
	require.NoError(t, command.Flags().Set(balanceHistoryTierIntervalFlag, "45s"))
	require.NoError(t, command.Flags().Set(balanceHistoryRemoteGCIntervalFlag, "30m"))
	require.NoError(t, command.Flags().Set(balanceHistoryRemoteGCGracePeriodFlag, "48h"))
	require.NoError(t, command.Flags().Set(balanceHistoryRemoteGCScanLimitFlag, "444"))
	require.NoError(t, command.Flags().Set(balanceHistoryRemoteGCDeleteLimitFlag, "22"))
	require.NoError(t, command.Flags().Set("cold-storage-driver", "filesystem"))

	config, err := LoadConfig(context.Background(), command)
	require.NoError(t, err)
	require.Equal(t, bootstrap.BalanceHistoryConfig{
		Dir:                    "/dedicated/history",
		Enabled:                true,
		Ledgers:                []string{"canary", "shadow"},
		BuilderBatchSize:       321,
		RunCompactionThreshold: 7,
		MaintenanceInterval:    2 * time.Second,
		MaxCompactionsPerPass:  6,
		BackfillYield:          12 * time.Millisecond,
		DurabilityInterval:     3 * time.Second,
		VerifierInterval:       20 * time.Minute,
		VerifierReplayEvery:    9,
		ColdTierEnabled:        true,
		RetainLocalRuns:        11,
		ArchiveCacheMaxBytes:   2 << 30,
		MaxSegmentBytes:        64 << 20,
		MaxRunsPerTierPass:     3,
		TierInterval:           45 * time.Second,
		RemoteGCInterval:       30 * time.Minute,
		RemoteGCGracePeriod:    48 * time.Hour,
		RemoteGCScanLimit:      444,
		RemoteGCDeleteLimit:    22,
	}, config.BalanceHistoryConfig)
	require.NoError(t, config.BalanceHistoryConfig.Validate(config.ColdStorageConfig))
}

func TestLoadBalanceHistoryConfigRejectsColdTierWithoutBackend(t *testing.T) {
	t.Parallel()

	command := NewRunCommand()
	require.NoError(t, command.Flags().Set("node-id", "1"))
	require.NoError(t, command.Flags().Set(balanceHistoryEnabledFlag, "true"))
	require.NoError(t, command.Flags().Set(balanceHistoryColdTierFlag, "true"))

	config, err := LoadConfig(context.Background(), command)
	require.NoError(t, err)
	require.ErrorContains(t, config.BalanceHistoryConfig.Validate(config.ColdStorageConfig), "requires --cold-storage-driver=filesystem or s3")
}

func TestLoadBalanceHistoryConfigFromEnvironment(t *testing.T) {
	t.Setenv("BALANCE_HISTORY_DIR", "/history-volume")
	t.Setenv("BALANCE_HISTORY_LEDGERS", "canary,shadow")
	t.Setenv("BALANCE_HISTORY_BUILDER_BATCH_SIZE", "77")
	t.Setenv("BALANCE_HISTORY_MAINTENANCE_INTERVAL", "3s")
	t.Setenv("BALANCE_HISTORY_MAX_COMPACTIONS_PER_PASS", "5")
	t.Setenv("BALANCE_HISTORY_WAL_SYNC_INTERVAL", "8s")
	t.Setenv("BALANCE_HISTORY_ARCHIVE_CACHE_MAX_BYTES", "256Mi")
	t.Setenv("BALANCE_HISTORY_MAX_SEGMENT_BYTES", "32Mi")
	t.Setenv("BALANCE_HISTORY_MAX_RUNS_PER_TIER_PASS", "2")
	t.Setenv("BALANCE_HISTORY_TIER_INTERVAL", "90s")
	t.Setenv("BALANCE_HISTORY_REMOTE_GC_INTERVAL", "2h")
	t.Setenv("BALANCE_HISTORY_REMOTE_GC_GRACE_PERIOD", "72h")
	t.Setenv("BALANCE_HISTORY_REMOTE_GC_SCAN_LIMIT", "333")
	t.Setenv("BALANCE_HISTORY_REMOTE_GC_DELETE_LIMIT", "11")
	t.Setenv("BALANCE_HISTORY_ENABLED", "true")

	command := NewRunCommand()
	service.BindEnvToCommand(command)
	config := balanceHistoryConfigFromFlags(command)

	require.True(t, config.Enabled)
	require.Equal(t, "/history-volume", config.Dir)
	require.Equal(t, []string{"canary", "shadow"}, config.Ledgers)
	require.Equal(t, 77, config.BuilderBatchSize)
	require.Equal(t, 3*time.Second, config.MaintenanceInterval)
	require.Equal(t, 5, config.MaxCompactionsPerPass)
	require.Equal(t, 8*time.Second, config.DurabilityInterval)
	require.Equal(t, int64(256<<20), config.ArchiveCacheMaxBytes)
	require.Equal(t, int64(32<<20), config.MaxSegmentBytes)
	require.Equal(t, 2, config.MaxRunsPerTierPass)
	require.Equal(t, 90*time.Second, config.TierInterval)
	require.Equal(t, 2*time.Hour, config.RemoteGCInterval)
	require.Equal(t, 72*time.Hour, config.RemoteGCGracePeriod)
	require.Equal(t, 333, config.RemoteGCScanLimit)
	require.Equal(t, 11, config.RemoteGCDeleteLimit)
}
