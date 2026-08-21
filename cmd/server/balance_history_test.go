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
	require.NoError(t, command.Flags().Set(balanceHistoryBuilderBatchSizeFlag, "321"))
	require.NoError(t, command.Flags().Set(balanceHistoryCompactionThresholdFlag, "7"))
	require.NoError(t, command.Flags().Set(balanceHistoryMaintenanceIntervalFlag, "2s"))
	require.NoError(t, command.Flags().Set(balanceHistoryMaxCompactionsFlag, "6"))
	require.NoError(t, command.Flags().Set(balanceHistoryBackfillYieldFlag, "12ms"))
	require.NoError(t, command.Flags().Set(balanceHistoryWALSyncIntervalFlag, "3s"))

	config, err := LoadConfig(context.Background(), command)
	require.NoError(t, err)
	require.Equal(t, bootstrap.BalanceHistoryConfig{
		Dir:                        "/dedicated/history",
		BuilderBatchSize:           321,
		SegmentCompactionThreshold: 7,
		MaintenanceInterval:        2 * time.Second,
		MaxCompactionsPerPass:      6,
		BackfillYield:              12 * time.Millisecond,
		DurabilityInterval:         3 * time.Second,
	}, config.BalanceHistoryConfig)
	require.NoError(t, config.BalanceHistoryConfig.Validate())
}

func TestLoadBalanceHistoryConfigFromEnvironment(t *testing.T) {
	t.Setenv("BALANCE_HISTORY_DIR", "/history-volume")
	t.Setenv("BALANCE_HISTORY_BUILDER_BATCH_SIZE", "77")
	t.Setenv("BALANCE_HISTORY_SEGMENT_COMPACTION_THRESHOLD", "9")
	t.Setenv("BALANCE_HISTORY_MAINTENANCE_INTERVAL", "3s")
	t.Setenv("BALANCE_HISTORY_MAX_COMPACTIONS_PER_PASS", "5")
	t.Setenv("BALANCE_HISTORY_BACKFILL_YIELD", "4ms")
	t.Setenv("BALANCE_HISTORY_WAL_SYNC_INTERVAL", "8s")

	command := NewRunCommand()
	service.BindEnvToCommand(command)
	config := balanceHistoryConfigFromFlags(command)

	require.Equal(t, "/history-volume", config.Dir)
	require.Equal(t, 77, config.BuilderBatchSize)
	require.Equal(t, 9, config.SegmentCompactionThreshold)
	require.Equal(t, 3*time.Second, config.MaintenanceInterval)
	require.Equal(t, 5, config.MaxCompactionsPerPass)
	require.Equal(t, 4*time.Millisecond, config.BackfillYield)
	require.Equal(t, 8*time.Second, config.DurabilityInterval)
}
