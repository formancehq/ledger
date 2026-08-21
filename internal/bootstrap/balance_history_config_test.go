package bootstrap

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestBalanceHistoryConfigZeroValueUsesProductionDefaults(t *testing.T) {
	t.Parallel()

	defaults := DefaultBalanceHistoryConfig()
	effective := (BalanceHistoryConfig{}).Effective()

	require.Equal(t, defaults, effective)
	require.Positive(t, effective.BuilderBatchSize)
	require.GreaterOrEqual(t, effective.SegmentCompactionThreshold, 2)
	require.Equal(t, time.Second, effective.MaintenanceInterval)
	require.Equal(t, 2, effective.MaxCompactionsPerPass)
	require.Positive(t, effective.BackfillYield)
	require.Positive(t, effective.DurabilityInterval)
	require.NoError(t, (BalanceHistoryConfig{}).Validate())
}

func TestBalanceHistoryStoreDir(t *testing.T) {
	t.Parallel()

	require.Equal(t, "/data/app/balance-history", balanceHistoryStoreDir(Config{DataDir: "/data/app"}))
	require.Equal(t, "/dedicated/history", balanceHistoryStoreDir(Config{
		DataDir:              "/data/app",
		BalanceHistoryConfig: BalanceHistoryConfig{Dir: "/dedicated/history"},
	}))
}

func TestBalanceHistoryConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*BalanceHistoryConfig)
		wantErr string
	}{
		{name: "negative batch size", mutate: func(c *BalanceHistoryConfig) { c.BuilderBatchSize = -1 }, wantErr: "--balance-history-builder-batch-size"},
		{name: "single-segment compaction threshold", mutate: func(c *BalanceHistoryConfig) { c.SegmentCompactionThreshold = 1 }, wantErr: "--balance-history-segment-compaction-threshold"},
		{name: "negative maintenance interval", mutate: func(c *BalanceHistoryConfig) { c.MaintenanceInterval = -time.Second }, wantErr: "--balance-history-maintenance-interval"},
		{name: "negative max compactions", mutate: func(c *BalanceHistoryConfig) { c.MaxCompactionsPerPass = -1 }, wantErr: "--balance-history-max-compactions-per-pass"},
		{name: "excessive max compactions", mutate: func(c *BalanceHistoryConfig) { c.MaxCompactionsPerPass = MaxBalanceHistoryCompactionsPerPass + 1 }, wantErr: "--balance-history-max-compactions-per-pass"},
		{name: "negative backfill yield", mutate: func(c *BalanceHistoryConfig) { c.BackfillYield = -time.Millisecond }, wantErr: "--balance-history-backfill-yield"},
		{name: "negative durability interval", mutate: func(c *BalanceHistoryConfig) { c.DurabilityInterval = -time.Second }, wantErr: "--balance-history-wal-sync-interval"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			config := DefaultBalanceHistoryConfig()
			tt.mutate(&config)
			require.ErrorContains(t, config.Validate(), tt.wantErr)
		})
	}
}

func TestBalanceHistoryConfigAcceptsEventDrivenMaintenance(t *testing.T) {
	t.Parallel()

	config := DefaultBalanceHistoryConfig()
	config.SegmentCompactionThreshold = 2
	config.MaxCompactionsPerPass = 1
	config.MaintenanceInterval = time.Hour
	require.NoError(t, config.Validate())
}

func TestConfigDisplayUsesCamelCaseBalanceHistoryAndKeepsRedaction(t *testing.T) {
	t.Parallel()

	config := Config{ClusterSecret: "must-not-leak", BalanceHistoryConfig: DefaultBalanceHistoryConfig()}

	encodedJSON, err := json.Marshal(config)
	require.NoError(t, err)
	require.Contains(t, string(encodedJSON), `"balanceHistory"`)
	require.Contains(t, string(encodedJSON), `"builderBatchSize"`)
	require.Contains(t, string(encodedJSON), `"segmentCompactionThreshold"`)
	require.NotContains(t, string(encodedJSON), "must-not-leak")
	require.Contains(t, string(encodedJSON), RedactedSecretPlaceholder)

	encodedYAML, err := yaml.Marshal(config)
	require.NoError(t, err)
	require.Contains(t, string(encodedYAML), "balanceHistory:")
	require.Contains(t, string(encodedYAML), "builderBatchSize:")
	require.Contains(t, string(encodedYAML), "segmentCompactionThreshold:")
	require.NotContains(t, string(encodedYAML), "must-not-leak")
	require.Contains(t, string(encodedYAML), RedactedSecretPlaceholder)
}
