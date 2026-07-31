package bootstrap

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	appbalancehistory "github.com/formancehq/ledger/v3/internal/application/balancehistory"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
)

func TestBalanceHistoryConfigZeroValueUsesProductionDefaults(t *testing.T) {
	t.Parallel()

	defaults := DefaultBalanceHistoryConfig()
	effective := (BalanceHistoryConfig{}).Effective()

	require.Equal(t, defaults, effective)
	require.False(t, effective.Enabled)
	require.False(t, effective.ColdTierEnabled)
	require.Positive(t, effective.BuilderBatchSize)
	require.GreaterOrEqual(t, effective.RunCompactionThreshold, 2)
	require.Equal(t, time.Second, effective.MaintenanceInterval)
	require.Equal(t, 2, effective.MaxCompactionsPerPass)
	require.Positive(t, effective.BackfillYield)
	require.Positive(t, effective.DurabilityInterval)
	require.Positive(t, effective.VerifierInterval)
	require.Positive(t, effective.VerifierReplayEvery)
	require.Positive(t, effective.RetainLocalRuns)
	require.Positive(t, effective.ArchiveCacheMaxBytes)
	require.Positive(t, effective.MaxSegmentBytes)
	require.Positive(t, effective.MaxRunsPerTierPass)
	require.Positive(t, effective.TierInterval)
	require.Positive(t, effective.RemoteGCInterval)
	require.Positive(t, effective.RemoteGCGracePeriod)
	require.Positive(t, effective.RemoteGCScanLimit)
	require.Positive(t, effective.RemoteGCDeleteLimit)
	require.NoError(t, (BalanceHistoryConfig{}).Validate(coldstorage.Config{}))
}

func TestBalanceHistoryStoreDir(t *testing.T) {
	t.Parallel()

	require.Equal(t, "/data/app/balance-history", balanceHistoryStoreDir(Config{
		DataDir: "/data/app",
	}))
	require.Equal(t, "/dedicated/history", balanceHistoryStoreDir(Config{
		DataDir: "/data/app",
		BalanceHistoryConfig: BalanceHistoryConfig{
			Dir: "/dedicated/history",
		},
	}))
}

func TestBalanceHistoryConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*BalanceHistoryConfig)
		cold    coldstorage.Config
		wantErr string
	}{
		{
			name: "empty ledger allowlist entry",
			mutate: func(config *BalanceHistoryConfig) {
				config.Ledgers = []string{"canary", ""}
			},
			wantErr: "entry 2 must not be empty",
		},
		{
			name: "duplicate ledger allowlist entry",
			mutate: func(config *BalanceHistoryConfig) {
				config.Ledgers = []string{"canary", "canary"}
			},
			wantErr: `duplicate ledger "canary"`,
		},
		{
			name: "negative batch size",
			mutate: func(config *BalanceHistoryConfig) {
				config.BuilderBatchSize = -1
			},
			wantErr: "--balance-history-builder-batch-size",
		},
		{
			name: "single-run compaction threshold",
			mutate: func(config *BalanceHistoryConfig) {
				config.RunCompactionThreshold = 1
			},
			wantErr: "--balance-history-compaction-threshold",
		},
		{
			name: "negative maintenance interval",
			mutate: func(config *BalanceHistoryConfig) {
				config.MaintenanceInterval = -time.Second
			},
			wantErr: "--balance-history-maintenance-interval",
		},
		{
			name: "negative max compactions per pass",
			mutate: func(config *BalanceHistoryConfig) {
				config.MaxCompactionsPerPass = -1
			},
			wantErr: "--balance-history-max-compactions-per-pass",
		},
		{
			name: "absurd max compactions per pass",
			mutate: func(config *BalanceHistoryConfig) {
				config.MaxCompactionsPerPass = MaxBalanceHistoryCompactionsPerPass + 1
			},
			wantErr: "--balance-history-max-compactions-per-pass",
		},
		{
			name: "maintenance retirement rate below builder publication ceiling",
			mutate: func(config *BalanceHistoryConfig) {
				config.RunCompactionThreshold = 2
				config.MaxCompactionsPerPass = 1
				config.MaintenanceInterval = time.Second
			},
			wantErr: "balance history maintenance is unstable",
		},
		{
			name: "negative backfill yield",
			mutate: func(config *BalanceHistoryConfig) {
				config.BackfillYield = -time.Millisecond
			},
			wantErr: "--balance-history-backfill-yield",
		},
		{
			name: "negative durability interval",
			mutate: func(config *BalanceHistoryConfig) {
				config.DurabilityInterval = -time.Second
			},
			wantErr: "--balance-history-wal-sync-interval",
		},
		{
			name: "negative verifier interval",
			mutate: func(config *BalanceHistoryConfig) {
				config.VerifierInterval = -time.Second
			},
			wantErr: "--balance-history-verifier-interval",
		},
		{
			name: "negative local retention",
			mutate: func(config *BalanceHistoryConfig) {
				config.RetainLocalRuns = -1
			},
			wantErr: "--balance-history-retain-local-runs",
		},
		{
			name: "negative archive cache",
			mutate: func(config *BalanceHistoryConfig) {
				config.ArchiveCacheMaxBytes = -1
			},
			wantErr: "--balance-history-archive-cache-max-bytes",
		},
		{
			name: "cold segment below archive overhead",
			mutate: func(config *BalanceHistoryConfig) {
				config.MaxSegmentBytes = 1
			},
			wantErr: "--balance-history-max-segment-bytes",
		},
		{
			name: "negative max runs per tier pass",
			mutate: func(config *BalanceHistoryConfig) {
				config.MaxRunsPerTierPass = -1
			},
			wantErr: "--balance-history-max-runs-per-tier-pass",
		},
		{
			name: "absurd max runs per tier pass",
			mutate: func(config *BalanceHistoryConfig) {
				config.MaxRunsPerTierPass = MaxBalanceHistoryRunsPerTierPass + 1
			},
			wantErr: "--balance-history-max-runs-per-tier-pass",
		},
		{
			name: "negative tier interval",
			mutate: func(config *BalanceHistoryConfig) {
				config.TierInterval = -time.Second
			},
			wantErr: "--balance-history-tier-interval",
		},
		{
			name: "negative remote GC interval",
			mutate: func(config *BalanceHistoryConfig) {
				config.RemoteGCInterval = -time.Second
			},
			wantErr: "--balance-history-remote-gc-interval",
		},
		{
			name: "negative remote GC grace period",
			mutate: func(config *BalanceHistoryConfig) {
				config.RemoteGCGracePeriod = -time.Second
			},
			wantErr: "--balance-history-remote-gc-grace-period",
		},
		{
			name: "negative remote GC scan limit",
			mutate: func(config *BalanceHistoryConfig) {
				config.RemoteGCScanLimit = -1
			},
			wantErr: "--balance-history-remote-gc-scan-limit",
		},
		{
			name: "absurd remote GC scan limit",
			mutate: func(config *BalanceHistoryConfig) {
				config.RemoteGCScanLimit = MaxBalanceHistoryRemoteGCScanLimit + 1
			},
			wantErr: "--balance-history-remote-gc-scan-limit",
		},
		{
			name: "negative remote GC delete limit",
			mutate: func(config *BalanceHistoryConfig) {
				config.RemoteGCDeleteLimit = -1
			},
			wantErr: "--balance-history-remote-gc-delete-limit",
		},
		{
			name: "absurd remote GC delete limit",
			mutate: func(config *BalanceHistoryConfig) {
				config.RemoteGCDeleteLimit = MaxBalanceHistoryRemoteGCDeleteLimit + 1
			},
			wantErr: "--balance-history-remote-gc-delete-limit",
		},
		{
			name: "cold tier while projection disabled",
			mutate: func(config *BalanceHistoryConfig) {
				config.Enabled = false
				config.ColdTierEnabled = true
			},
			cold:    coldstorage.Config{Driver: "filesystem"},
			wantErr: "requires --balance-history-enabled",
		},
		{
			name: "cold tier without cold storage",
			mutate: func(config *BalanceHistoryConfig) {
				config.Enabled = true
				config.ColdTierEnabled = true
			},
			cold:    coldstorage.Config{Driver: "none"},
			wantErr: "requires --cold-storage-driver=filesystem or s3",
		},
		{
			name: "cold tier with filesystem storage",
			mutate: func(config *BalanceHistoryConfig) {
				config.Enabled = true
				config.ColdTierEnabled = true
			},
			cold: coldstorage.Config{Driver: "filesystem"},
		},
		{
			name: "cold tier with s3 storage",
			mutate: func(config *BalanceHistoryConfig) {
				config.Enabled = true
				config.ColdTierEnabled = true
			},
			cold: coldstorage.Config{Driver: "s3"},
		},
		{
			name: "cold tier with unknown storage",
			mutate: func(config *BalanceHistoryConfig) {
				config.Enabled = true
				config.ColdTierEnabled = true
			},
			cold:    coldstorage.Config{Driver: "custom"},
			wantErr: "requires --cold-storage-driver=filesystem or s3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := DefaultBalanceHistoryConfig()
			tt.mutate(&config)
			err := config.Validate(tt.cold)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestBalanceHistoryConfigAcceptsMaintenanceRateAtExactBuilderCeiling(t *testing.T) {
	t.Parallel()

	config := DefaultBalanceHistoryConfig()
	config.RunCompactionThreshold = 2
	config.MaxCompactionsPerPass = 1
	config.MaintenanceInterval = appbalancehistory.TickInterval
	require.NoError(t, config.Validate(coldstorage.Config{}))
}

func TestBalanceHistoryConfigLedgerAllowlistIsExactAndCaseSensitive(t *testing.T) {
	t.Parallel()

	config := DefaultBalanceHistoryConfig()
	config.Ledgers = []string{"canary", "Canary"}
	require.NoError(t, config.Validate(coldstorage.Config{}))
}

func TestConfigDisplayUsesCamelCaseBalanceHistoryAndKeepsRedaction(t *testing.T) {
	t.Parallel()

	config := Config{
		ClusterSecret:        "must-not-leak",
		BalanceHistoryConfig: DefaultBalanceHistoryConfig(),
	}

	encodedJSON, err := json.Marshal(config)
	require.NoError(t, err)
	require.Contains(t, string(encodedJSON), `"balanceHistory"`)
	require.Contains(t, string(encodedJSON), `"builderBatchSize"`)
	require.Contains(t, string(encodedJSON), `"maintenanceInterval"`)
	require.Contains(t, string(encodedJSON), `"maxCompactionsPerPass"`)
	require.Contains(t, string(encodedJSON), `"remoteGcInterval"`)
	require.NotContains(t, string(encodedJSON), "must-not-leak")
	require.Contains(t, string(encodedJSON), RedactedSecretPlaceholder)

	encodedYAML, err := yaml.Marshal(config)
	require.NoError(t, err)
	require.Contains(t, string(encodedYAML), "balanceHistory:")
	require.Contains(t, string(encodedYAML), "builderBatchSize:")
	require.Contains(t, string(encodedYAML), "maintenanceInterval:")
	require.Contains(t, string(encodedYAML), "maxCompactionsPerPass:")
	require.Contains(t, string(encodedYAML), "remoteGcInterval:")
	require.NotContains(t, string(encodedYAML), "must-not-leak")
	require.Contains(t, string(encodedYAML), RedactedSecretPlaceholder)
}
