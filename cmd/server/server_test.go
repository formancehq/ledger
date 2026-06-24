package server

import (
	"context"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestLoadBloomConfigIncludesLedgerMetadata(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{}
	registerBloomFlags(cmd)

	require.NoError(t, cmd.Flags().Set("bloom-ledger-metadata-expected-keys", "42"))

	cfg := &commonpb.ClusterConfig{}
	loadBloomConfig(cmd, cfg)

	require.Equal(t, uint64(42), cfg.GetBloomLedgerMetadata().GetExpectedKeys())
	require.Equal(t, 0.01, cfg.GetBloomLedgerMetadata().GetFpRate())
}

// TestLoadConfig_ZeroPreservedForSentinelFlags pins the fix for #324.
// Several flags document `0` as a meaningful sentinel (disable /
// never expire). The pre-fix helpers swallowed any value-zero through
// a `val != 0` shortcut and silently substituted the cobra default,
// so passing --idempotency-ttl=0 (meant: never expire) yielded
// IdempotencyTTL=24h and the eviction scheduler deleted keys the
// operator wanted kept — duplicate transactions on client retries.
func TestLoadConfig_ZeroPreservedForSentinelFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		flag  string
		value string
		check func(t *testing.T, cmd *cobra.Command)
	}{
		{
			name:  "idempotency-ttl=0 means never expire",
			flag:  "idempotency-ttl",
			value: "0s",
			check: func(t *testing.T, cmd *cobra.Command) {
				cfg, err := LoadConfig(context.Background(), cmd)
				require.NoError(t, err)
				require.Equal(t, time.Duration(0), cfg.IdempotencyTTL,
					"--idempotency-ttl=0 must propagate as 0 (never expire), not the 24h default")
			},
		},
		{
			name:  "learner-promotion-threshold=0 disables auto-promotion",
			flag:  "learner-promotion-threshold",
			value: "0",
			check: func(t *testing.T, cmd *cobra.Command) {
				cfg, err := LoadConfig(context.Background(), cmd)
				require.NoError(t, err)
				require.Equal(t, uint64(0), cfg.RaftConfig.AutoPromoteThreshold,
					"--learner-promotion-threshold=0 must propagate as 0 (disabled), not the 100 default")
			},
		},
		{
			name:  "health-clock-skew-threshold=0 disables the check",
			flag:  "health-clock-skew-threshold",
			value: "0s",
			check: func(t *testing.T, cmd *cobra.Command) {
				cfg, err := LoadConfig(context.Background(), cmd)
				require.NoError(t, err)
				require.Equal(t, time.Duration(0), cfg.HealthConfig.ClockSkewThreshold,
					"--health-clock-skew-threshold=0 must propagate as 0 (disabled), not 500ms")
			},
		},
		{
			name:  "query-profile-threshold=0 disables profiling",
			flag:  "query-profile-threshold",
			value: "0s",
			check: func(t *testing.T, cmd *cobra.Command) {
				cfg, err := LoadConfig(context.Background(), cmd)
				require.NoError(t, err)
				require.Equal(t, time.Duration(0), cfg.QueryProfileThreshold,
					"--query-profile-threshold=0 must propagate as 0 (disabled), not 10ms")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmd := NewRunCommand()
			// node-id is required by Config.Validate; populate enough that
			// LoadConfig itself returns a usable Config for the fields we
			// assert on.
			require.NoError(t, cmd.Flags().Set("node-id", "1"))
			require.NoError(t, cmd.Flags().Set(tt.flag, tt.value))

			tt.check(t, cmd)
		})
	}
}

// TestLoadConfig_DefaultsApplyWhenFlagUnset confirms the existing
// behavior for the "user did not pass --foo" path is unchanged: cobra
// flag defaults still flow through.
func TestLoadConfig_DefaultsApplyWhenFlagUnset(t *testing.T) {
	t.Parallel()

	cmd := NewRunCommand()
	require.NoError(t, cmd.Flags().Set("node-id", "1"))

	cfg, err := LoadConfig(context.Background(), cmd)
	require.NoError(t, err)
	require.Equal(t, 24*time.Hour, cfg.IdempotencyTTL,
		"--idempotency-ttl unset must yield the 24h default")
	require.Equal(t, uint64(100), cfg.RaftConfig.AutoPromoteThreshold)
	require.Equal(t, 500*time.Millisecond, cfg.HealthConfig.ClockSkewThreshold)
	require.Equal(t, 10*time.Millisecond, cfg.QueryProfileThreshold)
	// Regression: --cache-rotation-threshold (cobra default 1000) used to
	// regress to 0 when the call site's local fallback (0) was preferred
	// to cobra's registered default. Zero rotation threshold triggers a
	// divide-by-zero in cache/generation.go via Builder.Build.
	require.Equal(t, uint64(1000), cfg.RaftConfig.RotationThreshold,
		"--cache-rotation-threshold unset must yield the 1000 cobra default, not 0")
	// Regression: same divergence pattern on raft tick flags — cobra
	// registers 10 / 1 / 100ms, the call sites passed 0 as fallback.
	require.Equal(t, 10, cfg.RaftConfig.ElectionTick)
	require.Equal(t, 1, cfg.RaftConfig.HeartbeatTick)
	require.Equal(t, 100*time.Millisecond, cfg.RaftConfig.TickInterval)
	// Resume thresholds, unset, derive from the block defaults (0.8 * 0.9375),
	// reproducing the shipped 0.75 default exactly.
	require.InDelta(t, 0.75, cfg.HealthConfig.WALResumeThreshold, 1e-9)
	require.InDelta(t, 0.75, cfg.HealthConfig.DataResumeThreshold, 1e-9)
}

// TestLoadConfig_ResumeThresholdDerivedFromBlock pins the upgrade-safety fix:
// when an operator lowered a block threshold below the old fixed 0.75 resume
// default and did not set the new resume flag, a static 0.75 default would make
// resume >= block and Config.Validate reject startup. The resume default now
// tracks the block threshold so the gap stays valid for any block.
func TestLoadConfig_ResumeThresholdDerivedFromBlock(t *testing.T) {
	t.Parallel()

	cmd := NewRunCommand()
	require.NoError(t, cmd.Flags().Set("node-id", "1"))
	// Lower the block thresholds below the old fixed 0.75 resume default.
	require.NoError(t, cmd.Flags().Set("health-wal-threshold", "0.7"))
	require.NoError(t, cmd.Flags().Set("health-data-threshold", "0.6"))

	cfg, err := LoadConfig(context.Background(), cmd)
	require.NoError(t, err)

	require.InDelta(t, 0.7*0.9375, cfg.HealthConfig.WALResumeThreshold, 1e-9)
	require.InDelta(t, 0.6*0.9375, cfg.HealthConfig.DataResumeThreshold, 1e-9)
	// The derived resume stays strictly between 0 and its block mark, so the
	// pair Config.Validate enforces (0 < resume < block) holds.
	require.Greater(t, cfg.HealthConfig.WALResumeThreshold, 0.0)
	require.Less(t, cfg.HealthConfig.WALResumeThreshold, cfg.HealthConfig.WALThreshold)
	require.Greater(t, cfg.HealthConfig.DataResumeThreshold, 0.0)
	require.Less(t, cfg.HealthConfig.DataResumeThreshold, cfg.HealthConfig.DataThreshold)
}

// TestLoadConfig_ResumeThresholdExplicitHonored confirms an explicitly-set
// resume flag is used verbatim instead of the derived value.
func TestLoadConfig_ResumeThresholdExplicitHonored(t *testing.T) {
	t.Parallel()

	cmd := NewRunCommand()
	require.NoError(t, cmd.Flags().Set("node-id", "1"))
	require.NoError(t, cmd.Flags().Set("health-wal-resume-threshold", "0.5"))

	cfg, err := LoadConfig(context.Background(), cmd)
	require.NoError(t, err)

	require.InDelta(t, 0.5, cfg.HealthConfig.WALResumeThreshold, 1e-9)
	// Data resume, left unset, still derives from its block default.
	require.InDelta(t, 0.75, cfg.HealthConfig.DataResumeThreshold, 1e-9)
}
