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
}
