package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	otlpmetrics "github.com/formancehq/go-libs/v5/pkg/observe/metrics"
	"github.com/formancehq/go-libs/v5/pkg/service"
	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	ledgermetrics "github.com/formancehq/ledger/v3/internal/infra/monitoring/metrics"
)

func TestLoadBloomConfigIncludesLedgerMetadata(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{}
	registerBloomFlags(cmd)

	require.NoError(t, cmd.Flags().Set("bloom-ledger-metadata-expected-keys", "42"))

	cfg := &ledgerpb.ClusterConfig{}
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
	require.Equal(t, 30*time.Second, cfg.HealthConfig.Interval)
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

// TestRunCommand_MetricsPrefixDefault pins the ledger's default namespace on
// the go-libs --otel-metrics-prefix flag, which the metrics module reads to
// prefix the instruments created through the injected MeterProvider.
func TestRunCommand_MetricsPrefixDefault(t *testing.T) {
	t.Parallel()

	cmd := NewRunCommand()
	require.Equal(t, ledgermetrics.DefaultPrefix, otlpmetrics.ConfigFromFlags(cmd.Flags()).Prefix)

	require.NoError(t, cmd.Flags().Set(otlpmetrics.OtelMetricsPrefixFlag, "none"))
	require.Equal(t, "none", otlpmetrics.ConfigFromFlags(cmd.Flags()).Prefix)
}

// TestRunCommandRejectsByteSizeOverflow pins EN-2924 at the CLI boundary:
// invalid explicit input must fail instead of selecting the zero/default sentinel.
func TestRunCommandRejectsByteSizeOverflow(t *testing.T) {
	t.Parallel()
	cmd := NewRunCommand()
	require.NoError(t, cmd.ParseFlags([]string{"--node-id=1", "--raft-max-size-per-msg=2Mi"}))
	require.ErrorContains(t, cmd.ParseFlags([]string{"--raft-max-size-per-msg=16Ei"}), "out of range")
	cfg, err := LoadConfig(context.Background(), cmd)
	require.NoError(t, err)
	require.Equal(t, uint64(2<<20), cfg.RaftConfig.MaxSizePerMsg,
		"failed parsing must preserve the previous flag value")
}

func TestLoadConfigByteSizeDefaults(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "0", "0Ei", "2Mi"} {
		t.Run("value="+value, func(t *testing.T) {
			t.Parallel()
			cmd := NewRunCommand()
			args := []string{"--node-id=1"}
			if value != "" {
				args = append(args, "--raft-max-size-per-msg="+value)
			}
			require.NoError(t, cmd.ParseFlags(args))
			cfg, err := LoadConfig(context.Background(), cmd)
			require.NoError(t, err)
			expected := uint64(0)
			if value == "2Mi" {
				expected = 2 << 20
			}
			require.Equal(t, expected, cfg.RaftConfig.MaxSizePerMsg)
			cfg.RaftConfig.SetDefaults()
			if expected == 0 {
				expected = 1 << 20
			}
			require.Equal(t, expected, cfg.RaftConfig.MaxSizePerMsg)
		})
	}
}

func TestLoadConfig_RejectsUnknownHashAlgorithm(t *testing.T) {
	t.Parallel()

	cmd := NewRunCommand()
	require.NoError(t, cmd.ParseFlags([]string{"--node-id=1", "--cluster-id=hash-test", "--hash-algorithm=xxh33"}))

	cfg, err := LoadConfig(context.Background(), cmd)
	require.EqualError(t, err, `invalid --hash-algorithm "xxh33": must be blake3 or xxh3`)
	require.Nil(t, cfg)
}

func TestLoadConfig_HashAlgorithm(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		args    []string
		want    ledgerpb.HashAlgorithm
		invalid string
	}{
		{name: "default", want: ledgerpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3},
		{name: "empty retains default", args: []string{"--hash-algorithm="}, want: ledgerpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3},
		{name: "blake3", args: []string{"--hash-algorithm=blake3"}, want: ledgerpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3},
		{name: "xxh3", args: []string{"--hash-algorithm=xxh3"}, want: ledgerpb.HashAlgorithm_HASH_ALGORITHM_XXH3},
		{name: "unsupported", args: []string{"--hash-algorithm=sha256"}, invalid: "sha256"},
		{name: "case sensitive", args: []string{"--hash-algorithm=BLAKE3"}, invalid: "BLAKE3"},
		{name: "whitespace", args: []string{"--hash-algorithm= xxh3 "}, invalid: " xxh3 "},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cmd := NewRunCommand()
			require.NoError(t, cmd.ParseFlags(append([]string{"--node-id=1", "--cluster-id=hash-test"}, test.args...)))
			cfg, err := LoadConfig(context.Background(), cmd)
			if test.invalid != "" {
				require.EqualError(t, err, fmt.Sprintf("invalid --hash-algorithm %q: must be blake3 or xxh3", test.invalid))
				require.Nil(t, cfg)

				return
			}
			require.NoError(t, err)
			require.NoError(t, cfg.Validate())
			require.Equal(t, test.want, cfg.BloomConfig.GetHashAlgorithm())
		})
	}
}

func TestLoadConfig_HashAlgorithmEnvironment(t *testing.T) {
	// The production binder reads process environment; these cases must be serial.
	for _, test := range []struct {
		name    string
		env     string
		args    []string
		want    ledgerpb.HashAlgorithm
		invalid bool
	}{
		{name: "empty retains default", want: ledgerpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3},
		{name: "blake3", env: "blake3", want: ledgerpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3},
		{name: "xxh3", env: "xxh3", want: ledgerpb.HashAlgorithm_HASH_ALGORITHM_XXH3},
		{name: "invalid", env: "xxh33", invalid: true},
		{name: "flag overrides invalid environment", env: "xxh33", args: []string{"--hash-algorithm=xxh3"}, want: ledgerpb.HashAlgorithm_HASH_ALGORITHM_XXH3},
		{name: "invalid flag overrides valid environment", env: "blake3", args: []string{"--hash-algorithm=xxh33"}, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("HASH_ALGORITHM", test.env)
			cmd := NewRunCommand()
			require.NoError(t, cmd.ParseFlags(append([]string{"--node-id=1", "--cluster-id=hash-test"}, test.args...)))
			require.NoError(t, service.BindEnvToCommandWithError(cmd))
			cfg, err := LoadConfig(context.Background(), cmd)
			if test.invalid {
				require.EqualError(t, err, `invalid --hash-algorithm "xxh33": must be blake3 or xxh3`)
				require.Nil(t, cfg)

				return
			}
			require.NoError(t, err)
			require.NoError(t, cfg.Validate())
			require.Equal(t, test.want, cfg.BloomConfig.GetHashAlgorithm())
		})
	}
}
