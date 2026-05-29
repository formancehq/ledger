package server

import (
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/tracesampling"
)

const (
	// TraceSamplingEnabledFlag enables error-aware trace sampling.
	TraceSamplingEnabledFlag = "trace-sampling-enabled"

	// TraceSamplingSuccessRatioFlag sets the sampling ratio for successful spans.
	TraceSamplingSuccessRatioFlag = "trace-sampling-success-ratio"
)

// addTraceSamplingFlags adds trace sampling flags to the given flag set.
func addTraceSamplingFlags(flags *flag.FlagSet) {
	flags.Bool(TraceSamplingEnabledFlag, false,
		"Enable error-aware trace sampling (always sample errors, ratio-sample successes)")
	flags.Float64(TraceSamplingSuccessRatioFlag, 0.1,
		"Sampling ratio for successful spans (0.0-1.0). Error spans are always sampled.")
}

// traceSamplingConfigFromFlags creates a tracesampling.Config from command flags.
func traceSamplingConfigFromFlags(cmd *cobra.Command) tracesampling.Config {
	cfg := tracesampling.DefaultConfig()

	cfg.Enabled, _ = cmd.Flags().GetBool(TraceSamplingEnabledFlag)
	cfg.SuccessRatio, _ = cmd.Flags().GetFloat64(TraceSamplingSuccessRatioFlag)

	return cfg
}
