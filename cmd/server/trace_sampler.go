package server

import (
	"fmt"
	"math"
	"strconv"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v5/pkg/fx/observefx"
)

const (
	traceSamplerFlag    = "trace-sampler"
	traceSamplerArgFlag = "trace-sampler-arg"
)

func addTraceSamplerFlags(flags *flag.FlagSet) {
	flags.String(traceSamplerFlag, "", "OpenTelemetry SDK sampler (always_on|always_off|traceidratio|parentbased_always_on|parentbased_always_off|parentbased_traceidratio); overrides OTEL_TRACES_SAMPLER")
	flags.String(traceSamplerArgFlag, "", "Sampling probability in [0,1], required for ratio samplers; overrides OTEL_TRACES_SAMPLER_ARG")
}

// tracesModuleFromFlags leaves raw OTEL_TRACES_SAMPLER* parsing to the SDK when
// no typed settings are selected. A selected flag pair replaces the raw pair;
// validation is strict so a typo cannot silently enable exhaustive export.
// The sampler only decides SDK recording/export and never changes Raft apply.
func tracesModuleFromFlags(cmd *cobra.Command) (fx.Option, error) {
	policy, err := cmd.Flags().GetString(traceSamplerFlag)
	if err != nil {
		return nil, err
	}
	arg, err := cmd.Flags().GetString(traceSamplerArgFlag)
	if err != nil {
		return nil, err
	}
	module := observefx.TracesModuleFromFlags(cmd)
	if policy == "" && arg == "" && !cmd.Flags().Changed(traceSamplerFlag) && !cmd.Flags().Changed(traceSamplerArgFlag) {
		return module, nil
	}
	var sampler sdktrace.Sampler
	switch policy {
	case "always_on", "parentbased_always_on":
		sampler = sdktrace.AlwaysSample()
	case "always_off", "parentbased_always_off":
		sampler = sdktrace.NeverSample()
	case "traceidratio", "parentbased_traceidratio":
		if arg == "" {
			return nil, fmt.Errorf("--%s=%s requires --%s", traceSamplerFlag, policy, traceSamplerArgFlag)
		}
		ratio, parseErr := strconv.ParseFloat(arg, 64)
		if parseErr != nil || math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
			return nil, fmt.Errorf("--%s must be a finite number in [0,1]", traceSamplerArgFlag)
		}
		sampler = sdktrace.TraceIDRatioBased(ratio)
	case "":
		return nil, fmt.Errorf("--%s requires --%s", traceSamplerArgFlag, traceSamplerFlag)
	default:
		return nil, fmt.Errorf("unsupported --%s %q", traceSamplerFlag, policy)
	}
	if policy != "traceidratio" && policy != "parentbased_traceidratio" && (arg != "" || cmd.Flags().Changed(traceSamplerArgFlag)) {
		return nil, fmt.Errorf("--%s is only supported for ratio samplers", traceSamplerArgFlag)
	}
	switch policy {
	case "parentbased_always_on", "parentbased_always_off", "parentbased_traceidratio":
		sampler = sdktrace.ParentBased(sampler)
	}

	return fx.Options(module, observefx.ProvideTracerProviderOption(func() sdktrace.TracerProviderOption {
		return sdktrace.WithSampler(sampler)
	})), nil
}
