package controller

import (
	"errors"
	"fmt"
	"strconv"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// validateTraceSampling also protects existing resources admitted before the
// sampler schema was installed. Raw-only SDK configuration remains supported.
func validateTraceSampling(spec *ledgerv1alpha1.ClusterSpec) error {
	if spec.Monitoring == nil || spec.Monitoring.Traces == nil {
		return nil
	}
	traces := spec.Monitoring.Traces
	if traces.Sampler == "" && traces.SamplerArg == "" {
		return nil
	}
	for _, env := range spec.ExtraEnv {
		switch env.Name {
		case "OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG", "TRACE_SAMPLER", "TRACE_SAMPLER_ARG":
			return fmt.Errorf("monitoring.traces sampling cannot be combined with %s in extraEnv", env.Name)
		}
	}
	switch traces.Sampler {
	case "traceidratio", "parentbased_traceidratio":
		ratio, err := strconv.ParseFloat(traces.SamplerArg, 64)
		if err != nil || !(ratio >= 0 && ratio <= 1) {
			return fmt.Errorf("monitoring.traces.samplerArg must be an explicit finite ratio in [0,1] for sampler %q", traces.Sampler)
		}
	case "always_on", "always_off", "parentbased_always_on", "parentbased_always_off":
		if traces.SamplerArg != "" {
			return errors.New("monitoring.traces.samplerArg is only supported for ratio samplers")
		}
	case "":
		return errors.New("monitoring.traces.samplerArg requires monitoring.traces.sampler")
	default:
		return fmt.Errorf("monitoring.traces.sampler %q is not a supported OpenTelemetry sampler", traces.Sampler)
	}

	return nil
}
