package utils

import (
	"context"
	"github.com/formancehq/go-libs/v2/collectionutils"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"slices"
	"time"
)

type OtelTracesArgs struct {
	OtelTracesBatch                  pulumix.Input[bool]
	OtelTracesExporterFlag           pulumix.Input[string]
	OtelTracesExporterJaegerEndpoint pulumix.Input[string]
	OtelTracesExporterJaegerUser     pulumix.Input[string]
	OtelTracesExporterJaegerPassword pulumix.Input[string]
	OtelTracesExporterOTLPMode       pulumix.Input[string]
	OtelTracesExporterOTLPEndpoint   pulumix.Input[string]
	OtelTracesExporterOTLPInsecure   pulumix.Input[bool]
}

type OtelMetricsArgs struct {
	OtelMetricsExporterPushInterval               pulumix.Input[*time.Duration]
	OtelMetricsRuntime                            pulumix.Input[bool]
	OtelMetricsRuntimeMinimumReadMemStatsInterval pulumix.Input[*time.Duration]
	OtelMetricsExporter                           pulumix.Input[string]
	OtelMetricsKeepInMemory                       pulumix.Input[bool]
	OtelMetricsExporterOTLPMode                   pulumix.Input[string]
	OtelMetricsExporterOTLPEndpoint               pulumix.Input[string]
	OtelMetricsExporterOTLPInsecure               pulumix.Input[bool]
}

type OtelArgs struct {
	ResourceAttributes pulumix.Input[map[string]string]
	ServiceName        pulumix.Input[string]

	Traces  *OtelTracesArgs
	Metrics *OtelMetricsArgs
}

func (args *OtelArgs) GetEnvVars(ctx context.Context) corev1.EnvVarArray {
	envVars := corev1.EnvVarArray{}
	if args == nil {
		return envVars
	}
	if args.ServiceName != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name:  pulumi.String("OTEL_SERVICE_NAME"),
			Value: args.ServiceName.ToOutput(ctx).Untyped().(pulumi.StringOutput),
		})
	}
	if args.ResourceAttributes != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name: pulumi.String("OTEL_RESOURCE_ATTRIBUTES"),
			Value: pulumix.Apply(args.ResourceAttributes, func(rawResourceAttributes map[string]string) string {
				ret := ""
				keys := collectionutils.Keys(rawResourceAttributes)
				slices.Sort(keys)
				for _, key := range keys {
					ret += key + "=" + rawResourceAttributes[key] + ","
				}
				if len(ret) > 0 {
					ret = ret[:len(ret)-1]
				}
				return ret
			}).Untyped().(pulumi.StringOutput),
		})
	}
	if traces := args.Traces; traces != nil {
		if traces.OtelTracesBatch != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_TRACES_BATCH"),
				Value: BoolToString(traces.OtelTracesBatch).Untyped().(pulumi.StringOutput),
			})
		}
		if traces.OtelTracesExporterFlag != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_TRACES_EXPORTER"),
				Value: traces.OtelTracesExporterFlag.ToOutput(ctx).Untyped().(pulumi.StringOutput),
			})
		}
		if traces.OtelTracesExporterJaegerEndpoint != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_TRACES_EXPORTER_JAEGER_ENDPOINT"),
				Value: traces.OtelTracesExporterJaegerEndpoint.ToOutput(ctx).Untyped().(pulumi.StringOutput),
			})
		}
		if traces.OtelTracesExporterJaegerUser != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_TRACES_EXPORTER_JAEGER_USER"),
				Value: traces.OtelTracesExporterJaegerUser.ToOutput(ctx).Untyped().(pulumi.StringOutput),
			})
		}
		if traces.OtelTracesExporterJaegerPassword != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_TRACES_EXPORTER_JAEGER_PASSWORD"),
				Value: traces.OtelTracesExporterJaegerPassword.ToOutput(ctx).Untyped().(pulumi.StringOutput),
			})
		}
		if traces.OtelTracesExporterOTLPMode != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_TRACES_EXPORTER_OTLP_MODE"),
				Value: traces.OtelTracesExporterOTLPMode.ToOutput(ctx).Untyped().(pulumi.StringOutput),
			})
		}
		if traces.OtelTracesExporterOTLPEndpoint != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_TRACES_EXPORTER_OTLP_ENDPOINT"),
				Value: traces.OtelTracesExporterOTLPEndpoint.ToOutput(ctx).Untyped().(pulumi.StringOutput),
			})
		}
		if traces.OtelTracesExporterOTLPInsecure != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_TRACES_EXPORTER_OTLP_INSECURE"),
				Value: BoolToString(traces.OtelTracesExporterOTLPInsecure).Untyped().(pulumi.StringOutput),
			})
		}
	}

	if metrics := args.Metrics; metrics != nil {
		if metrics.OtelMetricsExporterPushInterval != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name: pulumi.String("OTEL_METRICS_EXPORTER_PUSH_INTERVAL"),
				Value: pulumix.Apply(metrics.OtelMetricsExporterPushInterval, func(pushInterval *time.Duration) string {
					if pushInterval == nil {
						return ""
					}
					return pushInterval.String()
				}).Untyped().(pulumi.StringOutput),
			})
		}
		if metrics.OtelMetricsRuntime != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_METRICS_RUNTIME"),
				Value: BoolToString(metrics.OtelMetricsRuntime).Untyped().(pulumi.StringOutput),
			})
		}
		if metrics.OtelMetricsRuntimeMinimumReadMemStatsInterval != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name: pulumi.String("OTEL_METRICS_RUNTIME_MINIMUM_READ_MEM_STATS_INTERVAL"),
				Value: pulumix.Apply(metrics.OtelMetricsRuntimeMinimumReadMemStatsInterval, func(interval *time.Duration) string {
					if interval == nil {
						return ""
					}
					return interval.String()
				}).Untyped().(pulumi.StringOutput),
			})
		}
		if metrics.OtelMetricsExporter != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_METRICS_EXPORTER"),
				Value: metrics.OtelMetricsExporter.ToOutput(ctx).Untyped().(pulumi.StringOutput),
			})
		}
		if metrics.OtelMetricsKeepInMemory != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_METRICS_KEEP_IN_MEMORY"),
				Value: BoolToString(metrics.OtelMetricsKeepInMemory).Untyped().(pulumi.StringOutput),
			})
		}
		if metrics.OtelMetricsExporterOTLPMode != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_METRICS_EXPORTER_OTLP_MODE"),
				Value: metrics.OtelMetricsExporterOTLPMode.ToOutput(ctx).Untyped().(pulumi.StringOutput),
			})
		}
		if metrics.OtelMetricsExporterOTLPEndpoint != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_METRICS_EXPORTER_OTLP_ENDPOINT"),
				Value: metrics.OtelMetricsExporterOTLPEndpoint.ToOutput(ctx).Untyped().(pulumi.StringOutput),
			})
		}
		if metrics.OtelMetricsExporterOTLPInsecure != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_METRICS_EXPORTER_OTLP_INSECURE"),
				Value: BoolToString(metrics.OtelMetricsExporterOTLPInsecure).Untyped().(pulumi.StringOutput),
			})
		}
	}

	return envVars
}

