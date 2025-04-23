package monitoring

import (
	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"slices"
	"time"
)

type JaegerExporterArgs struct {
	Endpoint pulumix.Input[string]
	User     pulumix.Input[string]
	Password pulumix.Input[string]
}

type EndpointArgs struct {
	Endpoint pulumix.Input[string]
	Insecure pulumix.Input[bool]
	Mode     pulumix.Input[string]
}

type TracesArgs struct {
	Batch    pulumix.Input[bool]
	Exporter pulumix.Input[string]

	Jaeger *JaegerExporterArgs
	OTLP   *EndpointArgs
}

type MetricsArgs struct {
	PushInterval                pulumix.Input[*time.Duration]
	Runtime                     pulumix.Input[bool]
	MinimumReadMemStatsInterval pulumix.Input[*time.Duration]
	Exporter                    pulumix.Input[string]
	KeepInMemory                pulumix.Input[bool]

	OTLP *EndpointArgs
}

type Args struct {
	ResourceAttributes pulumix.Input[map[string]string]
	ServiceName        pulumix.Input[string]

	Traces  *TracesArgs
	Metrics *MetricsArgs
}

func (args *Args) SetDefaults() {
	if args.ServiceName == nil {
		args.ServiceName = pulumi.String("")
	}
	args.ServiceName = pulumix.Apply(args.ServiceName, func(serviceName string) string {
		if serviceName == "" {
			return "ledger"
		}
		return serviceName
	})
}

func (args *Args) GetEnvVars(ctx *pulumi.Context) corev1.EnvVarArray {
	envVars := corev1.EnvVarArray{}
	if args == nil {
		return envVars
	}
	if args.ServiceName != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name:  pulumi.String("OTEL_SERVICE_NAME"),
			Value: args.ServiceName.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		})
	}
	if args.ResourceAttributes == nil {
		args.ResourceAttributes = pulumix.Val(map[string]string{})
	}
	envVars = append(envVars, corev1.EnvVarArgs{
		Name: pulumi.String("OTEL_RESOURCE_ATTRIBUTES"),
		Value: pulumix.Apply(args.ResourceAttributes, func(rawResourceAttributes map[string]string) string {

			if rawResourceAttributes == nil {
				rawResourceAttributes = map[string]string{}
			}

			rawResourceAttributes["com.formance.stack/pulumi-stack"] = ctx.Stack()
			rawResourceAttributes["com.formance.stack/pulumi-project"] = ctx.Project()
			rawResourceAttributes["com.formance.stack/pulumi-organization"] = ctx.Organization()

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
	if traces := args.Traces; traces != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name:  pulumi.String("OTEL_TRACES"),
			Value: pulumi.String("true"),
		})
		if traces.Batch != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_TRACES_BATCH"),
				Value: utils.BoolToString(traces.Batch).Untyped().(pulumi.StringOutput),
			})
		}
		if traces.Exporter != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_TRACES_EXPORTER"),
				Value: traces.Exporter.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			})
		}
		if traces.Jaeger != nil {
			if traces.Jaeger.Endpoint != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER_JAEGER_ENDPOINT"),
					Value: traces.Jaeger.Endpoint.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if traces.Jaeger.User != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER_JAEGER_USER"),
					Value: traces.Jaeger.User.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if traces.Jaeger.Password != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER_JAEGER_PASSWORD"),
					Value: traces.Jaeger.Password.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
		}

		if traces.OTLP != nil {
			if traces.OTLP.Endpoint != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER_OTLP_ENDPOINT"),
					Value: traces.OTLP.Endpoint.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if traces.OTLP.Insecure != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER_OTLP_INSECURE"),
					Value: utils.BoolToString(traces.OTLP.Insecure).Untyped().(pulumi.StringOutput),
				})
			}
			if traces.OTLP.Mode != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_TRACES_EXPORTER_OTLP_MODE"),
					Value: traces.OTLP.Mode.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
		}
	}

	if metrics := args.Metrics; metrics != nil {
		envVars = append(envVars, corev1.EnvVarArgs{
			Name:  pulumi.String("OTEL_METRICS"),
			Value: pulumi.String("true"),
		})
		if metrics.PushInterval != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name: pulumi.String("OTEL_METRICS_EXPORTER_PUSH_INTERVAL"),
				Value: pulumix.Apply(metrics.PushInterval, func(pushInterval *time.Duration) string {
					if pushInterval == nil {
						return ""
					}
					return pushInterval.String()
				}).Untyped().(pulumi.StringOutput),
			})
		}
		if metrics.Runtime != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_METRICS_RUNTIME"),
				Value: utils.BoolToString(metrics.Runtime).Untyped().(pulumi.StringOutput),
			})
		}
		if metrics.MinimumReadMemStatsInterval != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name: pulumi.String("OTEL_METRICS_RUNTIME_MINIMUM_READ_MEM_STATS_INTERVAL"),
				Value: pulumix.Apply(metrics.MinimumReadMemStatsInterval, func(interval *time.Duration) string {
					if interval == nil {
						return ""
					}
					return interval.String()
				}).Untyped().(pulumi.StringOutput),
			})
		}
		if metrics.Exporter != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_METRICS_EXPORTER"),
				Value: metrics.Exporter.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			})
		}
		if metrics.KeepInMemory != nil {
			envVars = append(envVars, corev1.EnvVarArgs{
				Name:  pulumi.String("OTEL_METRICS_KEEP_IN_MEMORY"),
				Value: utils.BoolToString(metrics.KeepInMemory).Untyped().(pulumi.StringOutput),
			})
		}

		if metrics.OTLP != nil {
			if metrics.OTLP.Endpoint != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_METRICS_EXPORTER_OTLP_ENDPOINT"),
					Value: metrics.OTLP.Endpoint.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
			if metrics.OTLP.Insecure != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_METRICS_EXPORTER_OTLP_INSECURE"),
					Value: utils.BoolToString(metrics.OTLP.Insecure).Untyped().(pulumi.StringOutput),
				})
			}
			if metrics.OTLP.Mode != nil {
				envVars = append(envVars, corev1.EnvVarArgs{
					Name:  pulumi.String("OTEL_METRICS_EXPORTER_OTLP_MODE"),
					Value: metrics.OTLP.Mode.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
				})
			}
		}
	}

	return envVars
}
