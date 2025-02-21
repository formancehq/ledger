package main

import (
	"github.com/formancehq/go-libs/v2/pointer"
	pulumi_ledger "github.com/formancehq/ledger/deployments/pulumi/pkg"
	pulumi_dataset_init_stack "github.com/formancehq/ledger/tools/k8s-stack/pkg"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"time"
)

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {

		args := &pulumi_dataset_init_stack.StackComponentArgs{
			Debug:     pulumix.Val(config.GetBool(ctx, "debug")),
			Namespace: pulumi.String(config.Get(ctx, "namespace")),
			Ledger: &pulumi_dataset_init_stack.StackLedgerArgs{
				Version:     pulumix.Val(config.Get(ctx, "ledger-version")),
				GracePeriod: pulumix.Val(config.Get(ctx, "ledger-grace-period")),
				Upgrade:     pulumix.Val(pulumi_ledger.UpgradeMode(config.Get(ctx, "ledger-upgrade-mode"))),
			},
		}

		tracesEnabled := config.GetBool(ctx, "ledger-otel-traces-enabled")
		metricsEnabled := config.GetBool(ctx, "ledger-otel-metrics-enabled")
		if tracesEnabled || metricsEnabled {
			args.Ledger.Otel = &pulumi_ledger.OtelArgs{
				ResourceAttributes: pulumix.Val(map[string]string{
					"deployment.environment": "test-volumes",
				}),
			}
		}

		if tracesEnabled {
			args.Ledger.Otel.Traces = &pulumi_ledger.OtelTracesArgs{
				OtelTracesBatch:                  pulumix.Val(config.GetBool(ctx, "ledger-otel-traces-batch")),
				OtelTracesExporterFlag:           pulumix.Val(config.Get(ctx, "ledger-otel-traces-exporter")),
				OtelTracesExporterJaegerEndpoint: pulumix.Val(config.Get(ctx, "ledger-otel-traces-exporter-jaeger-endpoint")),
				OtelTracesExporterJaegerUser:     pulumix.Val(config.Get(ctx, "ledger-otel-traces-exporter-jaeger-user")),
				OtelTracesExporterJaegerPassword: pulumix.Val(config.Get(ctx, "ledger-otel-traces-exporter-jaeger-password")),
				OtelTracesExporterOTLPMode:       pulumix.Val(config.Get(ctx, "ledger-otel-traces-exporter-otlp-mode")),
				OtelTracesExporterOTLPEndpoint:   pulumix.Val(config.Get(ctx, "ledger-otel-traces-exporter-otlp-endpoint")),
				OtelTracesExporterOTLPInsecure:   pulumix.Val(config.GetBool(ctx, "ledger-otel-traces-exporter-otlp-insecure")),
			}
		}

		if metricsEnabled {
			args.Ledger.Otel.Metrics = &pulumi_ledger.OtelMetricsArgs{
				OtelMetricsExporterPushInterval:               pulumix.Val(pointer.For(getDuration(ctx, "ledger-otel-metrics-exporter-push-interval"))),
				OtelMetricsRuntime:                            pulumix.Val(config.GetBool(ctx, "ledger-otel-metrics-runtime")),
				OtelMetricsRuntimeMinimumReadMemStatsInterval: pulumix.Val(pointer.For(getDuration(ctx, "ledger-otel-metrics-runtime-minimum-read-mem-stats-interval"))),
				OtelMetricsExporter:                           pulumix.Val(config.Get(ctx, "ledger-otel-metrics-exporter")),
				OtelMetricsKeepInMemory:                       pulumix.Val(config.GetBool(ctx, "ledger-otel-metrics-keep-in-memory")),
				OtelMetricsExporterOTLPMode:                   pulumix.Val(config.Get(ctx, "ledger-otel-metrics-exporter-otlp-mode")),
				OtelMetricsExporterOTLPEndpoint:               pulumix.Val(config.Get(ctx, "ledger-otel-metrics-exporter-otlp-endpoint")),
				OtelMetricsExporterOTLPInsecure:               pulumix.Val(config.GetBool(ctx, "ledger-otel-metrics-exporter-otlp-insecure")),
			}
		}

		cmp, err := pulumi_dataset_init_stack.NewStack(ctx, ctx.Stack(), args)

		ctx.Export("ledger-url", cmp.Ledger.ServiceInternalURL)

		return err
	})
}

func getDuration(ctx *pulumi.Context, key string) time.Duration {
	configValue := config.Get(ctx, key)

	ret, _ := time.ParseDuration(configValue)

	return ret
}
