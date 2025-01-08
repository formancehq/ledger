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

		connectors := make([]string, 0)
		if err := config.GetObject(ctx, "connectors", &connectors); err != nil {
			return err
		}

		ledgerConfig := config.New(ctx, "ledger")

		args := &pulumi_dataset_init_stack.StackComponentArgs{
			Debug:     pulumix.Val(config.GetBool(ctx, "debug")),
			Namespace: pulumi.String(config.Get(ctx, "namespace")),
			Ledger: &pulumi_dataset_init_stack.StackLedgerArgs{
				Version:     pulumix.Val(ledgerConfig.Get("version")),
				GracePeriod: pulumix.Val(ledgerConfig.Get("grace-period")),
				Upgrade:     pulumix.Val(pulumi_ledger.UpgradeMode(ledgerConfig.Get("upgrade-mode"))),
			},
			Connectors: connectors,
		}

		tracesEnabled := ledgerConfig.GetBool("otel-traces-enabled")
		metricsEnabled := ledgerConfig.GetBool("otel-metrics-enabled")
		if tracesEnabled || metricsEnabled {
			args.Ledger.Otel = &pulumi_ledger.OtelArgs{
				ResourceAttributes: pulumix.Val(map[string]string{
					"deployment.environment": "test-volumes",
				}),
			}
		}

		if tracesEnabled {
			args.Ledger.Otel.Traces = &pulumi_ledger.OtelTracesArgs{
				OtelTracesBatch:                  pulumix.Val(ledgerConfig.GetBool("otel-traces-batch")),
				OtelTracesExporterFlag:           pulumix.Val(ledgerConfig.Get("otel-traces-exporter")),
				OtelTracesExporterJaegerEndpoint: pulumix.Val(ledgerConfig.Get("otel-traces-exporter-jaeger-endpoint")),
				OtelTracesExporterJaegerUser:     pulumix.Val(ledgerConfig.Get("otel-traces-exporter-jaeger-user")),
				OtelTracesExporterJaegerPassword: pulumix.Val(ledgerConfig.Get("otel-traces-exporter-jaeger-password")),
				OtelTracesExporterOTLPMode:       pulumix.Val(ledgerConfig.Get("otel-traces-exporter-otlp-mode")),
				OtelTracesExporterOTLPEndpoint:   pulumix.Val(ledgerConfig.Get("otel-traces-exporter-otlp-endpoint")),
				OtelTracesExporterOTLPInsecure:   pulumix.Val(ledgerConfig.GetBool("otel-traces-exporter-otlp-insecure")),
			}
		}

		if metricsEnabled {
			args.Ledger.Otel.Metrics = &pulumi_ledger.OtelMetricsArgs{
				OtelMetricsExporterPushInterval:               pulumix.Val(pointer.For(getDuration(ledgerConfig, "otel-metrics-exporter-push-interval"))),
				OtelMetricsRuntime:                            pulumix.Val(ledgerConfig.GetBool("otel-metrics-runtime")),
				OtelMetricsRuntimeMinimumReadMemStatsInterval: pulumix.Val(pointer.For(getDuration(ledgerConfig, "otel-metrics-runtime-minimum-read-mem-stats-interval"))),
				OtelMetricsExporter:                           pulumix.Val(ledgerConfig.Get("otel-metrics-exporter")),
				OtelMetricsKeepInMemory:                       pulumix.Val(ledgerConfig.GetBool("otel-metrics-keep-in-memory")),
				OtelMetricsExporterOTLPMode:                   pulumix.Val(ledgerConfig.Get("otel-metrics-exporter-otlp-mode")),
				OtelMetricsExporterOTLPEndpoint:               pulumix.Val(ledgerConfig.Get("otel-metrics-exporter-otlp-endpoint")),
				OtelMetricsExporterOTLPInsecure:               pulumix.Val(ledgerConfig.GetBool("otel-metrics-exporter-otlp-insecure")),
			}
		}

		cmp, err := pulumi_dataset_init_stack.NewStack(ctx, ctx.Stack(), args)

		ctx.Export("ledger-url", cmp.Ledger.ServiceInternalURL)

		return err
	})
}

func getDuration(config *config.Config, key string) time.Duration {
	configValue := config.Get(key)

	ret, _ := time.ParseDuration(configValue)

	return ret
}
