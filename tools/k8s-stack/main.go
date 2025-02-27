package main

import (
	"errors"
	"github.com/formancehq/go-libs/v2/pointer"
	pulumi_ledger "github.com/formancehq/ledger/deployments/pulumi/pkg"
	pulumi_ledger_testing "github.com/formancehq/ledger/tools/k8s-stack/pkg"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
	"time"
)

type IngressConfig struct {
	Host   string `json:"host"`
	Secret string `json:"secret"`
}

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {

		ledgerConfig := config.New(ctx, "ledger")

		ingress := &IngressConfig{}
		if err := ledgerConfig.TryObject("ingress", ingress); err != nil {
			if !errors.Is(err, config.ErrMissingVar) {
				return err
			}
		}

		args := &pulumi_ledger_testing.StackComponentArgs{
			Debug:     pulumix.Val(config.GetBool(ctx, "debug")),
			Namespace: pulumi.String(config.Get(ctx, "namespace")),
			Ledger: &pulumi_ledger_testing.StackLedgerArgs{
				Version:     pulumix.Val(ledgerConfig.Get("version")),
				GracePeriod: pulumix.Val(ledgerConfig.Get("grace-period")),
				Ingress: func() *pulumi_ledger.IngressArgs {
					if ingress.Host == "" {
						return nil
					}

					return &pulumi_ledger.IngressArgs{
						Host:   pulumix.Val(ingress.Host),
						Secret: pulumix.Val(&ingress.Secret),
					}
				}(),
			},
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

		cmp, err := pulumi_ledger_testing.NewStack(ctx, ctx.Stack(), args)

		ctx.Export("ledger-url", cmp.Ledger.ServiceInternalURL)
		ctx.Export("ledger-deployment", cmp.Ledger.ServerDeployment.Metadata.Name())
		ctx.Export("postgres-service", cmp.Postgres.Service)
		ctx.Export("postgres-username", cmp.Postgres.Username)
		ctx.Export("postgres-password", cmp.Postgres.Password)
		ctx.Export("namespace", cmp.Namespace)

		return err
	})
}

func getDuration(config *config.Config, key string) time.Duration {
	configValue := config.Get(key)

	ret, _ := time.ParseDuration(configValue)

	return ret
}
