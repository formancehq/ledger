package otlpmetrics

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/formancehq/go-libs/otlp"
	flag "github.com/spf13/pflag"
	"go.uber.org/fx"
)

const (
	OtelMetricsFlag                                   = "otel-metrics"
	OtelMetricsExporterPushIntervalFlag               = "otel-metrics-exporter-push-interval"
	OtelMetricsRuntimeFlag                            = "otel-metrics-runtime"
	OtelMetricsRuntimeMinimumReadMemStatsIntervalFlag = "otel-metrics-runtime-minimum-read-mem-stats-interval"
	OtelMetricsExporterFlag                           = "otel-metrics-exporter"
	OtelMetricsExporterOTLPModeFlag                   = "otel-metrics-exporter-otlp-mode"
	OtelMetricsExporterOTLPEndpointFlag               = "otel-metrics-exporter-otlp-endpoint"
	OtelMetricsExporterOTLPInsecureFlag               = "otel-metrics-exporter-otlp-insecure"
)

func AddFlags(flags *flag.FlagSet) {
	otlp.AddFlags(flags)

	flags.Bool(OtelMetricsFlag, false, "Enable OpenTelemetry traces support")
	flags.Duration(OtelMetricsExporterPushIntervalFlag, 10*time.Second, "OpenTelemetry metrics exporter push interval")
	flags.Bool(OtelMetricsRuntimeFlag, false, "Enable OpenTelemetry runtime metrics")
	flags.Duration(OtelMetricsRuntimeMinimumReadMemStatsIntervalFlag, 15*time.Second, "OpenTelemetry runtime metrics minimum read mem stats interval")
	flags.String(OtelMetricsExporterFlag, "stdout", "OpenTelemetry metrics exporter")
	flags.String(OtelMetricsExporterOTLPModeFlag, "grpc", "OpenTelemetry traces OTLP exporter mode (grpc|http)")
	flags.String(OtelMetricsExporterOTLPEndpointFlag, "", "OpenTelemetry traces grpc endpoint")
	flags.Bool(OtelMetricsExporterOTLPInsecureFlag, false, "OpenTelemetry traces grpc insecure")
}

func FXModuleFromFlags(cmd *cobra.Command) fx.Option {
	otelMetrics, _ := cmd.Flags().GetBool(OtelMetricsFlag)

	if otelMetrics {
		otelServiceName, _ := cmd.Flags().GetString(otlp.OtelServiceNameFlag)
		otelMetricsExporterOTLPEndpoint, _ := cmd.Flags().GetString(OtelMetricsExporterOTLPEndpointFlag)
		otelMetricsExporterOTLPMode, _ := cmd.Flags().GetString(OtelMetricsExporterOTLPModeFlag)
		otelMetricsExporterOTLPInsecure, _ := cmd.Flags().GetBool(OtelMetricsExporterOTLPInsecureFlag)
		otelMetricsExporter, _ := cmd.Flags().GetString(OtelMetricsExporterFlag)
		otelMetricsRuntime, _ := cmd.Flags().GetBool(OtelMetricsRuntimeFlag)
		otelMetricsRuntimeMinimumReadMemStatsInterval, _ := cmd.Flags().GetDuration(OtelMetricsRuntimeMinimumReadMemStatsIntervalFlag)
		otelMetricsExporterPushInterval, _ := cmd.Flags().GetDuration(OtelMetricsExporterPushIntervalFlag)
		otelResourceAttributes, _ := cmd.Flags().GetStringSlice(otlp.OtelResourceAttributesFlag)

		return MetricsModule(ModuleConfig{
			ServiceName:    otelServiceName,
			ServiceVersion: "develop",
			OTLPConfig: &OTLPConfig{
				Mode:     otelMetricsExporterOTLPMode,
				Endpoint: otelMetricsExporterOTLPEndpoint,
				Insecure: otelMetricsExporterOTLPInsecure,
			},
			Exporter:                    otelMetricsExporter,
			RuntimeMetrics:              otelMetricsRuntime,
			MinimumReadMemStatsInterval: otelMetricsRuntimeMinimumReadMemStatsInterval,
			PushInterval:                otelMetricsExporterPushInterval,
			ResourceAttributes:          otelResourceAttributes,
		})
	}
	return fx.Options()
}
