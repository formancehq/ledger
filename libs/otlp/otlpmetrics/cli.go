package otlpmetrics

import (
	"time"

	"github.com/formancehq/stack/libs/go-libs/otlp"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
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

func InitOTLPMetricsFlags(flags *flag.FlagSet) {
	otlp.InitOTLPFlags(flags)

	flags.Bool(OtelMetricsFlag, false, "Enable OpenTelemetry traces support")
	flags.Duration(OtelMetricsExporterPushIntervalFlag, 10*time.Second, "OpenTelemetry metrics exporter push interval")
	flags.Bool(OtelMetricsRuntimeFlag, false, "Enable OpenTelemetry runtime metrics")
	flags.Duration(OtelMetricsRuntimeMinimumReadMemStatsIntervalFlag, 15*time.Second, "OpenTelemetry runtime metrics minimum read mem stats interval")
	flags.String(OtelMetricsExporterFlag, "stdout", "OpenTelemetry metrics exporter")
	flags.String(OtelMetricsExporterOTLPModeFlag, "grpc", "OpenTelemetry traces OTLP exporter mode (grpc|http)")
	flags.String(OtelMetricsExporterOTLPEndpointFlag, "", "OpenTelemetry traces grpc endpoint")
	flags.Bool(OtelMetricsExporterOTLPInsecureFlag, false, "OpenTelemetry traces grpc insecure")
}

func CLIMetricsModule() fx.Option {
	if viper.GetBool(OtelMetricsFlag) {
		return MetricsModule(ModuleConfig{
			ServiceName:    viper.GetString(otlp.OtelServiceName),
			ServiceVersion: "develop",
			OTLPConfig: &OTLPConfig{
				Mode:     viper.GetString(OtelMetricsExporterOTLPModeFlag),
				Endpoint: viper.GetString(OtelMetricsExporterOTLPEndpointFlag),
				Insecure: viper.GetBool(OtelMetricsExporterOTLPInsecureFlag),
			},
			Exporter:                    viper.GetString(OtelMetricsExporterFlag),
			RuntimeMetrics:              viper.GetBool(OtelMetricsRuntimeFlag),
			MinimumReadMemStatsInterval: viper.GetDuration(OtelMetricsRuntimeMinimumReadMemStatsIntervalFlag),
			PushInterval:                viper.GetDuration(OtelMetricsExporterPushIntervalFlag),
			ResourceAttributes:          viper.GetStringSlice(otlp.OtelResourceAttributes),
		})
	}
	return fx.Options()
}
