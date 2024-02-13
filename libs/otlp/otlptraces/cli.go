package otlptraces

import (
	"github.com/formancehq/stack/libs/go-libs/otlp"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const (
	OtelTracesFlag                       = "otel-traces"
	OtelTracesBatchFlag                  = "otel-traces-batch"
	OtelTracesExporterFlag               = "otel-traces-exporter"
	OtelTracesExporterJaegerEndpointFlag = "otel-traces-exporter-jaeger-endpoint"
	OtelTracesExporterJaegerUserFlag     = "otel-traces-exporter-jaeger-user"
	OtelTracesExporterJaegerPasswordFlag = "otel-traces-exporter-jaeger-password"
	OtelTracesExporterOTLPModeFlag       = "otel-traces-exporter-otlp-mode"
	OtelTracesExporterOTLPEndpointFlag   = "otel-traces-exporter-otlp-endpoint"
	OtelTracesExporterOTLPInsecureFlag   = "otel-traces-exporter-otlp-insecure"
)

func InitOTLPTracesFlags(flags *flag.FlagSet) {
	otlp.InitOTLPFlags(flags)

	flags.Bool(OtelTracesFlag, false, "Enable OpenTelemetry traces support")
	flags.Bool(OtelTracesBatchFlag, false, "Use OpenTelemetry batching")
	flags.String(OtelTracesExporterFlag, "stdout", "OpenTelemetry traces exporter")
	flags.String(OtelTracesExporterJaegerEndpointFlag, "", "OpenTelemetry traces Jaeger exporter endpoint")
	flags.String(OtelTracesExporterJaegerUserFlag, "", "OpenTelemetry traces Jaeger exporter user")
	flags.String(OtelTracesExporterJaegerPasswordFlag, "", "OpenTelemetry traces Jaeger exporter password")
	flags.String(OtelTracesExporterOTLPModeFlag, "grpc", "OpenTelemetry traces OTLP exporter mode (grpc|http)")
	flags.String(OtelTracesExporterOTLPEndpointFlag, "", "OpenTelemetry traces grpc endpoint")
	flags.Bool(OtelTracesExporterOTLPInsecureFlag, false, "OpenTelemetry traces grpc insecure")
}

func CLITracesModule() fx.Option {
	if viper.GetBool(OtelTracesFlag) {
		return TracesModule(ModuleConfig{
			Batch:    viper.GetBool(OtelTracesBatchFlag),
			Exporter: viper.GetString(OtelTracesExporterFlag),
			OTLPConfig: func() *OTLPConfig {
				if viper.GetString(OtelTracesExporterFlag) != OTLPExporter {
					return nil
				}
				return &OTLPConfig{
					Mode:     viper.GetString(OtelTracesExporterOTLPModeFlag),
					Endpoint: viper.GetString(OtelTracesExporterOTLPEndpointFlag),
					Insecure: viper.GetBool(OtelTracesExporterOTLPInsecureFlag),
				}
			}(),
			ServiceName:        viper.GetString(otlp.OtelServiceName),
			ResourceAttributes: viper.GetStringSlice(otlp.OtelResourceAttributes),
		})
	}
	return fx.Options()
}
