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

func CLITracesModule(v *viper.Viper) fx.Option {
	if v.GetBool(OtelTracesFlag) {
		return TracesModule(ModuleConfig{
			Batch:    v.GetBool(OtelTracesBatchFlag),
			Exporter: v.GetString(OtelTracesExporterFlag),
			OTLPConfig: func() *OTLPConfig {
				if v.GetString(OtelTracesExporterFlag) != OTLPExporter {
					return nil
				}
				return &OTLPConfig{
					Mode:     v.GetString(OtelTracesExporterOTLPModeFlag),
					Endpoint: v.GetString(OtelTracesExporterOTLPEndpointFlag),
					Insecure: v.GetBool(OtelTracesExporterOTLPInsecureFlag),
				}
			}(),
			ServiceName:        v.GetString(otlp.OtelServiceName),
			ResourceAttributes: v.GetStringSlice(otlp.OtelResourceAttributes),
		})
	}
	return fx.Options()
}
