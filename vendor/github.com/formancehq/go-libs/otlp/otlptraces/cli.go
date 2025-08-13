package otlptraces

import (
	"github.com/formancehq/go-libs/otlp"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
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

func AddFlags(flags *flag.FlagSet) {
	otlp.AddFlags(flags)

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

func FXModuleFromFlags(cmd *cobra.Command) fx.Option {
	otlpEnabled, _ := cmd.Flags().GetBool(OtelTracesFlag)
	if otlpEnabled {
		batch, _ := cmd.Flags().GetBool(OtelTracesBatchFlag)
		exporter, _ := cmd.Flags().GetString(OtelTracesExporterFlag)
		serviceName, _ := cmd.Flags().GetString(otlp.OtelServiceNameFlag)
		resourceAttributes, _ := cmd.Flags().GetStringSlice(otlp.OtelResourceAttributesFlag)

		return TracesModule(ModuleConfig{
			Batch:    batch,
			Exporter: exporter,
			OTLPConfig: func() *OTLPConfig {
				if exporter != OTLPExporter {
					return nil
				}
				mode, _ := cmd.Flags().GetString(OtelTracesExporterOTLPModeFlag)
				endpoint, _ := cmd.Flags().GetString(OtelTracesExporterOTLPEndpointFlag)
				insecure, _ := cmd.Flags().GetBool(OtelTracesExporterOTLPInsecureFlag)

				return &OTLPConfig{
					Mode:     mode,
					Endpoint: endpoint,
					Insecure: insecure,
				}
			}(),
			ServiceName:        serviceName,
			ResourceAttributes: resourceAttributes,
		})
	}
	return fx.Options()
}
