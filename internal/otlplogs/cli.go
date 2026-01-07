package otlplogs

import (
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/service"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/otlp"
)

const (
	OtelLogsExporterFlag               = "otel-logs-exporter"
	OtelLogsExporterOTLPModeFlag       = "otel-logs-exporter-otlp-mode"
	OtelLogsExporterOTLPEndpointFlag   = "otel-logs-exporter-otlp-endpoint"
	OtelLogsExporterOTLPInsecureFlag   = "otel-logs-exporter-otlp-insecure"
)

func AddFlags(flags *flag.FlagSet) {
	otlp.AddFlags(flags)

	flags.String(OtelLogsExporterFlag, "", "OpenTelemetry logs exporter")
	flags.String(OtelLogsExporterOTLPModeFlag, "grpc", "OpenTelemetry logs OTLP exporter mode (grpc|http)")
	flags.String(OtelLogsExporterOTLPEndpointFlag, "", "OpenTelemetry logs grpc endpoint")
	flags.Bool(OtelLogsExporterOTLPInsecureFlag, false, "OpenTelemetry logs grpc insecure")
}

func FXModuleFromFlags(cmd *cobra.Command, defaultFields map[string]any) fx.Option {
	exporter, _ := cmd.Flags().GetString(OtelLogsExporterFlag)
	jsonFormatting, _ := cmd.Flags().GetBool(logging.JsonFormattingLoggerFlag)

	return LogsModule(ModuleConfig{
		Exporter: exporter,
		OTLPConfig: func() *OTLPConfig {
			if exporter != OTLPExporter {
				return nil
			}
			mode, _ := cmd.Flags().GetString(OtelLogsExporterOTLPModeFlag)
			endpoint, _ := cmd.Flags().GetString(OtelLogsExporterOTLPEndpointFlag)
			insecure, _ := cmd.Flags().GetBool(OtelLogsExporterOTLPInsecureFlag)

			return &OTLPConfig{
				Mode:     mode,
				Endpoint: endpoint,
				Insecure: insecure,
			}
		}(),
		Output:             cmd.OutOrStdout(),
		Debug:              service.IsDebug(cmd),
		FormatJSON:         jsonFormatting,
		Fields: defaultFields,
	})
}
