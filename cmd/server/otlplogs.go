package server

import (
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/service"
	"github.com/formancehq/ledger-v3-poc/internal/monitoring/otlplogs"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
)

const (
	OtelLogsExporterFlag             = "otel-logs-exporter"
	OtelLogsExporterOTLPModeFlag     = "otel-logs-exporter-otlp-mode"
	OtelLogsExporterOTLPEndpointFlag = "otel-logs-exporter-otlp-endpoint"
	OtelLogsExporterOTLPInsecureFlag = "otel-logs-exporter-otlp-insecure"
)

func addOtlpLogsFlags(flags *flag.FlagSet) {
	otlp.AddFlags(flags)

	flags.String(OtelLogsExporterFlag, "", "OpenTelemetry logs exporter")
	flags.String(OtelLogsExporterOTLPModeFlag, "grpc", "OpenTelemetry logs OTLP exporter mode (grpc|http)")
	flags.String(OtelLogsExporterOTLPEndpointFlag, "", "OpenTelemetry logs grpc endpoint")
	flags.Bool(OtelLogsExporterOTLPInsecureFlag, false, "OpenTelemetry logs grpc insecure")
}

func loggerFromFlags(cmd *cobra.Command, defaultFields map[string]any) (logging.Logger, error) {
	exporter, _ := cmd.Flags().GetString(OtelLogsExporterFlag)
	jsonFormatting, _ := cmd.Flags().GetBool(logging.JsonFormattingLoggerFlag)

	return otlplogs.Logger(otlplogs.ModuleConfig{
		Exporter: exporter,
		OTLPConfig: func() *otlplogs.OTLPConfig {
			if exporter != otlplogs.OTLPExporter {
				return nil
			}
			mode, _ := cmd.Flags().GetString(OtelLogsExporterOTLPModeFlag)
			endpoint, _ := cmd.Flags().GetString(OtelLogsExporterOTLPEndpointFlag)
			insecure, _ := cmd.Flags().GetBool(OtelLogsExporterOTLPInsecureFlag)

			return &otlplogs.OTLPConfig{
				Mode:     mode,
				Endpoint: endpoint,
				Insecure: insecure,
			}
		}(),
		Output:     cmd.OutOrStdout(),
		Debug:      service.IsDebug(cmd),
		FormatJSON: jsonFormatting,
		Fields:     defaultFields,
	})
}
