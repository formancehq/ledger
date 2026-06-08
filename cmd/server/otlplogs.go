package server

import (
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"

	otlp "github.com/formancehq/go-libs/v5/pkg/observe"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/otlplogs"
)

const (
	OtelLogsExporterFlag             = "otel-logs-exporter"
	OtelLogsExporterOTLPModeFlag     = "otel-logs-exporter-otlp-mode"
	OtelLogsExporterOTLPEndpointFlag = "otel-logs-exporter-otlp-endpoint"
	OtelLogsExporterOTLPInsecureFlag = "otel-logs-exporter-otlp-insecure"
	LogLevelFlag                     = "log-level"
)

func addOtlpLogsFlags(flags *flag.FlagSet) {
	otlp.AddFlags(flags)

	flags.String(OtelLogsExporterFlag, "", "OpenTelemetry logs exporter")
	flags.String(OtelLogsExporterOTLPModeFlag, "grpc", "OpenTelemetry logs OTLP exporter mode (grpc|http)")
	flags.String(OtelLogsExporterOTLPEndpointFlag, "", "OpenTelemetry logs grpc endpoint")
	flags.Bool(OtelLogsExporterOTLPInsecureFlag, false, "OpenTelemetry logs grpc insecure")
	flags.String(LogLevelFlag, "", "Log level (error|info|debug|trace). Overrides --debug when set. Trace is stdout-only and never exported via OTLP.")
}

func loggerFromFlags(cmd *cobra.Command, defaultFields map[string]any) (logging.Logger, error) {
	exporter, _ := cmd.Flags().GetString(OtelLogsExporterFlag)
	jsonFormatting, _ := cmd.Flags().GetBool(logging.JsonFormattingLoggerFlag)

	level, err := resolveLogLevel(cmd)
	if err != nil {
		return nil, err
	}

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
		Level:      level,
		FormatJSON: jsonFormatting,
		Fields:     defaultFields,
	})
}

// resolveLogLevel picks the effective log level from the CLI flags.
// --log-level wins when explicitly set; otherwise --debug maps to DebugLevel;
// otherwise the default is InfoLevel.
func resolveLogLevel(cmd *cobra.Command) (logging.Level, error) {
	if raw, _ := cmd.Flags().GetString(LogLevelFlag); raw != "" {
		return logging.ParseLevel(raw)
	}
	if service.IsDebug(cmd) {
		return logging.DebugLevel, nil
	}

	return logging.InfoLevel, nil
}
