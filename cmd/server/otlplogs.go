package server

import (
	"fmt"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"go.opentelemetry.io/otel/sdk/resource"

	otlp "github.com/formancehq/go-libs/v5/pkg/observe"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/otlplogs"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
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

// resourceFromFlags builds the resource shared by logs, traces, and metrics.
// Keep go-libs' resource attribute precedence, including explicit overrides of
// service.name and service.version through --otel-resource-attributes.
func resourceFromFlags(cmd *cobra.Command, nodeID uint64, info version.Info) (*resource.Resource, error) {
	serviceName, _ := cmd.Flags().GetString(otlp.OtelServiceNameFlag)
	if serviceName == "" {
		serviceName = fmt.Sprintf("ledger-node-%d", nodeID)
		if err := cmd.Flags().Set(otlp.OtelServiceNameFlag, serviceName); err != nil {
			return nil, fmt.Errorf("setting default service name: %w", err)
		}
	}
	attributes, _ := cmd.Flags().GetStringSlice(otlp.OtelResourceAttributesFlag)

	return otlp.BuildResource(serviceName, attributes, fmt.Sprintf("%s-%s", info.Version, info.Commit))
}

func loggerFromFlags(cmd *cobra.Command, defaultFields map[string]any, res *resource.Resource) (logging.Logger, error) {
	exporter, _ := cmd.Flags().GetString(OtelLogsExporterFlag)
	jsonFormatting, _ := cmd.Flags().GetBool(logging.JsonFormattingLoggerFlag)

	level, err := resolveLogLevel(cmd)
	if err != nil {
		return nil, err
	}

	return otlplogs.Logger(otlplogs.ModuleConfig{
		Exporter: exporter,
		Resource: res,
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
