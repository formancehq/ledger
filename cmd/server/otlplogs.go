package server

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

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

const (
	// defaultServiceName is the logical service reported by every node. A
	// cluster is told apart by service.instance.id (its name) and its nodes by
	// formance.ledger.node.id, not by service.name.
	defaultServiceName = "ledger"

	// Custom resource attributes live under the formance.ledger namespace, the
	// same one that prefixes the ledger metrics: OpenTelemetry reserves the
	// semantic-convention namespaces (service.*, k8s.*, ...) for its own
	// attributes.
	resourceAttributeClusterID   = "formance.ledger.cluster.id"
	resourceAttributeClusterName = "formance.ledger.cluster.name"
	resourceAttributeNodeID      = "formance.ledger.node.id"
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
// The node identity attributes (service.instance.id, formance.ledger.cluster.*,
// formance.ledger.node.id) are placed before --otel-resource-attributes, which
// OTEL_RESOURCE_ATTRIBUTES is bound to, so go-libs' last-wins precedence keeps
// explicit overrides authoritative, as it does for service.name and
// service.version.
//
// The cluster name, not the declared cluster ID, keys the cluster: IDs are
// declared per deployment and repeat across clusters (EN-2031). The operator
// supplies the name (the Cluster resource name) and k8s.namespace.name, which
// together tell clusters apart; without the operator the name defaults to the
// cluster ID. service.instance.id is the effective cluster name: the cluster
// is the ledger instance, and its nodes are told apart by
// formance.ledger.node.id.
func resourceFromFlags(cmd *cobra.Command, clusterID string, nodeID uint64, info version.Info) (*resource.Resource, error) {
	// addOtlpLogsFlags registers this flag with its string type before use.
	serviceName, _ := cmd.Flags().GetString(otlp.OtelServiceNameFlag)
	if serviceName == "" {
		serviceName = defaultServiceName
		if err := cmd.Flags().Set(otlp.OtelServiceNameFlag, serviceName); err != nil {
			return nil, fmt.Errorf("setting default service name: %w", err)
		}
	}
	// addOtlpLogsFlags registers this flag with its string-slice type before use.
	explicit, _ := cmd.Flags().GetStringSlice(otlp.OtelResourceAttributesFlag)
	clusterName := lastAttributeValue(explicit, resourceAttributeClusterName, clusterID)
	attributes := append([]string{
		fmt.Sprintf("%s=%s", semconv.ServiceInstanceIDKey, clusterName),
		fmt.Sprintf("%s=%s", resourceAttributeClusterID, clusterID),
		fmt.Sprintf("%s=%s", resourceAttributeClusterName, clusterID),
		fmt.Sprintf("%s=%d", resourceAttributeNodeID, nodeID),
	}, explicit...)

	return otlp.BuildResource(serviceName, attributes, info.ServiceVersion())
}

// lastAttributeValue returns the value the resource will hold for key: the
// last key=value entry wins, as in go-libs BuildResource. Malformed entries
// are skipped here; BuildResource rejects them.
func lastAttributeValue(attributes []string, key, fallback string) string {
	value := fallback
	for _, attribute := range attributes {
		if k, v, ok := strings.Cut(attribute, "="); ok && k == key {
			value = v
		}
	}

	return value
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
