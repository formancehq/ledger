package explain

import (
	"fmt"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// Field describes a single CRD field.
type Field struct {
	Name        string
	Type        string
	Required    bool
	Default     string
	Description string
	Children    []Field
}

// SpecFields returns the schema fields for the LedgerService spec.
func SpecFields() []Field {
	return specFields()
}

// Lookup finds a field by dotted path (e.g. "config.raft.electionTick").
func Lookup(fields []Field, path string) (Field, bool) {
	return lookup(fields, path)
}

// DefaultsSpecFields returns the schema fields for the LedgerDefaults spec.
func DefaultsSpecFields() []Field {
	return defaultsSpecFields()
}

// DefaultsConfigFields returns the schema fields for the LedgerDefaults config section.
func DefaultsConfigFields() []Field {
	return defaultsConfigFields()
}

// NewCommand returns the "explain" command.
func NewCommand() *cobra.Command {
	var showDefaults bool

	cmd := &cobra.Command{
		Use:     "explain [field.path]",
		Aliases: []string{"schema", "fields"},
		Short:   "Describe the LedgerService CRD schema and fields",
		Long:    "Displays the LedgerService CRD field hierarchy with types, defaults, and descriptions.\nOptionally pass a dotted field path to show only that subtree (e.g. spec.config.raft).\nUse --defaults to show the LedgerDefaults schema instead.",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if showDefaults {
				return runExplainDefaults(args)
			}
			return runExplain(args)
		},
	}

	cmd.Flags().BoolVar(&showDefaults, "defaults", false, "Show the LedgerDefaults schema instead of LedgerService")

	return cmd
}

func runExplain(args []string) error {
	root := ledgerSchema()

	if len(args) > 0 {
		node, ok := Lookup(root, args[0])
		if !ok {
			return fmt.Errorf("unknown field path %q", args[0])
		}
		pterm.Println()
		pterm.Printf("%s %s\n", pterm.Bold.Sprint(pterm.Cyan(args[0])), pterm.Gray(node.Type))
		if node.Description != "" {
			pterm.Printf("  %s\n", node.Description)
		}
		if node.Default != "" {
			pterm.Printf("  Default: %s\n", pterm.Green(node.Default))
		}
		pterm.Println()
		printFields(node.Children, 0)
		return nil
	}

	pterm.Println()
	pterm.DefaultSection.Println("LedgerService CRD — ledger.formance.com/v1alpha1")
	printFields(root, 0)
	return nil
}

func lookup(fields []Field, path string) (Field, bool) {
	parts := strings.SplitN(path, ".", 2)
	for _, f := range fields {
		if f.Name == parts[0] {
			if len(parts) == 1 {
				return f, true
			}
			return lookup(f.Children, parts[1])
		}
	}
	return Field{}, false
}

func printFields(fields []Field, depth int) {
	indent := strings.Repeat("  ", depth)
	for _, f := range fields {
		req := ""
		if f.Required {
			req = pterm.Red(" *")
		}
		def := ""
		if f.Default != "" {
			def = pterm.Gray(fmt.Sprintf(" [%s]", f.Default))
		}

		pterm.Printf("%s%s%s  %s%s\n", indent, pterm.Cyan(f.Name), req, pterm.Gray(f.Type), def)
		if f.Description != "" {
			pterm.Printf("%s  %s\n", indent, f.Description)
		}

		if len(f.Children) > 0 {
			printFields(f.Children, depth+1)
		}
	}
}

func ledgerSchema() []Field {
	return []Field{
		{Name: "spec", Type: "object", Description: "Desired state of the LedgerService deployment.", Children: specFields()},
		{Name: "status", Type: "object", Description: "Observed state (read-only, set by the operator).", Children: statusFields()},
	}
}

func specFields() []Field {
	return []Field{
		{Name: "defaultsRef", Type: "string", Description: "Name of a cluster-scoped LedgerDefaults resource to inherit defaults from."},
		{Name: "replicas", Type: "int32", Default: "3", Description: "Number of Raft nodes (must be odd)."},
		{Name: "image", Type: "object", Description: "Container image configuration.", Children: imageFields()},
		{Name: "imagePullSecrets", Type: "[]LocalObjectReference", Description: "Secrets for private container registries."},
		{Name: "serviceAccount", Type: "object", Description: "Service account configuration.", Children: serviceAccountFields()},
		{Name: "config", Type: "object", Description: "Application configuration.", Children: configFields()},
		{Name: "service", Type: "object", Description: "ClusterIP service configuration.", Children: serviceFields()},
		{Name: "headlessService", Type: "object", Description: "Headless service for Raft peer discovery.", Children: headlessServiceFields()},
		{Name: "ingress", Type: "object", Description: "HTTP ingress configuration.", Children: ingressFields()},
		{Name: "ingressGrpc", Type: "object", Description: "gRPC ingress configuration.", Children: ingressGrpcFields()},
		{Name: "persistence", Type: "object", Description: "WAL and data volume configuration.", Children: persistenceFields()},
		{Name: "resources", Type: "ResourceRequirements", Description: "CPU and memory resource requests/limits."},
		{Name: "livenessProbe", Type: "Probe", Description: "Liveness probe configuration override."},
		{Name: "readinessProbe", Type: "Probe", Description: "Readiness probe configuration override."},
		{Name: "podAnnotations", Type: "map[string]string", Description: "Annotations added to each pod (used for rolling restarts)."},
		{Name: "nodeSelector", Type: "map[string]string", Description: "Node selector for pod scheduling."},
		{Name: "tolerations", Type: "[]Toleration", Description: "Tolerations for pod scheduling."},
		{Name: "affinity", Type: "Affinity", Description: "Affinity rules for pod scheduling."},
		{Name: "podAntiAffinity", Type: "object", Description: "Pod anti-affinity configuration.", Children: podAntiAffinityFields()},
		{Name: "podDisruptionBudget", Type: "object", Description: "PodDisruptionBudget configuration.", Children: pdbFields()},
		{Name: "podSecurityContext", Type: "PodSecurityContext", Description: "Pod-level security context."},
		{Name: "securityContext", Type: "SecurityContext", Description: "Container-level security context."},
		{Name: "serviceMonitor", Type: "object", Description: "Prometheus ServiceMonitor configuration.", Children: serviceMonitorFields()},
	}
}

func imageFields() []Field {
	return []Field{
		{Name: "repository", Type: "string", Default: "ghcr.io/formancehq/ledger-v3-poc", Description: "Container image repository."},
		{Name: "tag", Type: "string", Default: "latest", Description: "Container image tag."},
		{Name: "pullPolicy", Type: "string", Default: "IfNotPresent", Description: "Image pull policy."},
	}
}

func serviceAccountFields() []Field {
	return []Field{
		{Name: "create", Type: "bool", Default: "true", Description: "Create a service account."},
		{Name: "annotations", Type: "map[string]string", Description: "Annotations on the service account."},
		{Name: "name", Type: "string", Description: "Override the service account name."},
	}
}

func configFields() []Field {
	return []Field{
		{Name: "clusterID", Type: "string", Default: "default", Description: "Cluster ID for inter-node validation."},
		{Name: "bindAddr", Type: "string", Default: "0.0.0.0:7777", Description: "Raft transport bind address."},
		{Name: "grpcPort", Type: "int32", Default: "8888", Description: "gRPC service port."},
		{Name: "httpPort", Type: "int32", Default: "9000", Description: "HTTP service port."},
		{Name: "walDir", Type: "string", Default: "/data/raft", Description: "WAL data directory."},
		{Name: "dataDir", Type: "string", Default: "/data/app", Description: "Application data directory."},
		{Name: "debug", Type: "bool", Description: "Enable debug logging."},
		{Name: "restore", Type: "bool", Description: "Start in restore mode."},
		{Name: "raft", Type: "object", Description: "Raft consensus tuning.", Children: raftFields()},
		{Name: "pebble", Type: "object", Description: "Pebble storage engine tuning.", Children: pebbleFields()},
		{Name: "cache", Type: "object", Description: "Cache configuration.", Children: cacheFields()},
		{Name: "health", Type: "object", Description: "Health check configuration.", Children: healthFields()},
		{Name: "audit", Type: "object", Description: "Audit log configuration.", Children: auditFields()},
		{Name: "admissionMetrics", Type: "bool", Description: "Enable admission path metrics."},
		{Name: "coldStorage", Type: "object", Description: "Cold storage archival configuration.", Children: coldStorageFields()},
		{Name: "tls", Type: "object", Description: "TLS configuration for gRPC connections.", Children: tlsFields()},
		{Name: "responseSigning", Type: "object", Description: "Ed25519 response signing.", Children: responseSigningFields()},
		{Name: "monitoring", Type: "object", Description: "OpenTelemetry monitoring.", Children: monitoringFields()},
	}
}

func raftFields() []Field {
	return []Field{
		{Name: "snapshotThreshold", Type: "int32", Description: "Number of log entries before triggering a snapshot."},
		{Name: "compactionMargin", Type: "int32", Description: "Compaction margin."},
		{Name: "snapshotInterval", Type: "duration", Description: "Minimum interval between snapshots."},
		{Name: "electionTick", Type: "int32", Description: "Election timeout in ticks."},
		{Name: "heartbeatTick", Type: "int32", Description: "Heartbeat interval in ticks."},
		{Name: "maxSizePerMsg", Type: "int64", Description: "Maximum size per Raft message in bytes."},
		{Name: "maxInflightMsgs", Type: "int32", Description: "Maximum number of in-flight messages."},
		{Name: "tickInterval", Type: "duration", Description: "Interval between Raft ticks."},
		{Name: "proposeQueueCapacity", Type: "int32", Description: "Capacity of the propose queue."},
		{Name: "learnerPromotionThreshold", Type: "int32", Description: "Max log entry lag before auto-promoting a learner."},
		{Name: "transport", Type: "object", Description: "Transport queue configuration.", Children: []Field{
			{Name: "receptionQueues", Type: "[]int32", Description: "Reception queue capacities per priority."},
			{Name: "sendQueues", Type: "[]int32", Description: "Send queue capacities per priority."},
		}},
	}
}

func pebbleFields() []Field {
	return []Field{
		{Name: "memTableSize", Type: "int64", Description: "MemTable size in bytes."},
		{Name: "memTableStopWritesThreshold", Type: "int32", Description: "Number of memtables before writes stop."},
		{Name: "l0CompactionThreshold", Type: "int32", Description: "L0 file count to trigger compaction."},
		{Name: "l0StopWritesThreshold", Type: "int32", Description: "L0 file count before writes stop."},
		{Name: "lBaseMaxBytes", Type: "int64", Description: "Maximum size of L1 in bytes."},
		{Name: "cacheSize", Type: "int64", Description: "Block cache size in bytes."},
		{Name: "targetFileSize", Type: "int64", Description: "Target SST file size in bytes."},
		{Name: "bytesPerSync", Type: "int64", Description: "Bytes written before sync during flush/compaction."},
		{Name: "walBytesPerSync", Type: "int64", Description: "WAL bytes written before sync."},
		{Name: "maxConcurrentCompactions", Type: "int32", Description: "Maximum concurrent compactions."},
		{Name: "walMinSyncInterval", Type: "duration", Description: "Minimum interval between WAL syncs."},
		{Name: "disableWAL", Type: "bool", Description: "Disable WAL entirely."},
	}
}

func cacheFields() []Field {
	return []Field{
		{Name: "rotationThreshold", Type: "int32", Description: "Raft log entries before rotating cache generations."},
	}
}

func healthFields() []Field {
	return []Field{
		{Name: "interval", Type: "duration", Description: "Interval between health checks."},
		{Name: "walThreshold", Type: "string", Description: "WAL volume usage threshold (0.0-1.0)."},
		{Name: "dataThreshold", Type: "string", Description: "Data volume usage threshold (0.0-1.0)."},
		{Name: "clockSkewThreshold", Type: "duration", Description: "Maximum allowed clock skew between nodes."},
	}
}

func auditFields() []Field {
	return []Field{
		{Name: "enabled", Type: "bool", Description: "Enable audit logging."},
	}
}

func coldStorageFields() []Field {
	return []Field{
		{Name: "driver", Type: "string", Description: "Storage driver: \"filesystem\" or \"s3\"."},
		{Name: "path", Type: "string", Description: "Base path for filesystem driver."},
		{Name: "bucketId", Type: "string", Description: "Shared namespace prefix for archives."},
		{Name: "s3", Type: "object", Description: "S3 configuration.", Children: []Field{
			{Name: "bucket", Type: "string", Description: "S3 bucket name."},
			{Name: "region", Type: "string", Description: "AWS region."},
			{Name: "endpoint", Type: "string", Description: "Custom S3 endpoint (for MinIO)."},
		}},
	}
}

func tlsFields() []Field {
	return []Field{
		{Name: "enabled", Type: "bool", Description: "Enable TLS."},
		{Name: "secretName", Type: "string", Description: "Kubernetes secret with TLS certificate and key."},
		{Name: "caSecretKey", Type: "string", Description: "Key for the CA certificate in the secret."},
	}
}

func responseSigningFields() []Field {
	return []Field{
		{Name: "enabled", Type: "bool", Description: "Enable Ed25519 response signing."},
		{Name: "secretName", Type: "string", Description: "Kubernetes secret containing the Ed25519 seed."},
		{Name: "secretKey", Type: "string", Default: "seed", Description: "Key in the secret containing the seed."},
	}
}

func monitoringFields() []Field {
	return []Field{
		{Name: "serviceName", Type: "string", Default: "ledger-v3-poc", Description: "Service name for monitoring."},
		{Name: "traces", Type: "object", Description: "Trace configuration.", Children: []Field{
			{Name: "enabled", Type: "bool", Description: "Enable tracing."},
			{Name: "exporter", Type: "string", Description: "Exporter type (e.g. \"otlp\")."},
			{Name: "endpoint", Type: "string", Description: "Exporter endpoint."},
			{Name: "port", Type: "string", Description: "Exporter port."},
			{Name: "insecure", Type: "string", Description: "Disable TLS for the exporter."},
			{Name: "mode", Type: "string", Description: "Exporter mode (e.g. \"grpc\")."},
			{Name: "batch", Type: "string", Description: "Enable batch mode."},
			{Name: "sampling", Type: "object", Description: "Sampling configuration.", Children: []Field{
				{Name: "enabled", Type: "bool", Description: "Enable error-aware trace sampling."},
				{Name: "successRatio", Type: "string", Description: "Sampling ratio for successful traces (0.0-1.0)."},
			}},
		}},
		{Name: "metrics", Type: "object", Description: "Metrics configuration.", Children: []Field{
			{Name: "enabled", Type: "bool", Description: "Enable metrics."},
			{Name: "exporter", Type: "string", Description: "Exporter type."},
			{Name: "endpoint", Type: "string", Description: "Exporter endpoint."},
			{Name: "port", Type: "string", Description: "Exporter port."},
			{Name: "insecure", Type: "string", Description: "Disable TLS for the exporter."},
			{Name: "mode", Type: "string", Description: "Exporter mode."},
			{Name: "keepInMemory", Type: "bool", Description: "Keep metrics in memory."},
			{Name: "exporterPushInterval", Type: "duration", Description: "Push interval."},
			{Name: "runtime", Type: "bool", Description: "Enable runtime metrics."},
			{Name: "runtimeMinimumReadMemStatsInterval", Type: "duration", Description: "Minimum interval for reading mem stats."},
		}},
		{Name: "logs", Type: "object", Description: "Log export configuration.", Children: []Field{
			{Name: "enabled", Type: "bool", Description: "Enable log exporting."},
			{Name: "level", Type: "string", Description: "Log level."},
			{Name: "exporter", Type: "string", Description: "Exporter type."},
			{Name: "endpoint", Type: "string", Description: "Exporter endpoint."},
			{Name: "port", Type: "string", Description: "Exporter port."},
			{Name: "insecure", Type: "string", Description: "Disable TLS for the exporter."},
			{Name: "mode", Type: "string", Description: "Exporter mode."},
		}},
		{Name: "attributes", Type: "string", Description: "Additional OTEL resource attributes."},
		{Name: "pyroscope", Type: "object", Description: "Pyroscope continuous profiling.", Children: []Field{
			{Name: "enabled", Type: "bool", Description: "Enable Pyroscope profiling."},
			{Name: "serverAddress", Type: "string", Description: "Pyroscope server address."},
			{Name: "applicationName", Type: "string", Description: "Override the application name."},
			{Name: "authToken", Type: "string", Description: "Authentication token."},
			{Name: "tenantId", Type: "string", Description: "Multi-tenant Pyroscope tenant ID."},
			{Name: "basicAuthUser", Type: "string", Description: "Basic auth username."},
			{Name: "basicAuthPassword", Type: "string", Description: "Basic auth password."},
			{Name: "uploadRate", Type: "duration", Description: "Upload interval."},
			{Name: "tags", Type: "string", Description: "Tags in key=value,key2=value2 format."},
			{Name: "profileTypes", Type: "string", Description: "Profile types to collect."},
			{Name: "mutexProfileFraction", Type: "int32", Description: "Mutex profile fraction."},
			{Name: "blockProfileRate", Type: "int32", Description: "Block profile rate."},
			{Name: "disableGCRuns", Type: "bool", Description: "Disable GC runs."},
		}},
	}
}

func serviceFields() []Field {
	return []Field{
		{Name: "type", Type: "string", Default: "ClusterIP", Description: "Service type."},
		{Name: "httpPort", Type: "int32", Default: "9000", Description: "HTTP service port."},
		{Name: "grpcPort", Type: "int32", Default: "8888", Description: "gRPC service port."},
		{Name: "raftPort", Type: "int32", Default: "7777", Description: "Raft transport port."},
		{Name: "annotations", Type: "map[string]string", Description: "Annotations on the service."},
	}
}

func headlessServiceFields() []Field {
	return []Field{
		{Name: "enabled", Type: "bool", Default: "true", Description: "Enable the headless service."},
		{Name: "annotations", Type: "map[string]string", Description: "Annotations on the headless service."},
	}
}

func ingressFields() []Field {
	return []Field{
		{Name: "enabled", Type: "bool", Description: "Enable HTTP ingress."},
		{Name: "className", Type: "string", Description: "Ingress class name."},
		{Name: "annotations", Type: "map[string]string", Description: "Annotations on the ingress."},
		{Name: "hosts", Type: "[]object", Description: "Ingress host rules.", Children: []Field{
			{Name: "host", Type: "string", Required: true, Description: "Hostname."},
			{Name: "paths", Type: "[]object", Description: "Path rules.", Children: []Field{
				{Name: "path", Type: "string", Default: "/", Description: "URL path."},
				{Name: "pathType", Type: "string", Default: "Prefix", Description: "Path matching type."},
			}},
		}},
		{Name: "tls", Type: "[]object", Description: "TLS configuration.", Children: []Field{
			{Name: "hosts", Type: "[]string", Description: "TLS hostnames."},
			{Name: "secretName", Type: "string", Description: "TLS secret name."},
		}},
	}
}

func ingressGrpcFields() []Field {
	return []Field{
		{Name: "enabled", Type: "bool", Description: "Enable gRPC ingress."},
		{Name: "className", Type: "string", Description: "Ingress class name (e.g. \"nginx\", \"traefik\")."},
		{Name: "annotations", Type: "map[string]string", Description: "Annotations on the ingress."},
		{Name: "hosts", Type: "[]object", Description: "Ingress host rules.", Children: []Field{
			{Name: "host", Type: "string", Required: true, Description: "Hostname."},
			{Name: "paths", Type: "[]object", Description: "Path rules.", Children: []Field{
				{Name: "path", Type: "string", Default: "/", Description: "URL path."},
				{Name: "pathType", Type: "string", Default: "Prefix", Description: "Path matching type."},
			}},
		}},
		{Name: "tls", Type: "[]object", Description: "TLS configuration.", Children: []Field{
			{Name: "hosts", Type: "[]string", Description: "TLS hostnames."},
			{Name: "secretName", Type: "string", Description: "TLS secret name."},
		}},
		{Name: "targetGroupBinding", Type: "object", Description: "AWS TargetGroupBinding configuration.", Children: []Field{
			{Name: "enabled", Type: "bool", Description: "Enable the TargetGroupBinding."},
			{Name: "targetGroupARN", Type: "string", Description: "ARN of the target group."},
			{Name: "targetType", Type: "string", Default: "ip", Description: "Target type: \"instance\" or \"ip\"."},
			{Name: "networking", Type: "object", Description: "Networking configuration (raw JSON)."},
		}},
	}
}

func persistenceFields() []Field {
	return []Field{
		{Name: "wal", Type: "object", Description: "WAL volume configuration.", Children: volumeSpecFields()},
		{Name: "data", Type: "object", Description: "Data volume configuration.", Children: volumeSpecFields()},
		{Name: "retentionPolicy", Type: "object", Description: "PVC retention policy.", Children: []Field{
			{Name: "whenScaled", Type: "string", Default: "Retain", Description: "Policy when scaling down."},
			{Name: "whenDeleted", Type: "string", Default: "Retain", Description: "Policy when deleting the LedgerService."},
		}},
	}
}

func volumeSpecFields() []Field {
	return []Field{
		{Name: "storageClass", Type: "string", Description: "Storage class for the PVC."},
		{Name: "accessMode", Type: "string", Default: "ReadWriteOnce", Description: "PVC access mode."},
		{Name: "size", Type: "Quantity", Description: "Volume size (e.g. \"10Gi\")."},
	}
}

func podAntiAffinityFields() []Field {
	return []Field{
		{Name: "enabled", Type: "bool", Default: "true", Description: "Enable pod anti-affinity."},
		{Name: "type", Type: "string", Default: "soft", Description: "Anti-affinity type: \"soft\" or \"hard\"."},
		{Name: "weight", Type: "int32", Default: "100", Description: "Weight for soft anti-affinity (1-100)."},
		{Name: "topologyKey", Type: "string", Default: "kubernetes.io/hostname", Description: "Topology key for anti-affinity."},
	}
}

func pdbFields() []Field {
	return []Field{
		{Name: "enabled", Type: "bool", Description: "Enable the PodDisruptionBudget."},
		{Name: "minAvailable", Type: "int32", Description: "Minimum number of available pods."},
		{Name: "maxUnavailable", Type: "int32", Description: "Maximum number of unavailable pods."},
	}
}

func serviceMonitorFields() []Field {
	return []Field{
		{Name: "enabled", Type: "bool", Description: "Enable the Prometheus ServiceMonitor."},
		{Name: "interval", Type: "duration", Description: "Scrape interval."},
		{Name: "scrapeTimeout", Type: "duration", Description: "Scrape timeout."},
		{Name: "labels", Type: "map[string]string", Description: "Labels on the ServiceMonitor."},
		{Name: "relabelings", Type: "[]object", Description: "Relabeling rules (raw JSON)."},
		{Name: "metricRelabelings", Type: "[]object", Description: "Metric relabeling rules (raw JSON)."},
	}
}

func statusFields() []Field {
	return []Field{
		{Name: "phase", Type: "string", Description: "Current phase: Pending, Running, or Degraded."},
		{Name: "readyReplicas", Type: "int32", Description: "Number of ready pods."},
		{Name: "observedGeneration", Type: "int64", Description: "Generation last observed by the controller."},
		{Name: "conditions", Type: "[]Condition", Description: "Latest available observations of the LedgerService's state."},
	}
}

// --- LedgerDefaults schema ---

func runExplainDefaults(args []string) error {
	root := ledgerDefaultsSchema()

	if len(args) > 0 {
		node, ok := Lookup(root, args[0])
		if !ok {
			return fmt.Errorf("unknown field path %q", args[0])
		}
		pterm.Println()
		pterm.Printf("%s %s\n", pterm.Bold.Sprint(pterm.Cyan(args[0])), pterm.Gray(node.Type))
		if node.Description != "" {
			pterm.Printf("  %s\n", node.Description)
		}
		if node.Default != "" {
			pterm.Printf("  Default: %s\n", pterm.Green(node.Default))
		}
		pterm.Println()
		printFields(node.Children, 0)
		return nil
	}

	pterm.Println()
	pterm.DefaultSection.Println("LedgerDefaults CRD — ledger.formance.com/v1alpha1")
	printFields(root, 0)
	return nil
}

func ledgerDefaultsSchema() []Field {
	return []Field{
		{Name: "spec", Type: "object", Description: "Shared default values for LedgerService deployments.", Children: defaultsSpecFields()},
	}
}

func defaultsSpecFields() []Field {
	return []Field{
		{Name: "image", Type: "object", Description: "Default container image configuration.", Children: imageFields()},
		{Name: "imagePullSecrets", Type: "[]LocalObjectReference", Description: "Default secrets for private container registries."},
		{Name: "serviceAccount", Type: "object", Description: "Default service account configuration.", Children: serviceAccountFields()},
		{Name: "config", Type: "object", Description: "Default application configuration (shared subset).", Children: defaultsConfigFields()},
		{Name: "resources", Type: "ResourceRequirements", Description: "Default CPU and memory resource requests/limits."},
		{Name: "livenessProbe", Type: "Probe", Description: "Default liveness probe configuration."},
		{Name: "readinessProbe", Type: "Probe", Description: "Default readiness probe configuration."},
		{Name: "podSecurityContext", Type: "PodSecurityContext", Description: "Default pod-level security context."},
		{Name: "securityContext", Type: "SecurityContext", Description: "Default container-level security context."},
		{Name: "nodeSelector", Type: "map[string]string", Description: "Default node selector for pod scheduling."},
		{Name: "tolerations", Type: "[]Toleration", Description: "Default tolerations for pod scheduling."},
		{Name: "affinity", Type: "Affinity", Description: "Default affinity rules for pod scheduling."},
		{Name: "podAntiAffinity", Type: "object", Description: "Default pod anti-affinity configuration.", Children: podAntiAffinityFields()},
		{Name: "podDisruptionBudget", Type: "object", Description: "Default PodDisruptionBudget configuration.", Children: pdbFields()},
		{Name: "serviceMonitor", Type: "object", Description: "Default Prometheus ServiceMonitor configuration.", Children: serviceMonitorFields()},
	}
}

func defaultsConfigFields() []Field {
	return []Field{
		{Name: "pebble", Type: "object", Description: "Default Pebble storage engine tuning.", Children: pebbleFields()},
		{Name: "raft", Type: "object", Description: "Default Raft consensus tuning.", Children: raftFields()},
		{Name: "health", Type: "object", Description: "Default health check configuration.", Children: healthFields()},
		{Name: "coldStorage", Type: "object", Description: "Default cold storage archival configuration.", Children: coldStorageFields()},
		{Name: "tls", Type: "object", Description: "Default TLS configuration for gRPC connections.", Children: tlsFields()},
		{Name: "responseSigning", Type: "object", Description: "Default Ed25519 response signing.", Children: responseSigningFields()},
		{Name: "monitoring", Type: "object", Description: "Default OpenTelemetry monitoring.", Children: monitoringFields()},
	}
}
