package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// LedgerService is the Schema for the ledgers API.
type LedgerService struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LedgerServiceSpec   `json:"spec,omitempty"`
	Status LedgerServiceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LedgerServiceList contains a list of LedgerService.
type LedgerServiceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []LedgerService `json:"items"`
}

// LedgerServiceSpec defines the desired state of LedgerService.
type LedgerServiceSpec struct {
	// DefaultsRef is the name of a cluster-scoped LedgerDefaults resource
	// whose values are used as defaults for this LedgerService. LedgerService-level values
	// take precedence over defaults.
	// +optional
	DefaultsRef string `json:"defaultsRef,omitempty"`

	// Image configuration for the ledger container.
	// +optional
	Image ImageSpec `json:"image,omitempty"`

	// ImagePullSecrets for private registries.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Replicas is the number of Raft nodes.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// ServiceAccount configuration.
	// +optional
	ServiceAccount ServiceAccountSpec `json:"serviceAccount,omitempty"`

	// PodSecurityContext for the pod.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// SecurityContext for the container.
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`

	// Config holds the application configuration.
	// +optional
	Config LedgerServiceConfig `json:"config,omitempty"`

	// Service configuration for the ClusterIP service.
	// +optional
	Service ServiceSpec `json:"service,omitempty"`

	// HeadlessService configuration for Raft peer discovery.
	// +optional
	HeadlessService HeadlessServiceSpec `json:"headlessService,omitempty"`

	// Ingress configuration for HTTP access.
	// +optional
	Ingress *IngressSpec `json:"ingress,omitempty"`

	// AutoIngress automatically creates an Ingress from the LedgerService name.
	// The generated host is: <service-name><suffix>.<tld>
	// +optional
	AutoIngress *AutoIngressSpec `json:"autoIngress,omitempty"`

	// IngressGrpc configuration for gRPC access.
	// +optional
	IngressGrpc *IngressGrpcSpec `json:"ingressGrpc,omitempty"`

	// Persistence configuration for WAL and data volumes.
	// +optional
	Persistence PersistenceSpec `json:"persistence,omitempty"`

	// Resources for the ledger container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// PodAnnotations to add to each pod.
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// NodeSelector for pod scheduling.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations for pod scheduling.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Affinity rules for pod scheduling.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// PodAntiAffinity configuration.
	// +optional
	PodAntiAffinity *PodAntiAffinitySpec `json:"podAntiAffinity,omitempty"`

	// PodDisruptionBudget configuration.
	// +optional
	PodDisruptionBudget *PodDisruptionBudgetSpec `json:"podDisruptionBudget,omitempty"`

	// ServiceMonitor configuration for Prometheus.
	// +optional
	ServiceMonitor *ServiceMonitorSpec `json:"serviceMonitor,omitempty"`

	// NetworkPolicy configuration for egress restrictions.
	// +optional
	NetworkPolicy *NetworkPolicySpec `json:"networkPolicy,omitempty"`

	// DNSEndpoint configuration for ExternalDNS.
	// +optional
	DNSEndpoint *DNSEndpointSpec `json:"dnsEndpoint,omitempty"`

	// AutoDNSEndpoint automatically creates a DNSEndpoint from the LedgerService name.
	// +optional
	AutoDNSEndpoint *AutoDNSEndpointSpec `json:"autoDNSEndpoint,omitempty"`
}

// ImageSpec defines container image configuration.
// Defaults are applied by the controller (not the API server) so that
// LedgerDefaults inheritance works correctly.
type ImageSpec struct {
	// Repository is the container image repository.
	// +optional
	Repository string `json:"repository,omitempty"`

	// Tag is the container image tag.
	// +optional
	Tag string `json:"tag,omitempty"`

	// PullPolicy for the container image.
	// +optional
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

// ServiceAccountSpec defines service account configuration.
// Defaults are applied by the controller so that LedgerDefaults inheritance works.
type ServiceAccountSpec struct {
	// Create specifies whether to create a service account.
	// +optional
	Create *bool `json:"create,omitempty"`

	// Annotations to add to the service account.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// Name overrides the service account name.
	// +optional
	Name string `json:"name,omitempty"`
}

// LedgerServiceConfig holds application configuration mapped to environment variables.
type LedgerServiceConfig struct {
	// ClusterID for inter-node communication validation.
	// +kubebuilder:default="default"
	// +optional
	ClusterID string `json:"clusterID,omitempty"`

	// BindAddr is the Raft transport bind address.
	// +kubebuilder:default="0.0.0.0:7777"
	// +optional
	BindAddr string `json:"bindAddr,omitempty"`

	// GrpcPort is the gRPC service port.
	// +kubebuilder:default=8888
	// +optional
	GrpcPort int32 `json:"grpcPort,omitempty"`

	// HttpPort is the HTTP service port.
	// +kubebuilder:default=9000
	// +optional
	HttpPort int32 `json:"httpPort,omitempty"`

	// WalDir is the WAL data directory.
	// +kubebuilder:default="/data/raft"
	// +optional
	WalDir string `json:"walDir,omitempty"`

	// DataDir is the application data directory.
	// +kubebuilder:default="/data/app"
	// +optional
	DataDir string `json:"dataDir,omitempty"`

	// Debug enables debug logging.
	// +optional
	Debug bool `json:"debug,omitempty"`

	// Restore starts the server in restore mode.
	// +optional
	Restore bool `json:"restore,omitempty"`

	// Pebble storage engine configuration.
	// +optional
	Pebble *PebbleConfig `json:"pebble,omitempty"`

	// Raft consensus configuration.
	// +optional
	Raft *RaftConfig `json:"raft,omitempty"`

	// Cache configuration.
	// +optional
	Cache *CacheConfig `json:"cache,omitempty"`

	// Health check configuration.
	// +optional
	Health *HealthConfig `json:"health,omitempty"`

	// Audit log configuration.
	// +optional
	Audit *AuditConfig `json:"audit,omitempty"`

	// AdmissionMetrics enables admission path metrics.
	// +optional
	AdmissionMetrics *bool `json:"admissionMetrics,omitempty"`

	// ColdStorage configuration for period archival.
	// +optional
	ColdStorage *ColdStorageConfig `json:"coldStorage,omitempty"`

	// TLS configuration for gRPC connections.
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`

	// ResponseSigning configuration (Ed25519).
	// +optional
	ResponseSigning *ResponseSigningConfig `json:"responseSigning,omitempty"`

	// Monitoring configuration (OpenTelemetry).
	// +optional
	Monitoring *MonitoringConfig `json:"monitoring,omitempty"`

	// Auth holds authentication and authorization configuration.
	// +optional
	Auth *AuthorizationConfig `json:"auth,omitempty"`

	// ReadIndex configuration for the bbolt read index store.
	// +optional
	ReadIndex *ReadIndexConfig `json:"readIndex,omitempty"`
}

// ReadIndexConfig holds bbolt read index configuration.
type ReadIndexConfig struct {
	// NoFreelistSync skips freelist serialization on commit.
	// Enables faster writes at the cost of slower database reopen.
	// +optional
	NoFreelistSync *bool `json:"noFreelistSync,omitempty"`

	// BatchSize is the number of log entries per bbolt write transaction.
	// Higher values amortize commit overhead but use more memory.
	// Default: 1000.
	// +optional
	BatchSize *int32 `json:"batchSize,omitempty"`

	// FreelistSyncInterval is the periodic interval at which the freelist
	// is synced to disk when NoFreelistSync is enabled. This limits data
	// loss on crash to at most one interval of freelist rebuild time.
	// Default: 5m. Set to "0" to disable periodic sync.
	// +optional
	FreelistSyncInterval *string `json:"freelistSyncInterval,omitempty"`

	// InitialMmapSize is the initial mmap size for the bbolt database in bytes.
	// Pre-allocating virtual address space prevents mmap stalls as the DB grows.
	// Default: 1073741824 (1 GiB).
	// +optional
	InitialMmapSize *int64 `json:"initialMmapSize,omitempty"`
}

// AuthorizationConfig holds authentication and authorization configuration.
type AuthorizationConfig struct {
	// Enabled enables authentication.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Issuer is the OIDC token issuer URL.
	// +optional
	Issuer string `json:"issuer,omitempty"`

	// Service is the scope prefix (e.g. "ledger" for "ledger:read").
	// +optional
	Service string `json:"service,omitempty"`

	// ScopeMapping maps virtual scopes (e.g. "ledger:read") to granular scopes.
	// When provided, overrides the default mapping and --auth-service is ignored for scope resolution.
	// +optional
	ScopeMapping map[string][]string `json:"scopeMapping,omitempty"`
}

// PebbleConfig holds Pebble storage engine configuration.
type PebbleConfig struct {
	// MemTableSize in bytes.
	// +optional
	MemTableSize *int64 `json:"memTableSize,omitempty"`

	// MemTableStopWritesThreshold is the number of memtables before writes stop.
	// +optional
	MemTableStopWritesThreshold *int32 `json:"memTableStopWritesThreshold,omitempty"`

	// L0CompactionThreshold is the L0 file count to trigger compaction.
	// +optional
	L0CompactionThreshold *int32 `json:"l0CompactionThreshold,omitempty"`

	// L0StopWritesThreshold is the L0 file count before writes stop.
	// +optional
	L0StopWritesThreshold *int32 `json:"l0StopWritesThreshold,omitempty"`

	// LBaseMaxBytes is the maximum size of L1 in bytes.
	// +optional
	LBaseMaxBytes *int64 `json:"lBaseMaxBytes,omitempty"`

	// CacheSize is the block cache size in bytes.
	// +optional
	CacheSize *int64 `json:"cacheSize,omitempty"`

	// TargetFileSize is the target SST file size in bytes.
	// +optional
	TargetFileSize *int64 `json:"targetFileSize,omitempty"`

	// BytesPerSync is bytes written before sync during flush/compaction.
	// +optional
	BytesPerSync *int64 `json:"bytesPerSync,omitempty"`

	// WalBytesPerSync is WAL bytes written before sync.
	// +optional
	WalBytesPerSync *int64 `json:"walBytesPerSync,omitempty"`

	// MaxConcurrentCompactions is the maximum concurrent compactions.
	// +optional
	MaxConcurrentCompactions *int32 `json:"maxConcurrentCompactions,omitempty"`

	// WalMinSyncInterval is the minimum interval between WAL syncs.
	// +optional
	WalMinSyncInterval string `json:"walMinSyncInterval,omitempty"`

	// DisableWAL disables WAL entirely.
	// +optional
	DisableWAL *bool `json:"disableWAL,omitempty"`

	// IncrementalCompactThreshold is the number of new log entries before
	// triggering an incremental compaction of the new range.
	// Default: 100000.
	// +optional
	IncrementalCompactThreshold *int64 `json:"incrementalCompactThreshold,omitempty"`
}

// RaftConfig holds Raft consensus configuration.
type RaftConfig struct {
	// SnapshotThreshold is the number of logs before triggering a snapshot.
	// +optional
	SnapshotThreshold *int32 `json:"snapshotThreshold,omitempty"`

	// CompactionMargin is the compaction margin.
	// +optional
	CompactionMargin *int32 `json:"compactionMargin,omitempty"`

	// SnapshotInterval is the minimum interval between snapshots.
	// +optional
	SnapshotInterval string `json:"snapshotInterval,omitempty"`

	// ElectionTick is the election timeout in ticks.
	// +optional
	ElectionTick *int32 `json:"electionTick,omitempty"`

	// HeartbeatTick is the heartbeat interval in ticks.
	// +optional
	HeartbeatTick *int32 `json:"heartbeatTick,omitempty"`

	// MaxSizePerMsg is the maximum size per message in bytes.
	// +optional
	MaxSizePerMsg *int64 `json:"maxSizePerMsg,omitempty"`

	// MaxInflightMsgs is the maximum number of in-flight messages.
	// +optional
	MaxInflightMsgs *int32 `json:"maxInflightMsgs,omitempty"`

	// TickInterval is the interval between Raft ticks.
	// +optional
	TickInterval string `json:"tickInterval,omitempty"`

	// ProposeQueueCapacity is the capacity of the propose queue.
	// +optional
	ProposeQueueCapacity *int32 `json:"proposeQueueCapacity,omitempty"`

	// LearnerPromotionThreshold is the max log entry lag before auto-promoting a learner.
	// +optional
	LearnerPromotionThreshold *int32 `json:"learnerPromotionThreshold,omitempty"`

	// Transport queue configuration.
	// +optional
	Transport *RaftTransportConfig `json:"transport,omitempty"`
}

// RaftTransportConfig holds Raft transport queue configuration.
type RaftTransportConfig struct {
	// ReceptionQueues are the reception queue capacities per priority.
	// +optional
	ReceptionQueues []int32 `json:"receptionQueues,omitempty"`

	// SendQueues are the send queue capacities per priority.
	// +optional
	SendQueues []int32 `json:"sendQueues,omitempty"`
}

// CacheConfig holds cache configuration.
type CacheConfig struct {
	// RotationThreshold is the number of Raft log entries before rotating cache generations.
	// +optional
	RotationThreshold *int32 `json:"rotationThreshold,omitempty"`
}

// HealthConfig holds health check configuration.
type HealthConfig struct {
	// Interval between health checks.
	// +optional
	Interval string `json:"interval,omitempty"`

	// WalThreshold is the WAL volume usage threshold (0.0-1.0).
	// +optional
	WalThreshold string `json:"walThreshold,omitempty"`

	// DataThreshold is the data volume usage threshold (0.0-1.0).
	// +optional
	DataThreshold string `json:"dataThreshold,omitempty"`

	// ClockSkewThreshold is the maximum allowed clock skew between nodes.
	// +optional
	ClockSkewThreshold string `json:"clockSkewThreshold,omitempty"`
}

// AuditConfig holds audit log configuration.
type AuditConfig struct {
	// Enabled enables audit logging.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
}

// ColdStorageConfig holds cold storage configuration.
type ColdStorageConfig struct {
	// Driver is the storage driver: "filesystem" or "s3".
	// +optional
	Driver string `json:"driver,omitempty"`

	// Path is the base path for filesystem driver.
	// +optional
	Path string `json:"path,omitempty"`

	// BucketID is the shared namespace prefix for archives.
	// +optional
	BucketID string `json:"bucketId,omitempty"`

	// S3 configuration.
	// +optional
	S3 *S3Config `json:"s3,omitempty"`
}

// S3Config holds S3-specific cold storage configuration.
type S3Config struct {
	// Bucket is the S3 bucket name.
	// +optional
	Bucket string `json:"bucket,omitempty"`

	// Region is the AWS region.
	// +optional
	Region string `json:"region,omitempty"`

	// Endpoint is a custom S3 endpoint (for MinIO).
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
}

// TLSConfig holds TLS configuration for gRPC connections.
type TLSConfig struct {
	// Enabled enables TLS.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// SecretName is the Kubernetes secret containing TLS certificate and key.
	// +optional
	SecretName string `json:"secretName,omitempty"`

	// CASecretKey is the key for the CA certificate in the secret.
	// +optional
	CASecretKey string `json:"caSecretKey,omitempty"`
}

// ResponseSigningConfig holds Ed25519 response signing configuration.
type ResponseSigningConfig struct {
	// Enabled enables response signing.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// SecretName is the Kubernetes secret containing the Ed25519 seed.
	// +optional
	SecretName string `json:"secretName,omitempty"`

	// SecretKey is the key in the secret containing the seed.
	// +kubebuilder:default="seed"
	// +optional
	SecretKey string `json:"secretKey,omitempty"`
}

// MonitoringConfig holds OpenTelemetry monitoring configuration.
type MonitoringConfig struct {
	// ServiceName for monitoring.
	// +kubebuilder:default="ledger-v3-poc"
	// +optional
	ServiceName string `json:"serviceName,omitempty"`

	// Traces configuration.
	// +optional
	Traces *TracesConfig `json:"traces,omitempty"`

	// Metrics configuration.
	// +optional
	Metrics *MetricsConfig `json:"metrics,omitempty"`

	// Logs configuration.
	// +optional
	Logs *LogsConfig `json:"logs,omitempty"`

	// Attributes are additional OTEL resource attributes.
	// +optional
	Attributes string `json:"attributes,omitempty"`

	// Pyroscope continuous profiling configuration.
	// +optional
	Pyroscope *PyroscopeConfig `json:"pyroscope,omitempty"`
}

// TracesConfig holds trace configuration.
type TracesConfig struct {
	// Enabled enables tracing.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Exporter type (e.g., "otlp").
	// +optional
	Exporter string `json:"exporter,omitempty"`

	// Endpoint for the trace exporter.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Port for the trace exporter.
	// +optional
	Port string `json:"port,omitempty"`

	// Insecure disables TLS for the exporter.
	// +optional
	Insecure string `json:"insecure,omitempty"`

	// Mode is the exporter mode (e.g., "grpc").
	// +optional
	Mode string `json:"mode,omitempty"`

	// Batch enables batch mode.
	// +optional
	Batch string `json:"batch,omitempty"`

	// Sampling configuration.
	// +optional
	Sampling *TraceSamplingConfig `json:"sampling,omitempty"`
}

// TraceSamplingConfig holds trace sampling configuration.
type TraceSamplingConfig struct {
	// Enabled enables error-aware trace sampling.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// SuccessRatio is the sampling ratio for successful traces (0.0-1.0).
	// +optional
	SuccessRatio string `json:"successRatio,omitempty"`
}

// MetricsConfig holds metrics configuration.
type MetricsConfig struct {
	// Enabled enables metrics.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Exporter type.
	// +optional
	Exporter string `json:"exporter,omitempty"`

	// Endpoint for the metrics exporter.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Port for the metrics exporter.
	// +optional
	Port string `json:"port,omitempty"`

	// Insecure disables TLS for the exporter.
	// +optional
	Insecure string `json:"insecure,omitempty"`

	// Mode is the exporter mode.
	// +optional
	Mode string `json:"mode,omitempty"`

	// KeepInMemory keeps metrics in memory.
	// +optional
	KeepInMemory *bool `json:"keepInMemory,omitempty"`

	// ExporterPushInterval is the push interval.
	// +optional
	ExporterPushInterval string `json:"exporterPushInterval,omitempty"`

	// Runtime enables runtime metrics.
	// +optional
	Runtime *bool `json:"runtime,omitempty"`

	// RuntimeMinimumReadMemStatsInterval is the minimum interval for reading mem stats.
	// +optional
	RuntimeMinimumReadMemStatsInterval string `json:"runtimeMinimumReadMemStatsInterval,omitempty"`
}

// LogsConfig holds logs configuration.
type LogsConfig struct {
	// Enabled enables log exporting.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Level is the log level.
	// +optional
	Level string `json:"level,omitempty"`

	// Exporter type.
	// +optional
	Exporter string `json:"exporter,omitempty"`

	// Endpoint for the log exporter.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Port for the log exporter.
	// +optional
	Port string `json:"port,omitempty"`

	// Insecure disables TLS for the exporter.
	// +optional
	Insecure string `json:"insecure,omitempty"`

	// Mode is the exporter mode.
	// +optional
	Mode string `json:"mode,omitempty"`
}

// PyroscopeConfig holds Pyroscope continuous profiling configuration.
type PyroscopeConfig struct {
	// Enabled enables Pyroscope profiling.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ServerAddress is the Pyroscope server address.
	// +optional
	ServerAddress string `json:"serverAddress,omitempty"`

	// ApplicationName overrides the application name.
	// +optional
	ApplicationName string `json:"applicationName,omitempty"`

	// AuthToken for Pyroscope authentication.
	// +optional
	AuthToken string `json:"authToken,omitempty"`

	// TenantID for multi-tenant Pyroscope.
	// +optional
	TenantID string `json:"tenantId,omitempty"`

	// BasicAuthUser for basic authentication.
	// +optional
	BasicAuthUser string `json:"basicAuthUser,omitempty"`

	// BasicAuthPassword for basic authentication.
	// +optional
	BasicAuthPassword string `json:"basicAuthPassword,omitempty"`

	// UploadRate is the upload interval.
	// +optional
	UploadRate string `json:"uploadRate,omitempty"`

	// Tags in key=value,key2=value2 format.
	// +optional
	Tags string `json:"tags,omitempty"`

	// ProfileTypes to collect.
	// +optional
	ProfileTypes string `json:"profileTypes,omitempty"`

	// MutexProfileFraction is the mutex profile fraction.
	// +optional
	MutexProfileFraction *int32 `json:"mutexProfileFraction,omitempty"`

	// BlockProfileRate is the block profile rate.
	// +optional
	BlockProfileRate *int32 `json:"blockProfileRate,omitempty"`

	// DisableGCRuns disables GC runs.
	// +optional
	DisableGCRuns *bool `json:"disableGCRuns,omitempty"`
}

// ServiceSpec defines the ClusterIP service configuration.
type ServiceSpec struct {
	// Type is the service type.
	// +kubebuilder:default="ClusterIP"
	// +optional
	Type corev1.ServiceType `json:"type,omitempty"`

	// HttpPort is the HTTP service port.
	// +kubebuilder:default=9000
	// +optional
	HttpPort int32 `json:"httpPort,omitempty"`

	// GrpcPort is the gRPC service port.
	// +kubebuilder:default=8888
	// +optional
	GrpcPort int32 `json:"grpcPort,omitempty"`

	// RaftPort is the Raft transport port.
	// +kubebuilder:default=7777
	// +optional
	RaftPort int32 `json:"raftPort,omitempty"`

	// Annotations to add to the service.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// HeadlessServiceSpec defines the headless service configuration.
type HeadlessServiceSpec struct {
	// Enabled enables the headless service.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Annotations to add to the headless service.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// AutoIngressSpec automatically creates an Ingress from the LedgerService name.
// The generated host is: <service-name><suffix>.<tld>
type AutoIngressSpec struct {
	// Enabled enables automatic Ingress creation.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// TLD is the top-level domain (e.g., "example.com").
	TLD string `json:"tld"`

	// Suffix is appended to the service name before the TLD (e.g., "-ledger-v3").
	// +optional
	Suffix string `json:"suffix,omitempty"`

	// ClassName is the ingress class name.
	// +optional
	ClassName string `json:"className,omitempty"`

	// Annotations to add to the ingress.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// TLS configuration.
	// +optional
	TLS []IngressTLS `json:"tls,omitempty"`

	// Paths are the path rules. Defaults to a single "/" Prefix path.
	// +optional
	Paths []IngressPath `json:"paths,omitempty"`
}

// IngressSpec defines HTTP ingress configuration.
type IngressSpec struct {
	// Enabled enables the HTTP ingress.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ClassName is the ingress class name.
	// +optional
	ClassName string `json:"className,omitempty"`

	// Annotations to add to the ingress.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// Hosts configuration.
	// +optional
	Hosts []IngressHost `json:"hosts,omitempty"`

	// TLS configuration.
	// +optional
	TLS []IngressTLS `json:"tls,omitempty"`
}

// IngressGrpcSpec defines gRPC ingress configuration.
type IngressGrpcSpec struct {
	// Enabled enables the gRPC ingress.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ClassName is the ingress class name (e.g., "nginx", "traefik").
	// +optional
	ClassName string `json:"className,omitempty"`

	// Annotations to add to the ingress.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// Hosts configuration.
	// +optional
	Hosts []IngressHost `json:"hosts,omitempty"`

	// TLS configuration.
	// +optional
	TLS []IngressTLS `json:"tls,omitempty"`

	// TargetGroupBinding for AWS Load Balancer Controller.
	// +optional
	TargetGroupBinding *TargetGroupBindingSpec `json:"targetGroupBinding,omitempty"`
}

// IngressHost defines an ingress host rule.
type IngressHost struct {
	// Host is the hostname.
	Host string `json:"host"`

	// Paths are the path rules.
	// +optional
	Paths []IngressPath `json:"paths,omitempty"`
}

// IngressPath defines an ingress path rule.
type IngressPath struct {
	// Path is the URL path.
	// +kubebuilder:default="/"
	Path string `json:"path,omitempty"`

	// PathType is the path matching type.
	// +kubebuilder:default="Prefix"
	PathType string `json:"pathType,omitempty"`
}

// IngressTLS defines ingress TLS configuration.
type IngressTLS struct {
	// Hosts are the TLS hostnames.
	Hosts []string `json:"hosts,omitempty"`

	// SecretName is the TLS secret.
	SecretName string `json:"secretName,omitempty"`
}

// TargetGroupBindingSpec defines AWS TargetGroupBinding configuration.
type TargetGroupBindingSpec struct {
	// Enabled enables the TargetGroupBinding.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// TargetGroupARN is the ARN of the target group.
	// +optional
	TargetGroupARN string `json:"targetGroupARN,omitempty"`

	// TargetType is "instance" or "ip".
	// +kubebuilder:default="ip"
	// +optional
	TargetType string `json:"targetType,omitempty"`

	// Networking configuration (raw JSON).
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Networking *runtime.RawExtension `json:"networking,omitempty"`
}

// PersistenceSpec defines persistence configuration.
type PersistenceSpec struct {
	// WAL persistence configuration.
	// +optional
	WAL VolumeSpec `json:"wal,omitempty"`

	// Data persistence configuration.
	// +optional
	Data VolumeSpec `json:"data,omitempty"`

	// RetentionPolicy for PVCs.
	// +optional
	RetentionPolicy *RetentionPolicySpec `json:"retentionPolicy,omitempty"`
}

// VolumeSpec defines a volume claim configuration.
type VolumeSpec struct {
	// StorageClass for the PVC.
	// +optional
	StorageClass string `json:"storageClass,omitempty"`

	// AccessMode for the PVC.
	// +kubebuilder:default="ReadWriteOnce"
	// +optional
	AccessMode string `json:"accessMode,omitempty"`

	// Size of the volume.
	// +optional
	Size resource.Quantity `json:"size,omitempty"`
}

// RetentionPolicySpec defines PVC retention policy.
type RetentionPolicySpec struct {
	// WhenScaled policy.
	// +kubebuilder:default="Retain"
	// +optional
	WhenScaled string `json:"whenScaled,omitempty"`

	// WhenDeleted policy.
	// +kubebuilder:default="Retain"
	// +optional
	WhenDeleted string `json:"whenDeleted,omitempty"`
}

// PodAntiAffinitySpec defines pod anti-affinity configuration.
type PodAntiAffinitySpec struct {
	// Enabled enables pod anti-affinity.
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Type is "soft" or "hard".
	// +kubebuilder:default="soft"
	// +kubebuilder:validation:Enum=soft;hard
	// +optional
	Type string `json:"type,omitempty"`

	// Weight for soft anti-affinity (1-100).
	// +kubebuilder:default=100
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +optional
	Weight int32 `json:"weight,omitempty"`

	// TopologyKey for anti-affinity.
	// +kubebuilder:default="kubernetes.io/hostname"
	// +optional
	TopologyKey string `json:"topologyKey,omitempty"`
}

// PodDisruptionBudgetSpec defines PDB configuration.
type PodDisruptionBudgetSpec struct {
	// Enabled enables the PDB.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// MinAvailable is the minimum number of available pods.
	// +optional
	MinAvailable *int32 `json:"minAvailable,omitempty"`

	// MaxUnavailable is the maximum number of unavailable pods.
	// +optional
	MaxUnavailable *int32 `json:"maxUnavailable,omitempty"`
}

// NetworkPolicySpec defines egress NetworkPolicy configuration.
type NetworkPolicySpec struct {
	// Enabled enables the egress NetworkPolicy.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ExternalCIDRExcept overrides the default RFC1918 CIDR blocks excluded from external egress.
	// Defaults to [10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16].
	// +optional
	ExternalCIDRExcept []string `json:"externalCIDRExcept,omitempty"`
}

// ServiceMonitorSpec defines Prometheus ServiceMonitor configuration.
type ServiceMonitorSpec struct {
	// Enabled enables the ServiceMonitor.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Interval is the scrape interval.
	// +optional
	Interval string `json:"interval,omitempty"`

	// ScrapeTimeout is the scrape timeout.
	// +optional
	ScrapeTimeout string `json:"scrapeTimeout,omitempty"`

	// Labels to add to the ServiceMonitor.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Relabelings for the ServiceMonitor (raw JSON array).
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Relabelings []runtime.RawExtension `json:"relabelings,omitempty"`

	// MetricRelabelings for the ServiceMonitor (raw JSON array).
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	MetricRelabelings []runtime.RawExtension `json:"metricRelabelings,omitempty"`
}

// AutoDNSEndpointSpec automatically creates a DNSEndpoint from the LedgerService name.
// The generated dnsName is: <service-name><suffix>.<tld>
type AutoDNSEndpointSpec struct {
	// Enabled enables automatic DNSEndpoint creation.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// TLD is the top-level domain (e.g., "example.com").
	TLD string `json:"tld"`

	// Suffix is appended to the service name before the TLD (e.g., "-ledger-v3").
	// +optional
	Suffix string `json:"suffix,omitempty"`

	// RecordType is the DNS record type (e.g., CNAME, A). Defaults to CNAME.
	// +optional
	RecordType string `json:"recordType,omitempty"`

	// Targets is the list of target hostnames or IPs.
	Targets []string `json:"targets"`

	// RecordTTL is the TTL in seconds for the DNS record.
	// +optional
	RecordTTL *int64 `json:"recordTTL,omitempty"`

	// Annotations to add to the DNSEndpoint resource.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// ProviderSpecific holds provider-specific properties.
	// +optional
	ProviderSpecific []ProviderSpecificProperty `json:"providerSpecific,omitempty"`
}

// DNSEndpointSpec defines ExternalDNS DNSEndpoint configuration.
type DNSEndpointSpec struct {
	// Enabled enables the DNSEndpoint resource.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Annotations to add to the DNSEndpoint resource.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// Endpoints is the list of DNS endpoint entries.
	// +optional
	Endpoints []DNSEndpointEntry `json:"endpoints,omitempty"`
}

// DNSEndpointEntry defines a single DNS endpoint.
type DNSEndpointEntry struct {
	// DNSName is the hostname for the DNS record.
	DNSName string `json:"dnsName"`

	// RecordType is the DNS record type (e.g., CNAME, A). Defaults to CNAME.
	// +optional
	RecordType string `json:"recordType,omitempty"`

	// Targets is the list of target hostnames or IPs.
	Targets []string `json:"targets"`

	// RecordTTL is the TTL in seconds for the DNS record.
	// +optional
	RecordTTL *int64 `json:"recordTTL,omitempty"`

	// ProviderSpecific holds provider-specific properties.
	// +optional
	ProviderSpecific []ProviderSpecificProperty `json:"providerSpecific,omitempty"`
}

// ProviderSpecificProperty defines a provider-specific key-value pair.
type ProviderSpecificProperty struct {
	// Name is the property name.
	Name string `json:"name"`

	// Value is the property value.
	Value string `json:"value"`
}

// LedgerServiceStatus defines the observed state of LedgerService.
type LedgerServiceStatus struct {
	// Phase of the LedgerService: Pending, Running, Degraded.
	// +optional
	Phase string `json:"phase,omitempty"`

	// ReadyReplicas is the number of ready pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// ObservedGeneration is the generation last observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

func init() {
	SchemeBuilder.Register(&LedgerService{}, &LedgerServiceList{})
}
