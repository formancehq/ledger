package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	// ClusterID for inter-node communication validation.
	// +kubebuilder:default="default"
	// +kubebuilder:validation:XValidation:rule="oldSelf == '' || self == oldSelf",message="clusterID is immutable once set"
	// +optional
	ClusterID string `json:"clusterID,omitempty" ledger:"immutable"`

	// BindAddr is the Raft transport bind address.
	// +kubebuilder:default="0.0.0.0:7777"
	// +kubebuilder:validation:XValidation:rule="oldSelf == '' || self == oldSelf",message="bindAddr is immutable once set"
	// +optional
	BindAddr string `json:"bindAddr,omitempty" ledger:"immutable"`

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
	// +kubebuilder:validation:XValidation:rule="oldSelf == '' || self == oldSelf",message="walDir is immutable once set"
	// +optional
	WalDir string `json:"walDir,omitempty" ledger:"immutable"`

	// DataDir is the application data directory.
	// +kubebuilder:default="/data/app"
	// +kubebuilder:validation:XValidation:rule="oldSelf == '' || self == oldSelf",message="dataDir is immutable once set"
	// +optional
	DataDir string `json:"dataDir,omitempty" ledger:"immutable"`

	// Debug enables debug logging. Equivalent to LogLevel="debug".
	// LogLevel takes precedence when both are set; prefer LogLevel for new
	// manifests since it also unlocks the trace level.
	// +optional
	Debug bool `json:"debug,omitempty"`

	// LogLevel sets the server's log verbosity. One of trace|debug|info|error.
	// Trace records are stdout-only and never exported via OTLP.
	// When set, takes precedence over Debug.
	// +kubebuilder:validation:Enum=trace;debug;info;error
	// +optional
	LogLevel string `json:"logLevel,omitempty"`

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

	// AdmissionMetrics enables admission path metrics.
	// +optional
	AdmissionMetrics *bool `json:"admissionMetrics,omitempty"`

	// MetricsNaming selects the convention for metric names emitted
	// by the server: "otel" (the default, dot-notation) preserves
	// the OpenTelemetry instrument names; "prom" rewrites every
	// metric the server emits with a `ledger_` prefix and dots
	// converted to underscores so the names are unambiguous after
	// an OTLP→Prometheus collector that sanitises dots. OTel
	// semantic-convention auto-instrumentation (`go.*`, `process.*`,
	// `system.*`, `http.*`) uses the global MeterProvider and is
	// never touched by this flag.
	// +kubebuilder:validation:Enum=otel;prom
	// +optional
	MetricsNaming string `json:"metricsNaming,omitempty"`

	// SentinelMode enables runtime volume consistency assertions
	// (monotonicity, delta/posting cross-check, post-commit cache/Pebble verification).
	// +optional
	SentinelMode *bool `json:"sentinelMode,omitempty"`

	// GrpcCompression enables gzip compression on gRPC calls.
	// +optional
	GrpcCompression *bool `json:"grpcCompression,omitempty"`

	// QueryProfileThreshold logs and emits OTel attributes for queries exceeding this duration (0 to disable).
	// +optional
	QueryProfileThreshold string `json:"queryProfileThreshold,omitempty"`

	// GrpcSlowThreshold is the duration above which a gRPC call is logged as slow.
	// +optional
	GrpcSlowThreshold string `json:"grpcSlowThreshold,omitempty"`

	// NumscriptCacheSize is the maximum number of parsed Numscript programs to cache (LRU eviction).
	// +optional
	NumscriptCacheSize *int32 `json:"numscriptCacheSize,omitempty"`

	// MirrorMaxBatchSize is the maximum allowed batch size for mirror sync.
	// +optional
	MirrorMaxBatchSize *int32 `json:"mirrorMaxBatchSize,omitempty"`

	// MaxExecutionPlanSize caps the number of AttributePlan entries an
	// ExecutionPlan may carry. Admission rejects proposals beyond this
	// (0 = unlimited). Default: 4096.
	// +optional
	MaxExecutionPlanSize *int32 `json:"maxExecutionPlanSize,omitempty"`

	// IdempotencyTTL is the time-to-live for idempotency keys (0 = never expire).
	// Default: 24h.
	// +optional
	IdempotencyTTL string `json:"idempotencyTTL,omitempty"`

	// IdempotencyEvictionInterval is how often the leader proposes idempotency eviction.
	// Default: 60s.
	// +optional
	IdempotencyEvictionInterval string `json:"idempotencyEvictionInterval,omitempty"`

	// HashAlgorithm selects the hash algorithm for the log chain.
	// Supported values: "blake3" (cryptographic, default) or "xxh3" (non-cryptographic, faster).
	// +kubebuilder:validation:Enum=blake3;xxh3
	// +optional
	HashAlgorithm string `json:"hashAlgorithm,omitempty"`

	// UnsafeSkipConfigValidation skips startup configuration safety checks.
	// DANGEROUS: allows node-id/cluster-id changes on existing data.
	// +optional
	UnsafeSkipConfigValidation *bool `json:"unsafeSkipConfigValidation,omitempty"`

	// FSMDeterminismEnabled opts the cluster into deterministic FSM byte
	// encoding and the cross-node rolling digest under SubGlobFSMDigest.
	// Designed as an observability/diagnostic safety net for the first
	// production deployments: peers can exchange a digest via the
	// GetFSMDigest RPC and detect FSM divergence.
	//
	// IMMUTABLE post-bootstrap: set once at first boot, validated on every
	// subsequent boot. A mismatch is fatal and NOT bypassable by
	// UnsafeSkipConfigValidation — flipping the flag would either re-encode
	// existing entries non-deterministically (ON→OFF) or compare new
	// deterministic entries against pre-existing non-deterministic ones
	// (OFF→ON), tripping the very digest it powers. To change it on an
	// existing cluster, the data directory must be wiped first.
	//
	// The audit hash chain remains the only source of cryptographic
	// authenticity; the digest is a diagnostic signal.
	// +optional
	FSMDeterminismEnabled *bool `json:"fsmDeterminismEnabled,omitempty"`

	// Snapshot sync configuration for Raft snapshot transfers.
	// +optional
	Snapshot *SnapshotConfig `json:"snapshot,omitempty"`

	// ReceiptSigning configures HMAC signing for JWT transaction receipts.
	// +optional
	ReceiptSigning *ReceiptSigningConfig `json:"receiptSigning,omitempty"`

	// Bloom filter configuration per attribute type.
	// +optional
	Bloom *BloomConfig `json:"bloom,omitempty"`

	// ColdStorage configuration for chapter archival.
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

	// ReadIndex configuration for the Pebble read index store.
	// +optional
	ReadIndex *ReadIndexConfig `json:"readIndex,omitempty"`

	// Service configuration for the ClusterIP service.
	// +optional
	Service ServiceSpec `json:"service,omitempty"`

	// HeadlessService configuration for Raft peer discovery.
	// +optional
	HeadlessService HeadlessServiceSpec `json:"headlessService,omitempty"`

	// Ingress configuration for HTTP access.
	// +optional
	Ingress *IngressSpec `json:"ingress,omitempty"`

	// IngressGrpc configuration for gRPC access.
	// +optional
	IngressGrpc *IngressGrpcSpec `json:"ingressGrpc,omitempty"`

	// Persistence configuration for WAL and data volumes.
	// +optional
	Persistence PersistenceSpec `json:"persistence,omitempty"`

	// Resources for the ledger container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// GoMemLimitRatio is the percentage of memory limit used for GOMEMLIMIT (0-100).
	// Defaults to 90 if not set.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	GoMemLimitRatio *int32 `json:"goMemLimitRatio,omitempty"`

	// ExtraEnv is a list of additional environment variables to inject into ledger containers.
	// +optional
	ExtraEnv []corev1.EnvVar `json:"extraEnv,omitempty"`

	// PodAnnotations to add to each pod.
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// AdditionalLabels are merged on top of the default selector labels
	// (app.kubernetes.io/name, app.kubernetes.io/instance) on every owned
	// resource AND on the pod template / Service selectors. Keys that collide
	// with a default override it; the app.kubernetes.io/managed-by ownership
	// label is dropped from the merge and stays operator-owned everywhere
	// (top-level objects AND pods).
	//
	// Use this to escape an unrelated Service whose selector accidentally
	// matches our pods. Selector fields on Service / StatefulSet are immutable
	// after creation: changing this field on an existing cluster will be
	// rejected by the operator (a SelectorImmutable=False condition is set).
	// +optional
	AdditionalLabels map[string]string `json:"additionalLabels,omitempty"`

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

	// TopologySpreadConstraints describe how pods are spread across failure
	// domains (e.g. zones, nodes). See PodSpec.topologySpreadConstraints. The
	// pod label selector defaults to the LedgerService's selector when omitted.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// NetworkPolicy configuration for egress restrictions.
	// +optional
	NetworkPolicy *NetworkPolicySpec `json:"networkPolicy,omitempty"`

	// DNSEndpoint configuration for ExternalDNS.
	// +optional
	DNSEndpoint *DNSEndpointSpec `json:"dnsEndpoint,omitempty"`

	// LivenessProbe overrides the default liveness probe for the ledger container.
	// +optional
	LivenessProbe *corev1.Probe `json:"livenessProbe,omitempty"`

	// ReadinessProbe overrides the default readiness probe for the ledger container.
	// +optional
	ReadinessProbe *corev1.Probe `json:"readinessProbe,omitempty"`

	// StartupProbe overrides the default startup probe for the ledger container.
	// +optional
	StartupProbe *corev1.Probe `json:"startupProbe,omitempty"`
}

// ImageSpec defines container image configuration.
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

// ReadIndexConfig holds Pebble read index configuration.
type ReadIndexConfig struct {
	// BatchSize is the number of log entries per Pebble batch commit.
	// Higher values amortize commit overhead but use more memory.
	// Default: 1000.
	// +optional
	BatchSize *int32 `json:"batchSize,omitempty"`

	// Pebble holds the common Pebble tunables for the read index.
	// Uses the same knobs as the primary store (cache size, memtable, L0 thresholds, etc.).
	// +optional
	Pebble *PebbleConfig `json:"pebble,omitempty"`
}

// AuthorizationConfig holds authentication and authorization configuration.
type AuthorizationConfig struct {
	// Enabled enables authentication.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Issuer is the OIDC token issuer URL (for backward compatibility).
	// +optional
	Issuer string `json:"issuer,omitempty"`

	// Issuers is a list of trusted OIDC issuers.
	// +optional
	Issuers []string `json:"issuers,omitempty"`

	// Service is the scope prefix (e.g. "ledger" for "ledger:read").
	// +optional
	Service string `json:"service,omitempty"`

	// CheckScopes enables scope checking.
	// +optional
	CheckScopes *bool `json:"checkScopes,omitempty"`

	// ReadKeySetMaxRetries is the maximum number of retries for reading key sets.
	// +optional
	ReadKeySetMaxRetries *int32 `json:"readKeySetMaxRetries,omitempty"`

	// ScopeMapping maps virtual scopes (e.g. "ledger:read") to granular scopes.
	// When provided, overrides the default mapping and --auth-service is ignored for scope resolution.
	// +optional
	ScopeMapping map[string][]string `json:"scopeMapping,omitempty"`

	// AnonymousScopes lists the granular scopes granted to requests that arrive
	// without a bearer token. Wildcards "*:read" / "*:write" expand to every
	// granular scope with that suffix. The canonical writes-only configuration
	// is `["*:read"]`: reads are public, writes still require a valid token.
	// Empty (default) preserves the strict behavior — every request must
	// authenticate. Invalid tokens are still rejected with 401 regardless of
	// this setting; only the *absence* of a token triggers the fallback.
	// +optional
	AnonymousScopes []string `json:"anonymousScopes,omitempty"`
}

// PebbleConfig holds Pebble storage engine configuration.
type PebbleConfig struct {
	// MemTableSize is the MemTable size (e.g. "256Mi", "1Gi").
	// Accepts Kubernetes quantity format.
	// +optional
	MemTableSize *resource.Quantity `json:"memTableSize,omitempty"`

	// MemTableStopWritesThreshold is the number of memtables before writes stop.
	// +optional
	MemTableStopWritesThreshold *int32 `json:"memTableStopWritesThreshold,omitempty"`

	// L0CompactionThreshold is the L0 file count to trigger compaction.
	// +optional
	L0CompactionThreshold *int32 `json:"l0CompactionThreshold,omitempty"`

	// L0StopWritesThreshold is the L0 file count before writes stop.
	// +optional
	L0StopWritesThreshold *int32 `json:"l0StopWritesThreshold,omitempty"`

	// LBaseMaxBytes is the maximum size of L1 (e.g. "64Mi", "256Mi").
	// Accepts Kubernetes quantity format.
	// +optional
	LBaseMaxBytes *resource.Quantity `json:"lBaseMaxBytes,omitempty"`

	// CacheSize is the block cache size (e.g. "1Gi", "512Mi").
	// Accepts Kubernetes quantity format.
	// +optional
	CacheSize *resource.Quantity `json:"cacheSize,omitempty"`

	// TargetFileSize is the target SST file size (e.g. "64Mi").
	// Accepts Kubernetes quantity format.
	// +optional
	TargetFileSize *resource.Quantity `json:"targetFileSize,omitempty"`

	// BytesPerSync is bytes written before sync during flush/compaction (e.g. "512Ki").
	// Accepts Kubernetes quantity format.
	// +optional
	BytesPerSync *resource.Quantity `json:"bytesPerSync,omitempty"`

	// WalBytesPerSync is WAL bytes written before sync (e.g. "512Ki").
	// Accepts Kubernetes quantity format.
	// +optional
	WalBytesPerSync *resource.Quantity `json:"walBytesPerSync,omitempty"`

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

	// MaxCheckpoints is the maximum number of Pebble checkpoints to keep.
	// Default: 10.
	// +optional
	MaxCheckpoints *int32 `json:"maxCheckpoints,omitempty"`

	// Compression is the per-level compression algorithm (L0-L6, comma-separated).
	// Options: none, snappy, zstd, fastest, fast, balanced, good, default.
	// +optional
	Compression string `json:"compression,omitempty"`

	// ValueSeparation configuration for large value storage in blob files.
	// +optional
	ValueSeparation *PebbleValueSeparationConfig `json:"valueSeparation,omitempty"`
}

// PebbleValueSeparationConfig holds Pebble value separation configuration.
type PebbleValueSeparationConfig struct {
	// Enabled enables value separation (large values stored in blob files).
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// MinSize is the minimum value size for separation (e.g. "256", "1Ki").
	// Accepts Kubernetes quantity format. Default: 256.
	// +optional
	MinSize *resource.Quantity `json:"minSize,omitempty"`

	// MaxDepth is the max blob reference depth per SSTable.
	// Default: 4.
	// +optional
	MaxDepth *int32 `json:"maxDepth,omitempty"`

	// RewriteAge is the minimum blob file age before rewrite.
	// Default: 1h.
	// +optional
	RewriteAge string `json:"rewriteAge,omitempty"`

	// GarbageRatio is the blob garbage ratio before rewrite (0.0-1.0).
	// Default: 0.20.
	// +optional
	GarbageRatio string `json:"garbageRatio,omitempty"`
}

// RaftConfig holds Raft consensus configuration.
type RaftConfig struct {
	// CompactionMargin is the compaction margin.
	// +optional
	CompactionMargin *int32 `json:"compactionMargin,omitempty"`

	// MaintenanceInterval is the interval for background WAL snapshot + Pebble checkpoint.
	// Default: 30s.
	// +optional
	MaintenanceInterval string `json:"maintenanceInterval,omitempty"`

	// ElectionTick is the election timeout in ticks.
	// +optional
	ElectionTick *int32 `json:"electionTick,omitempty"`

	// HeartbeatTick is the heartbeat interval in ticks.
	// +optional
	HeartbeatTick *int32 `json:"heartbeatTick,omitempty"`

	// MaxSizePerMsg is the maximum size per message (e.g. "1Mi", "4Mi").
	// Accepts Kubernetes quantity format.
	// +optional
	MaxSizePerMsg *resource.Quantity `json:"maxSizePerMsg,omitempty"`

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

	// ReplayBatchSize is the number of Raft log entries replayed per batch on startup.
	// +optional
	ReplayBatchSize *int32 `json:"replayBatchSize,omitempty"`

	// ProcessingTickInterval is the interval for processing committed entries.
	// Default: tickInterval/10.
	// +optional
	ProcessingTickInterval string `json:"processingTickInterval,omitempty"`

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

	// BufferSize is the per-peer send buffer capacity (e.g. "20Mi").
	// Accepts Kubernetes quantity format.
	// +optional
	BufferSize *resource.Quantity `json:"bufferSize,omitempty"`
}

// SnapshotConfig holds Raft snapshot sync configuration.
type SnapshotConfig struct {
	// SessionTTL is the server-side session TTL for snapshot sync.
	// Default: 5m.
	// +optional
	SessionTTL string `json:"sessionTTL,omitempty"`

	// Parallelism is the number of parallel file fetch workers during snapshot sync.
	// Default: 4.
	// +optional
	Parallelism *int32 `json:"parallelism,omitempty"`

	// RetryCount is the session-level retry attempts for snapshot sync on transient errors.
	// Default: 5.
	// +optional
	RetryCount *int32 `json:"retryCount,omitempty"`

	// FileRetryCount is the per-file retry attempts during snapshot sync on transient stream errors.
	// Default: 3.
	// +optional
	FileRetryCount *int32 `json:"fileRetryCount,omitempty"`
}

// CacheConfig holds cache configuration.
type CacheConfig struct {
	// RotationThreshold is the number of Raft log entries before rotating cache generations.
	// Changes trigger a rolling restart; convergence is deterministic via Raft (applyClusterConfig).
	// +kubebuilder:validation:Minimum=1
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

	// WalResumeThreshold is the WAL usage resume (low-water) threshold for
	// disk-usage hysteresis. Must be < WalThreshold. Maps to HEALTH_WAL_RESUME_THRESHOLD.
	// +optional
	WalResumeThreshold string `json:"walResumeThreshold,omitempty"`

	// DataResumeThreshold is the data usage resume (low-water) threshold for
	// disk-usage hysteresis. Must be < DataThreshold. Maps to HEALTH_DATA_RESUME_THRESHOLD.
	// +optional
	DataResumeThreshold string `json:"dataResumeThreshold,omitempty"`

	// ClockSkewThreshold is the maximum allowed clock skew between nodes.
	// +optional
	ClockSkewThreshold string `json:"clockSkewThreshold,omitempty"`
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
	// +kubebuilder:default="ledger"
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

	// FlightRecorder configuration for runtime execution trace buffering.
	// +optional
	FlightRecorder *FlightRecorderConfig `json:"flightRecorder,omitempty"`
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

// FlightRecorderConfig holds runtime flight recorder configuration.
type FlightRecorderConfig struct {
	// Enabled enables the runtime flight recorder.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// MinAge is the minimum duration of trace data retained in the buffer.
	// +optional
	MinAge string `json:"minAge,omitempty"`

	// MaxBytes is the maximum memory for the flight recorder buffer (e.g. "10Mi").
	// Accepts Kubernetes quantity format.
	// +optional
	MaxBytes *resource.Quantity `json:"maxBytes,omitempty"`
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

// IngressSpec defines HTTP ingress configuration.
// +kubebuilder:validation:XValidation:rule="!self.enabled || size(self.hosts) > 0",message="hosts are required when ingress is enabled"
type IngressSpec struct {
	// Enabled enables the HTTP ingress.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ClassName is the ingress class name.
	// +optional
	ClassName string `json:"className,omitempty"`

	// Labels to add to the ingress.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

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
// +kubebuilder:validation:XValidation:rule="!self.enabled || size(self.hosts) > 0",message="hosts are required when gRPC ingress is enabled"
type IngressGrpcSpec struct {
	// Enabled enables the gRPC ingress.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ClassName is the ingress class name (e.g., "nginx", "traefik").
	// +optional
	ClassName string `json:"className,omitempty"`

	// Labels to add to the ingress.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations to add to the ingress.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// Hosts configuration.
	// +optional
	Hosts []IngressHost `json:"hosts,omitempty"`

	// TLS configuration.
	// +optional
	TLS []IngressTLS `json:"tls,omitempty"`

	// ServiceAnnotations to add to the backing gRPC Kubernetes Service.
	// +optional
	ServiceAnnotations map[string]string `json:"serviceAnnotations,omitempty"`
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

// PersistenceSpec defines persistence configuration.
type PersistenceSpec struct {
	// WAL persistence configuration.
	// +optional
	WAL VolumeSpec `json:"wal,omitempty"`

	// Data persistence configuration.
	// +optional
	Data VolumeSpec `json:"data,omitempty"`

	// ColdCache persistence configuration for cold storage read cache.
	// Uses a separate volume to avoid filling the data disk when reading archived chapters.
	// +optional
	ColdCache VolumeSpec `json:"coldCache,omitempty"`

	// RetentionPolicy for PVCs.
	// +optional
	RetentionPolicy *RetentionPolicySpec `json:"retentionPolicy,omitempty"`

	// DeletionProtection opts this ledger's PVCs and bound PVs into the
	// cluster-scoped volume deletion-protection admission policy. When true, the
	// operator stamps the `ledger.formance.com/deletion-protection: enabled`
	// label on the volumes so the policy selects them and rejects accidental
	// DELETEs unless the object carries the allow-deletion annotation; when set
	// back to false the label is removed and protection is lifted. The policy
	// itself must be installed cluster-wide by an admin via the Helm value
	// `pvcProtection.enabled` — otherwise this flag has no effect and the
	// operator emits a DeletionProtectionInactive warning on the CR.
	// +kubebuilder:default=false
	// +optional
	DeletionProtection bool `json:"deletionProtection,omitempty"`
}

// VolumeSpec defines a volume configuration.
// A volume is either PVC-backed (default) or hostPath-backed (for NVMe instance stores).
// When HostPath is set, no PersistentVolumeClaim is created; the pod mounts the
// host directory directly. Each pod gets an isolated subdirectory via its ordinal.
type VolumeSpec struct {
	// StorageClass for the PVC.
	// Mutually exclusive with hostPath.
	// +optional
	StorageClass string `json:"storageClass,omitempty"`

	// AccessMode for the PVC.
	// Mutually exclusive with hostPath.
	// +kubebuilder:default="ReadWriteOnce"
	// +optional
	AccessMode string `json:"accessMode,omitempty"`

	// Size of the volume.
	// +optional
	Size resource.Quantity `json:"size,omitempty"`

	// VolumeAttributesClassName is the name of the VolumeAttributesClass to use for the PVC.
	// Requires the VolumeAttributesClass feature gate to be enabled (beta in K8s 1.31+).
	// Mutually exclusive with hostPath.
	// +optional
	VolumeAttributesClassName string `json:"volumeAttributesClassName,omitempty"`

	// HostPath configures a host-local volume instead of a PVC.
	// When set, no PersistentVolumeClaim is created for this volume.
	// The pod uses a hostPath volume mounted from the specified path on the node.
	// Each pod gets an isolated subdirectory named after its ordinal index.
	// This is intended for NVMe instance stores where Raft replication provides durability.
	// Mutually exclusive with storageClass, accessMode, and volumeAttributesClassName.
	// +optional
	HostPath *HostPathVolumeSpec `json:"hostPath,omitempty"`
}

// HostPathVolumeSpec configures a host-local volume.
type HostPathVolumeSpec struct {
	// Path on the host node (e.g. "/mnt/nvme0/data").
	// Each pod gets an isolated subdirectory: <path>/<pod-ordinal>.
	Path string `json:"path"`

	// Type is the hostPath type.
	// +kubebuilder:default="DirectoryOrCreate"
	// +kubebuilder:validation:Enum=Directory;DirectoryOrCreate
	// +optional
	Type string `json:"type,omitempty"`
}

// IsPVC returns true if this volume uses a PersistentVolumeClaim (the default).
func (v *VolumeSpec) IsPVC() bool {
	return v.HostPath == nil
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

// NetworkPolicySpec defines egress NetworkPolicy configuration.
type NetworkPolicySpec struct {
	// Enabled enables the egress NetworkPolicy.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ExternalCIDRExcept overrides the default RFC1918 CIDR blocks excluded from external egress.
	// Defaults to [10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16].
	// +optional
	ExternalCIDRExcept []string `json:"externalCIDRExcept,omitempty"`

	// AdditionalEgress appends custom egress rules to the generated NetworkPolicy.
	// Use this to allow traffic to cluster-internal services (e.g. databases, message brokers).
	// +optional
	AdditionalEgress []networkingv1.NetworkPolicyEgressRule `json:"additionalEgress,omitempty"`
}

// DNSEndpointSpec defines ExternalDNS DNSEndpoint configuration.
// +kubebuilder:validation:XValidation:rule="!self.enabled || size(self.endpoints) > 0",message="endpoints are required when dnsEndpoint is enabled"
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
	// Phase of the LedgerService: Pending, Running, Degraded, Error.
	// +optional
	Phase string `json:"phase,omitempty"`

	// ReadyReplicas is the number of ready pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// ObservedGeneration is the generation last observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Endpoints holds the resolved external and internal endpoints for this service.
	// +optional
	Endpoints *EndpointsStatus `json:"endpoints,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// TLSMigrationPhase reflects the operator's current TLS rollout phase.
	// One of "disabled", "required", "transitioning-to-required",
	// "transitioning-to-disabled". Set by the controller's TLS state
	// machine; users observe the user-facing tls.enabled bool, this field
	// exposes the intermediate "optional" mode used during a toggle.
	// +optional
	TLSMigrationPhase string `json:"tlsMigrationPhase,omitempty"`
}

// EndpointsStatus contains the resolved endpoints for a LedgerService.
type EndpointsStatus struct {
	// GRPC is the gRPC endpoint (e.g. "my-service-grpc.example.com:443" or "my-service.ns.svc.cluster.local:8888").
	GRPC string `json:"grpc"`

	// HTTP is the HTTP endpoint (e.g. "https://my-service.example.com" or "http://my-service.ns.svc.cluster.local:9000").
	HTTP string `json:"http"`

	// External is true when endpoints are externally reachable (via Ingress).
	External bool `json:"external"`
}

// ReceiptSigningConfig holds HMAC receipt signing configuration.
type ReceiptSigningConfig struct {
	// SecretName is the Kubernetes secret containing the HMAC key.
	// +optional
	SecretName string `json:"secretName,omitempty"`

	// SecretKey is the key in the secret containing the HMAC key.
	// +kubebuilder:default="key"
	// +optional
	SecretKey string `json:"secretKey,omitempty"`
}

// BloomConfig holds bloom filter configuration per attribute type.
type BloomConfig struct {
	// Volumes bloom filter configuration.
	// +optional
	Volumes *BloomFilterConfig `json:"volumes,omitempty"`

	// Metadata bloom filter configuration.
	// +optional
	Metadata *BloomFilterConfig `json:"metadata,omitempty"`

	// References bloom filter configuration.
	// +optional
	References *BloomFilterConfig `json:"references,omitempty"`

	// Ledgers bloom filter configuration.
	// +optional
	Ledgers *BloomFilterConfig `json:"ledgers,omitempty"`

	// Boundaries bloom filter configuration.
	// +optional
	Boundaries *BloomFilterConfig `json:"boundaries,omitempty"`

	// Transactions bloom filter configuration.
	// +optional
	Transactions *BloomFilterConfig `json:"transactions,omitempty"`

	// SinkConfigs bloom filter configuration.
	// +optional
	SinkConfigs *BloomFilterConfig `json:"sinkConfigs,omitempty"`

	// NumscriptVersions bloom filter configuration.
	// +optional
	NumscriptVersions *BloomFilterConfig `json:"numscriptVersions,omitempty"`

	// NumscriptContents bloom filter configuration.
	// +optional
	NumscriptContents *BloomFilterConfig `json:"numscriptContents,omitempty"`

	// LedgerMetadata bloom filter configuration.
	// +optional
	LedgerMetadata *BloomFilterConfig `json:"ledgerMetadata,omitempty"`

	// PreparedQueries bloom filter configuration.
	// +optional
	PreparedQueries *BloomFilterConfig `json:"preparedQueries,omitempty"`
}

// BloomFilterConfig holds configuration for a single bloom filter type.
type BloomFilterConfig struct {
	// ExpectedKeys is the expected number of unique keys (0 to disable this filter).
	// +optional
	ExpectedKeys *int64 `json:"expectedKeys,omitempty"`

	// FPRate is the false positive rate (0.0-1.0).
	// +optional
	FPRate string `json:"fpRate,omitempty"`
}

func init() {
	SchemeBuilder.Register(&LedgerService{}, &LedgerServiceList{})
}
