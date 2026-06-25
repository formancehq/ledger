package controller

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// env var helper constructors

func fieldRefEnv(name, fieldPath string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: fieldPath},
		},
	}
}

func strEnv(name, value string) corev1.EnvVar {
	return corev1.EnvVar{Name: name, Value: value}
}

func intEnv(name string, value int64) corev1.EnvVar {
	return corev1.EnvVar{Name: name, Value: strconv.FormatInt(value, 10)}
}

func int32Env(name string, value int32) corev1.EnvVar {
	return corev1.EnvVar{Name: name, Value: strconv.FormatInt(int64(value), 10)}
}

func boolEnv(name string, value bool) corev1.EnvVar {
	return corev1.EnvVar{Name: name, Value: strconv.FormatBool(value)}
}

// appendIfStr appends an env var if the string value is non-empty.
func appendIfStr(envs []corev1.EnvVar, name, value string) []corev1.EnvVar {
	if value != "" {
		return append(envs, strEnv(name, value))
	}

	return envs
}

// appendIfInt32 appends an env var if the pointer is non-nil.
func appendIfInt32(envs []corev1.EnvVar, name string, value *int32) []corev1.EnvVar {
	if value != nil {
		return append(envs, int32Env(name, *value))
	}

	return envs
}

// appendIfInt64 appends an env var if the pointer is non-nil.
func appendIfInt64(envs []corev1.EnvVar, name string, value *int64) []corev1.EnvVar {
	if value != nil {
		return append(envs, intEnv(name, *value))
	}

	return envs
}

// appendIfBool appends an env var if the pointer is non-nil.
func appendIfBool(envs []corev1.EnvVar, name string, value *bool) []corev1.EnvVar {
	if value != nil {
		return append(envs, boolEnv(name, *value))
	}

	return envs
}

// appendIfQuantity appends an env var if the pointer is non-nil, using the quantity's string representation.
func appendIfQuantity(envs []corev1.EnvVar, name string, value *resource.Quantity) []corev1.EnvVar {
	if value != nil {
		return append(envs, strEnv(name, value.String()))
	}

	return envs
}

// buildEnvVars constructs the complete list of environment variables for the
// ledger container. targetTLSMode is the operator-decided TLS mode for this
// reconcile pass (one of "disabled", "optional", "required"); it differs from
// spec.TLS.Enabled during a TLS toggle, when the operator first walks the
// StatefulSet through "optional".
//
// The agents slice drives whether AUTH_ED25519_KEYS is exposed: it is set
// only when at least one agent is registered AND auth is not explicitly
// disabled.
func buildEnvVars(ledger *ledgerv1alpha1.LedgerService, targetTLSMode string, agents []agentKeyInfo) []corev1.EnvVar {
	spec := &ledger.Spec
	hlsSvcName := headlessServiceName(ledger.Name)

	// ADVERTISE_ADDR is the host:port a Raft peer dials to reach this node's
	// Raft transport — it must point at the BindAddr port (Raft port, e.g.
	// 7777), not the service gRPC port. Sending peers to GRPC_PORT routes
	// them to the BucketService listener, which doesn't register
	// raft_transport.RaftTransportService — every AppendEntries comes back
	// `Unimplemented` and the cluster degrades silently.
	//
	// Kubernetes' built-in $(VAR) substitution on env var values lets the
	// kubelet resolve $(POD_NAME) / $(POD_NAMESPACE) from the field refs
	// above before passing the env to the container.
	advertiseAddr := fmt.Sprintf("$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:%d", hlsSvcName, raftPortFromBindAddr(spec.BindAddr))

	envs := []corev1.EnvVar{
		// Field references
		fieldRefEnv("POD_INDEX", "metadata.labels['apps.kubernetes.io/pod-index']"),
		fieldRefEnv("POD_NAME", "metadata.name"),
		fieldRefEnv("POD_NAMESPACE", "metadata.namespace"),

		// Core configuration
		strEnv("BIND_ADDR", spec.BindAddr),
		int32Env("GRPC_PORT", spec.GrpcPort),
		int32Env("HTTP_PORT", spec.HttpPort),
		strEnv("WAL_DIR", spec.WalDir),
		strEnv("DATA_DIR", spec.DataDir),
		strEnv("ADVERTISE_ADDR", advertiseAddr),
	}

	envs = appendIfStr(envs, "CLUSTER_ID", spec.ClusterID)
	envs = append(envs, boolEnv("DEBUG", spec.Debug))
	envs = appendIfStr(envs, "LOG_LEVEL", spec.LogLevel)

	// Cache
	if spec.Cache != nil {
		envs = appendIfInt32(envs, "CACHE_ROTATION_THRESHOLD", spec.Cache.RotationThreshold)
	}

	// Raft
	if spec.Raft != nil {
		envs = appendIfStr(envs, "MAINTENANCE_INTERVAL", spec.Raft.MaintenanceInterval)
		envs = appendIfInt32(envs, "RAFT_ELECTION_TICK", spec.Raft.ElectionTick)
		envs = appendIfInt32(envs, "RAFT_HEARTBEAT_TICK", spec.Raft.HeartbeatTick)
		envs = appendIfQuantity(envs, "RAFT_MAX_SIZE_PER_MSG", spec.Raft.MaxSizePerMsg)
		envs = appendIfInt32(envs, "RAFT_MAX_INFLIGHT_MSGS", spec.Raft.MaxInflightMsgs)
		envs = appendIfStr(envs, "RAFT_TICK_INTERVAL", spec.Raft.TickInterval)
		envs = appendIfInt32(envs, "RAFT_PROPOSE_QUEUE_CAPACITY", spec.Raft.ProposeQueueCapacity)
		envs = appendIfInt32(envs, "RAFT_COMPACTION_MARGIN", spec.Raft.CompactionMargin)
		envs = appendIfInt32(envs, "RAFT_REPLAY_BATCH_SIZE", spec.Raft.ReplayBatchSize)
		envs = appendIfStr(envs, "RAFT_PROCESSING_TICK_INTERVAL", spec.Raft.ProcessingTickInterval)
		envs = appendIfInt32(envs, "LEARNER_PROMOTION_THRESHOLD", spec.Raft.LearnerPromotionThreshold)

		if spec.Raft.Transport != nil {
			if len(spec.Raft.Transport.ReceptionQueues) > 0 {
				envs = append(envs, strEnv("RAFT_TRANSPORT_RECEPTION_QUEUES", int32SliceToCSV(spec.Raft.Transport.ReceptionQueues)))
			}
			if len(spec.Raft.Transport.SendQueues) > 0 {
				envs = append(envs, strEnv("RAFT_TRANSPORT_SEND_QUEUES", int32SliceToCSV(spec.Raft.Transport.SendQueues)))
			}
			envs = appendIfQuantity(envs, "RAFT_TRANSPORT_BUFFER_SIZE", spec.Raft.Transport.BufferSize)
		}
	}

	envs = appendIfBool(envs, "ADMISSION_METRICS", spec.AdmissionMetrics)
	envs = appendIfStr(envs, "METRICS_NAMING", spec.MetricsNaming)
	envs = appendIfBool(envs, "SENTINEL_MODE", spec.SentinelMode)
	envs = appendIfBool(envs, "GRPC_COMPRESSION", spec.GrpcCompression)
	envs = appendIfStr(envs, "QUERY_PROFILE_THRESHOLD", spec.QueryProfileThreshold)
	envs = appendIfStr(envs, "GRPC_SLOW_THRESHOLD", spec.GrpcSlowThreshold)
	envs = appendIfInt32(envs, "NUMSCRIPT_CACHE_SIZE", spec.NumscriptCacheSize)
	envs = appendIfInt32(envs, "MIRROR_MAX_BATCH_SIZE", spec.MirrorMaxBatchSize)
	envs = appendIfInt32(envs, "MAX_EXECUTION_PLAN_SIZE", spec.MaxExecutionPlanSize)
	envs = appendIfStr(envs, "IDEMPOTENCY_TTL", spec.IdempotencyTTL)
	envs = appendIfStr(envs, "IDEMPOTENCY_EVICTION_INTERVAL", spec.IdempotencyEvictionInterval)
	envs = appendIfStr(envs, "HASH_ALGORITHM", spec.HashAlgorithm)
	envs = appendIfBool(envs, "UNSAFE_SKIP_CONFIG_VALIDATION", spec.UnsafeSkipConfigValidation)

	// Snapshot sync
	if spec.Snapshot != nil {
		envs = appendIfStr(envs, "SNAPSHOT_SESSION_TTL", spec.Snapshot.SessionTTL)
		envs = appendIfInt32(envs, "SNAPSHOT_PARALLELISM", spec.Snapshot.Parallelism)
		envs = appendIfInt32(envs, "SNAPSHOT_RETRY_COUNT", spec.Snapshot.RetryCount)
		envs = appendIfInt32(envs, "SNAPSHOT_FILE_RETRY_COUNT", spec.Snapshot.FileRetryCount)
	}

	// Cold storage
	if spec.ColdStorage != nil {
		envs = appendIfStr(envs, "COLD_STORAGE_DRIVER", spec.ColdStorage.Driver)
		envs = appendIfStr(envs, "COLD_STORAGE_PATH", spec.ColdStorage.Path)
		envs = appendIfStr(envs, "COLD_STORAGE_BUCKET_ID", spec.ColdStorage.BucketID)
		if spec.ColdStorage.S3 != nil {
			envs = appendIfStr(envs, "COLD_STORAGE_S3_BUCKET", spec.ColdStorage.S3.Bucket)
			envs = appendIfStr(envs, "COLD_STORAGE_S3_REGION", spec.ColdStorage.S3.Region)
			envs = appendIfStr(envs, "COLD_STORAGE_S3_ENDPOINT", spec.ColdStorage.S3.Endpoint)
		}
	}

	// Cold cache directory — set to the dedicated mount point when cold storage is enabled
	if spec.ColdStorage == nil || spec.ColdStorage.Driver != "none" {
		envs = append(envs, corev1.EnvVar{Name: "COLD_CACHE_DIR", Value: "/data/cold-cache"})
	}

	// Health
	if spec.Health != nil {
		envs = appendIfStr(envs, "HEALTH_CHECK_INTERVAL", spec.Health.Interval)
		envs = appendIfStr(envs, "HEALTH_WAL_THRESHOLD", spec.Health.WalThreshold)
		envs = appendIfStr(envs, "HEALTH_DATA_THRESHOLD", spec.Health.DataThreshold)
		envs = appendIfStr(envs, "HEALTH_WAL_RESUME_THRESHOLD", spec.Health.WalResumeThreshold)
		envs = appendIfStr(envs, "HEALTH_DATA_RESUME_THRESHOLD", spec.Health.DataResumeThreshold)
		envs = appendIfStr(envs, "HEALTH_CLOCK_SKEW_THRESHOLD", spec.Health.ClockSkewThreshold)
	}

	// Pebble
	if spec.Pebble != nil {
		envs = appendPebbleEnvVars(envs, "PEBBLE", spec.Pebble)
		// DAL-specific Pebble flags
		envs = appendIfQuantity(envs, "PEBBLE_WAL_BYTES_PER_SYNC", spec.Pebble.WalBytesPerSync)
		envs = appendIfStr(envs, "PEBBLE_WAL_MIN_SYNC_INTERVAL", spec.Pebble.WalMinSyncInterval)
		envs = appendIfBool(envs, "PEBBLE_DISABLE_WAL", spec.Pebble.DisableWAL)
		envs = appendIfInt64(envs, "PEBBLE_INCREMENTAL_COMPACT_THRESHOLD", spec.Pebble.IncrementalCompactThreshold)
		envs = appendIfInt32(envs, "PEBBLE_MAX_CHECKPOINTS", spec.Pebble.MaxCheckpoints)
		if spec.Pebble.ValueSeparation != nil {
			envs = appendIfBool(envs, "PEBBLE_VALUE_SEPARATION", spec.Pebble.ValueSeparation.Enabled)
			envs = appendIfQuantity(envs, "PEBBLE_VALUE_SEPARATION_MIN_SIZE", spec.Pebble.ValueSeparation.MinSize)
			envs = appendIfInt32(envs, "PEBBLE_VALUE_SEPARATION_MAX_DEPTH", spec.Pebble.ValueSeparation.MaxDepth)
			envs = appendIfStr(envs, "PEBBLE_VALUE_SEPARATION_REWRITE_AGE", spec.Pebble.ValueSeparation.RewriteAge)
			envs = appendIfStr(envs, "PEBBLE_VALUE_SEPARATION_GARBAGE_RATIO", spec.Pebble.ValueSeparation.GarbageRatio)
		}
	}

	// Bloom filters
	if spec.Bloom != nil {
		envs = appendBloomEnvVars(envs, spec.Bloom)
	}

	// Receipt signing
	if spec.ReceiptSigning != nil && spec.ReceiptSigning.SecretName != "" {
		secretKey := spec.ReceiptSigning.SecretKey
		if secretKey == "" {
			secretKey = "key"
		}
		envs = append(envs, corev1.EnvVar{
			Name: "RECEIPT_SIGNING_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: spec.ReceiptSigning.SecretName,
					},
					Key: secretKey,
				},
			},
		})
	}

	// Monitoring
	envs = appendMonitoringEnvVars(envs, spec.Monitoring, ledger.Name)

	// TLS — TLS_MODE is driven by the operator state machine, not directly
	// by spec.TLS.Enabled (the operator passes through "optional" during a
	// toggle so that pods on either side of the rolling update can still
	// talk to each other).
	envs = append(envs, strEnv("TLS_MODE", targetTLSMode))

	if targetTLSMode != tlsModeDisabled {
		envs = append(envs,
			strEnv("TLS_CERT_FILE", "/tls/tls.crt"),
			strEnv("TLS_KEY_FILE", "/tls/tls.key"),
		)
		if spec.TLS != nil && spec.TLS.CASecretKey != "" {
			envs = append(envs, strEnv("TLS_CA_CERT_FILE", "/tls/"+spec.TLS.CASecretKey))
		}
	}

	// GOMEMLIMIT: set to a percentage of memory limit if available.
	if ledger.Spec.Resources.Limits != nil {
		if memLimit, ok := ledger.Spec.Resources.Limits[corev1.ResourceMemory]; ok {
			ratio := int64(90)
			if ledger.Spec.GoMemLimitRatio != nil {
				ratio = int64(*ledger.Spec.GoMemLimitRatio)
			}
			goMemLimit := memLimit.Value() * ratio / 100
			envs = append(envs, intEnv("GOMEMLIMIT", goMemLimit))
		}
	}

	// Auth
	if spec.Auth != nil {
		envs = appendIfBool(envs, "AUTH_ENABLED", spec.Auth.Enabled)
		envs = appendIfStr(envs, "AUTH_ISSUER", spec.Auth.Issuer)
		if len(spec.Auth.Issuers) > 0 {
			envs = append(envs, strEnv("AUTH_ISSUERS", strings.Join(spec.Auth.Issuers, ",")))
		}
		envs = appendIfStr(envs, "AUTH_SERVICE", spec.Auth.Service)
		envs = appendIfBool(envs, "AUTH_CHECK_SCOPES", spec.Auth.CheckScopes)
		envs = appendIfInt32(envs, "AUTH_READ_KEY_SET_MAX_RETRIES", spec.Auth.ReadKeySetMaxRetries)
		if len(spec.Auth.ScopeMapping) > 0 {
			data, err := json.Marshal(spec.Auth.ScopeMapping)
			if err == nil {
				envs = append(envs, strEnv("AUTH_SCOPE_MAPPING", string(data)))
			}
		}

		if len(spec.Auth.AnonymousScopes) > 0 {
			envs = append(envs, strEnv("AUTH_ANONYMOUS_SCOPES", strings.Join(spec.Auth.AnonymousScopes, ",")))
		}
	}

	// AUTH_ED25519_KEYS points at the configmap-mounted JSON file when
	// agents are registered and auth is not explicitly disabled. The path
	// is fixed by the volume mount declared in reconcileStatefulSet.
	authExplicitlyDisabled := spec.Auth != nil && spec.Auth.Enabled != nil && !*spec.Auth.Enabled
	if len(agents) > 0 && !authExplicitlyDisabled {
		envs = append(envs, strEnv("AUTH_ED25519_KEYS", "/auth-keys/auth-keys.json"))
	}

	// Read index
	if spec.ReadIndex != nil {
		envs = appendIfInt32(envs, "READ_INDEX_BATCH_SIZE", spec.ReadIndex.BatchSize)
		if spec.ReadIndex.Pebble != nil {
			envs = appendPebbleEnvVars(envs, "READ_INDEX", spec.ReadIndex.Pebble)
		}
	}

	// Response signing
	if spec.ResponseSigning != nil && spec.ResponseSigning.Enabled {
		secretKey := spec.ResponseSigning.SecretKey
		if secretKey == "" {
			secretKey = "seed"
		}
		envs = append(envs, strEnv("RESPONSE_SIGNING_KEY", "/response-signing/"+secretKey))
	}

	// Extra env vars from spec (appended last so they can override).
	envs = append(envs, spec.ExtraEnv...)

	return envs
}

func appendMonitoringEnvVars(envs []corev1.EnvVar, mon *ledgerv1alpha1.MonitoringConfig, clusterName string) []corev1.EnvVar {
	// OTEL_RESOURCE_ATTRIBUTES carries operator-injected attributes
	// (service.cluster, service.node_id) prefixed with the user-supplied
	// list, if any. $(POD_NAME) is resolved by the kubelet via env var
	// substitution. This var is emitted even when mon is nil so the
	// cluster attribution is always present.
	operatorAttrs := "service.cluster=" + clusterName + ",service.node_id=$(POD_NAME)"
	attrs := operatorAttrs
	if mon != nil && mon.Attributes != "" {
		attrs = mon.Attributes + "," + operatorAttrs
	}
	envs = append(envs, strEnv("OTEL_RESOURCE_ATTRIBUTES", attrs))

	if mon == nil {
		return envs
	}

	// Metrics-specific OTEL vars
	if mon.Metrics != nil {
		envs = appendIfBool(envs, "OTEL_METRICS_ENABLED", mon.Metrics.Enabled)
		envs = appendIfStr(envs, "OTEL_METRICS_EXPORTER", mon.Metrics.Exporter)
		if mon.Metrics.Endpoint != "" || mon.Metrics.Port != "" {
			envs = append(envs, strEnv("OTEL_METRICS_EXPORTER_OTLP_ENDPOINT", mon.Metrics.Endpoint+":"+mon.Metrics.Port))
		}
		envs = appendIfStr(envs, "OTEL_METRICS_EXPORTER_OTLP_INSECURE", mon.Metrics.Insecure)
		envs = appendIfStr(envs, "OTEL_METRICS_EXPORTER_OTLP_MODE", mon.Metrics.Mode)
		envs = appendIfBool(envs, "OTEL_METRICS_KEEP_IN_MEMORY", mon.Metrics.KeepInMemory)
		envs = appendIfStr(envs, "OTEL_METRICS_EXPORTER_PUSH_INTERVAL", mon.Metrics.ExporterPushInterval)
		envs = appendIfBool(envs, "OTEL_METRICS_RUNTIME", mon.Metrics.Runtime)
		envs = appendIfStr(envs, "OTEL_METRICS_RUNTIME_MINIMUM_READ_MEM_STATS_INTERVAL", mon.Metrics.RuntimeMinimumReadMemStatsInterval)
	}

	// Logs exporter vars
	if mon.Logs != nil {
		envs = appendIfStr(envs, "OTEL_LOGS_EXPORTER", mon.Logs.Exporter)
		if mon.Logs.Endpoint != "" || mon.Logs.Port != "" {
			envs = append(envs, strEnv("OTEL_LOGS_EXPORTER_OTLP_ENDPOINT", mon.Logs.Endpoint+":"+mon.Logs.Port))
		}
		envs = appendIfStr(envs, "OTEL_LOGS_EXPORTER_OTLP_INSECURE", mon.Logs.Insecure)
		envs = appendIfStr(envs, "OTEL_LOGS_EXPORTER_OTLP_MODE", mon.Logs.Mode)
	}

	// core.monitoring equivalent: OTEL_SERVICE_NAME, traces, logs enabled/level
	// (OTEL_RESOURCE_ATTRIBUTES is merged with operator-injected attrs above)
	envs = appendIfStr(envs, "OTEL_SERVICE_NAME", mon.ServiceName)

	// Traces
	if mon.Traces != nil {
		envs = appendIfBool(envs, "OTEL_TRACES", mon.Traces.Enabled)
		envs = appendIfStr(envs, "OTEL_TRACES_BATCH", mon.Traces.Batch)
		envs = appendIfStr(envs, "OTEL_TRACES_ENDPOINT", mon.Traces.Endpoint)
		envs = appendIfStr(envs, "OTEL_TRACES_PORT", mon.Traces.Port)
		envs = appendIfStr(envs, "OTEL_TRACES_EXPORTER", mon.Traces.Exporter)
		envs = appendIfStr(envs, "OTEL_TRACES_EXPORTER_OTLP_INSECURE", mon.Traces.Insecure)
		envs = appendIfStr(envs, "OTEL_TRACES_EXPORTER_OTLP_MODE", mon.Traces.Mode)
		if mon.Traces.Endpoint != "" || mon.Traces.Port != "" {
			envs = append(envs, strEnv("OTEL_TRACES_EXPORTER_OTLP_ENDPOINT", mon.Traces.Endpoint+":"+mon.Traces.Port))
		}

		// Trace sampling
		if mon.Traces.Sampling != nil && mon.Traces.Sampling.Enabled {
			envs = append(envs, boolEnv("TRACE_SAMPLING_ENABLED", true))
			envs = appendIfStr(envs, "TRACE_SAMPLING_SUCCESS_RATIO", mon.Traces.Sampling.SuccessRatio)
		}
	}

	// Logs enabled/level (core.monitoring)
	if mon.Logs != nil {
		envs = appendIfBool(envs, "LOGS_ENABLED", mon.Logs.Enabled)
		envs = appendIfStr(envs, "LOGS_LEVEL", mon.Logs.Level)
	}

	// Pyroscope
	if mon.Pyroscope != nil && mon.Pyroscope.Enabled {
		envs = append(envs, boolEnv("PYROSCOPE_ENABLED", true))
		envs = appendIfStr(envs, "PYROSCOPE_SERVER_ADDRESS", mon.Pyroscope.ServerAddress)

		appName := mon.Pyroscope.ApplicationName
		if appName == "" {
			appName = mon.ServiceName
		}
		envs = appendIfStr(envs, "PYROSCOPE_APPLICATION_NAME", appName)

		envs = appendIfStr(envs, "PYROSCOPE_AUTH_TOKEN", mon.Pyroscope.AuthToken)
		envs = appendIfStr(envs, "PYROSCOPE_TENANT_ID", mon.Pyroscope.TenantID)
		envs = appendIfStr(envs, "PYROSCOPE_BASIC_AUTH_USER", mon.Pyroscope.BasicAuthUser)
		envs = appendIfStr(envs, "PYROSCOPE_BASIC_AUTH_PASSWORD", mon.Pyroscope.BasicAuthPassword)
		envs = appendIfStr(envs, "PYROSCOPE_UPLOAD_RATE", mon.Pyroscope.UploadRate)
		envs = appendIfStr(envs, "PYROSCOPE_TAGS", mon.Pyroscope.Tags)
		envs = appendIfStr(envs, "PYROSCOPE_PROFILE_TYPES", mon.Pyroscope.ProfileTypes)
		envs = appendIfInt32(envs, "PYROSCOPE_MUTEX_PROFILE_FRACTION", mon.Pyroscope.MutexProfileFraction)
		envs = appendIfInt32(envs, "PYROSCOPE_BLOCK_PROFILE_RATE", mon.Pyroscope.BlockProfileRate)
		envs = appendIfBool(envs, "PYROSCOPE_DISABLE_GC_RUNS", mon.Pyroscope.DisableGCRuns)
	}

	// Flight recorder
	if mon.FlightRecorder != nil && mon.FlightRecorder.Enabled {
		envs = append(envs, boolEnv("FLIGHT_RECORDER_ENABLED", true))
		envs = appendIfStr(envs, "FLIGHT_RECORDER_MIN_AGE", mon.FlightRecorder.MinAge)
		envs = appendIfQuantity(envs, "FLIGHT_RECORDER_MAX_BYTES", mon.FlightRecorder.MaxBytes)
	}

	return envs
}

// appendPebbleEnvVars appends the common Pebble env vars for the given prefix.
// Prefix is "PEBBLE" or "READ_INDEX".
func appendPebbleEnvVars(envs []corev1.EnvVar, prefix string, p *ledgerv1alpha1.PebbleConfig) []corev1.EnvVar {
	envs = appendIfQuantity(envs, prefix+"_MEMTABLE_SIZE", p.MemTableSize)
	envs = appendIfInt32(envs, prefix+"_MEMTABLE_STOP_WRITES_THRESHOLD", p.MemTableStopWritesThreshold)
	envs = appendIfInt32(envs, prefix+"_L0_COMPACTION_THRESHOLD", p.L0CompactionThreshold)
	envs = appendIfInt32(envs, prefix+"_L0_STOP_WRITES_THRESHOLD", p.L0StopWritesThreshold)
	envs = appendIfQuantity(envs, prefix+"_LBASE_MAX_BYTES", p.LBaseMaxBytes)
	envs = appendIfQuantity(envs, prefix+"_CACHE_SIZE", p.CacheSize)
	envs = appendIfQuantity(envs, prefix+"_TARGET_FILE_SIZE", p.TargetFileSize)
	envs = appendIfQuantity(envs, prefix+"_BYTES_PER_SYNC", p.BytesPerSync)
	envs = appendIfInt32(envs, prefix+"_MAX_CONCURRENT_COMPACTIONS", p.MaxConcurrentCompactions)
	envs = appendIfStr(envs, prefix+"_COMPRESSION", p.Compression)

	return envs
}

// appendBloomEnvVars adds bloom filter environment variables.
func appendBloomEnvVars(envs []corev1.EnvVar, bloom *ledgerv1alpha1.BloomConfig) []corev1.EnvVar {
	type entry struct {
		prefix string
		spec   *ledgerv1alpha1.BloomFilterConfig
	}
	entries := []entry{
		{"BLOOM_VOLUMES", bloom.Volumes},
		{"BLOOM_METADATA", bloom.Metadata},
		{"BLOOM_REFERENCES", bloom.References},
		{"BLOOM_LEDGERS", bloom.Ledgers},
		{"BLOOM_BOUNDARIES", bloom.Boundaries},
		{"BLOOM_TRANSACTIONS", bloom.Transactions},
		{"BLOOM_SINK_CONFIGS", bloom.SinkConfigs},
		{"BLOOM_NUMSCRIPT_VERSIONS", bloom.NumscriptVersions},
		{"BLOOM_NUMSCRIPT_CONTENTS", bloom.NumscriptContents},
		{"BLOOM_LEDGER_METADATA", bloom.LedgerMetadata},
		{"BLOOM_PREPARED_QUERIES", bloom.PreparedQueries},
	}
	for _, e := range entries {
		if e.spec != nil {
			envs = appendIfInt64(envs, e.prefix+"_EXPECTED_KEYS", e.spec.ExpectedKeys)
			envs = appendIfStr(envs, e.prefix+"_FP_RATE", e.spec.FPRate)
		}
	}

	return envs
}

func int32SliceToCSV(values []int32) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = strconv.Itoa(int(v))
	}

	return strings.Join(parts, ",")
}
