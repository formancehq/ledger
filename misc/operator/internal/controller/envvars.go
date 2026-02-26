package controller

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
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

// buildEnvVars constructs the complete list of environment variables for the ledger container.
func buildEnvVars(ledger *ledgerv1alpha1.LedgerService) []corev1.EnvVar {
	spec := &ledger.Spec
	cfg := &spec.Config

	envs := []corev1.EnvVar{
		// Field references
		fieldRefEnv("POD_INDEX", "metadata.labels['apps.kubernetes.io/pod-index']"),
		fieldRefEnv("POD_NAME", "metadata.name"),
		fieldRefEnv("POD_NAMESPACE", "metadata.namespace"),

		// Core configuration
		strEnv("BIND_ADDR", cfg.BindAddr),
		int32Env("GRPC_PORT", cfg.GrpcPort),
		int32Env("HTTP_PORT", cfg.HttpPort),
		strEnv("WAL_DIR", cfg.WalDir),
		strEnv("DATA_DIR", cfg.DataDir),
	}

	envs = appendIfStr(envs, "CLUSTER_ID", cfg.ClusterID)
	envs = append(envs, boolEnv("DEBUG", cfg.Debug))

	// Cache
	if cfg.Cache != nil {
		envs = appendIfInt32(envs, "CACHE_ROTATION_THRESHOLD", cfg.Cache.RotationThreshold)
	}

	// Raft
	if cfg.Raft != nil {
		envs = appendIfInt32(envs, "SNAPSHOT_THRESHOLD", cfg.Raft.SnapshotThreshold)
		envs = appendIfStr(envs, "SNAPSHOT_INTERVAL", cfg.Raft.SnapshotInterval)
		envs = appendIfInt32(envs, "RAFT_ELECTION_TICK", cfg.Raft.ElectionTick)
		envs = appendIfInt32(envs, "RAFT_HEARTBEAT_TICK", cfg.Raft.HeartbeatTick)
		envs = appendIfInt64(envs, "RAFT_MAX_SIZE_PER_MSG", cfg.Raft.MaxSizePerMsg)
		envs = appendIfInt32(envs, "RAFT_MAX_INFLIGHT_MSGS", cfg.Raft.MaxInflightMsgs)
		envs = appendIfStr(envs, "RAFT_TICK_INTERVAL", cfg.Raft.TickInterval)
		envs = appendIfInt32(envs, "RAFT_PROPOSE_QUEUE_CAPACITY", cfg.Raft.ProposeQueueCapacity)
		envs = appendIfInt32(envs, "RAFT_COMPACTION_MARGIN", cfg.Raft.CompactionMargin)

		if cfg.Raft.Transport != nil {
			if len(cfg.Raft.Transport.ReceptionQueues) > 0 {
				envs = append(envs, strEnv("RAFT_TRANSPORT_RECEPTION_QUEUES", int32SliceToCSV(cfg.Raft.Transport.ReceptionQueues)))
			}
			if len(cfg.Raft.Transport.SendQueues) > 0 {
				envs = append(envs, strEnv("RAFT_TRANSPORT_SEND_QUEUES", int32SliceToCSV(cfg.Raft.Transport.SendQueues)))
			}
		}
	}

	// Audit
	if cfg.Audit != nil {
		envs = appendIfBool(envs, "AUDIT_ENABLED", cfg.Audit.Enabled)
	}
	envs = appendIfBool(envs, "ADMISSION_METRICS", cfg.AdmissionMetrics)

	// Cold storage
	if cfg.ColdStorage != nil {
		envs = appendIfStr(envs, "COLD_STORAGE_DRIVER", cfg.ColdStorage.Driver)
		envs = appendIfStr(envs, "COLD_STORAGE_PATH", cfg.ColdStorage.Path)
		envs = appendIfStr(envs, "COLD_STORAGE_BUCKET_ID", cfg.ColdStorage.BucketID)
		if cfg.ColdStorage.S3 != nil {
			envs = appendIfStr(envs, "COLD_STORAGE_S3_BUCKET", cfg.ColdStorage.S3.Bucket)
			envs = appendIfStr(envs, "COLD_STORAGE_S3_REGION", cfg.ColdStorage.S3.Region)
			envs = appendIfStr(envs, "COLD_STORAGE_S3_ENDPOINT", cfg.ColdStorage.S3.Endpoint)
		}
	}

	// Health
	if cfg.Health != nil {
		envs = appendIfStr(envs, "HEALTH_CHECK_INTERVAL", cfg.Health.Interval)
		envs = appendIfStr(envs, "HEALTH_WAL_THRESHOLD", cfg.Health.WalThreshold)
		envs = appendIfStr(envs, "HEALTH_DATA_THRESHOLD", cfg.Health.DataThreshold)
		envs = appendIfStr(envs, "HEALTH_CLOCK_SKEW_THRESHOLD", cfg.Health.ClockSkewThreshold)
	}

	// Pebble
	if cfg.Pebble != nil {
		envs = appendIfInt64(envs, "PEBBLE_MEMTABLE_SIZE", cfg.Pebble.MemTableSize)
		envs = appendIfInt32(envs, "PEBBLE_MEMTABLE_STOP_WRITES_THRESHOLD", cfg.Pebble.MemTableStopWritesThreshold)
		envs = appendIfInt32(envs, "PEBBLE_L0_COMPACTION_THRESHOLD", cfg.Pebble.L0CompactionThreshold)
		envs = appendIfInt32(envs, "PEBBLE_L0_STOP_WRITES_THRESHOLD", cfg.Pebble.L0StopWritesThreshold)
		envs = appendIfInt64(envs, "PEBBLE_LBASE_MAX_BYTES", cfg.Pebble.LBaseMaxBytes)
		envs = appendIfInt64(envs, "PEBBLE_CACHE_SIZE", cfg.Pebble.CacheSize)
		envs = appendIfInt64(envs, "PEBBLE_TARGET_FILE_SIZE", cfg.Pebble.TargetFileSize)
		envs = appendIfInt64(envs, "PEBBLE_BYTES_PER_SYNC", cfg.Pebble.BytesPerSync)
		envs = appendIfInt64(envs, "PEBBLE_WAL_BYTES_PER_SYNC", cfg.Pebble.WalBytesPerSync)
		envs = appendIfInt32(envs, "PEBBLE_MAX_CONCURRENT_COMPACTIONS", cfg.Pebble.MaxConcurrentCompactions)
		envs = appendIfStr(envs, "PEBBLE_WAL_MIN_SYNC_INTERVAL", cfg.Pebble.WalMinSyncInterval)
		envs = appendIfBool(envs, "PEBBLE_DISABLE_WAL", cfg.Pebble.DisableWAL)
	}

	// Monitoring
	if cfg.Monitoring != nil {
		envs = appendMonitoringEnvVars(envs, cfg.Monitoring)
	}

	// TLS
	if cfg.TLS != nil && cfg.TLS.Enabled {
		envs = append(envs,
			strEnv("TLS_CERT_FILE", "/tls/tls.crt"),
			strEnv("TLS_KEY_FILE", "/tls/tls.key"),
		)
		if cfg.TLS.CASecretKey != "" {
			envs = append(envs, strEnv("TLS_CA_CERT_FILE", "/tls/"+cfg.TLS.CASecretKey))
		}
	}

	// GOMEMLIMIT: set to 90% of memory limit if available.
	if ledger.Spec.Resources.Limits != nil {
		if memLimit, ok := ledger.Spec.Resources.Limits[corev1.ResourceMemory]; ok {
			goMemLimit := memLimit.Value() * 9 / 10
			envs = append(envs, intEnv("GOMEMLIMIT", goMemLimit))
		}
	}

	// Response signing
	if cfg.ResponseSigning != nil && cfg.ResponseSigning.Enabled {
		secretKey := cfg.ResponseSigning.SecretKey
		if secretKey == "" {
			secretKey = "seed"
		}
		envs = append(envs, strEnv("RESPONSE_SIGNING_KEY", "/response-signing/"+secretKey))
	}

	return envs
}

func appendMonitoringEnvVars(envs []corev1.EnvVar, mon *ledgerv1alpha1.MonitoringConfig) []corev1.EnvVar {
	// Metrics-specific OTEL vars
	if mon.Metrics != nil {
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

	// core.monitoring equivalent: OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES, traces, logs enabled/level
	envs = appendIfStr(envs, "OTEL_SERVICE_NAME", mon.ServiceName)
	envs = appendIfStr(envs, "OTEL_RESOURCE_ATTRIBUTES", mon.Attributes)

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

	return envs
}

func int32SliceToCSV(values []int32) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprintf("%d", v)
	}
	return strings.Join(parts, ",")
}
