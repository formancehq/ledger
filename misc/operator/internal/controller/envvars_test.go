package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// findEnv returns the env var with the given name, or nil if not found.
func findEnv(envs []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range envs {
		if envs[i].Name == name {
			return &envs[i]
		}
	}

	return nil
}

// assertEnv asserts that the env var exists with the expected value.
func assertEnv(t *testing.T, envs []corev1.EnvVar, name, expectedValue string) {
	t.Helper()
	e := findEnv(envs, name)
	if assert.NotNilf(t, e, "env var %q not found", name) {
		assert.Equal(t, expectedValue, e.Value, "env var %s", name)
	}
}

// assertNoEnv asserts that the env var does not exist.
func assertNoEnv(t *testing.T, envs []corev1.EnvVar, name string) {
	t.Helper()
	assert.Nilf(t, findEnv(envs, name), "env var %q should not be present", name)
}

// newMinimalCluster builds a minimal Cluster for testing buildEnvVars.
func newMinimalCluster() *ledgerv1alpha1.Cluster {
	return &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			BindAddr:  "0.0.0.0:7777",
			GrpcPort:  8888,
			HttpPort:  9000,
			WalDir:    "/data/raft",
			DataDir:   "/data/app",
			ClusterID: "default",
		},
	}
}

// ---------------------------------------------------------------------------
// Sentinel mode (was VolumeAssertions)
// ---------------------------------------------------------------------------

func TestBuildEnvVars_SentinelMode(t *testing.T) {
	t.Parallel()

	t.Run("set to true", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		b := true
		ls.Spec.SentinelMode = &b
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "SENTINEL_MODE", "true")
	})

	t.Run("set to false", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		b := false
		ls.Spec.SentinelMode = &b
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "SENTINEL_MODE", "false")
	})

	t.Run("nil omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "SENTINEL_MODE")
	})

	t.Run("old VOLUME_ASSERTIONS not emitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		b := true
		ls.Spec.SentinelMode = &b
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "VOLUME_ASSERTIONS")
	})
}

// ---------------------------------------------------------------------------
// FSM determinism + cross-node digest opt-in
// ---------------------------------------------------------------------------

func TestBuildEnvVars_FSMDeterminismEnabled(t *testing.T) {
	t.Parallel()

	t.Run("set to true", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		b := true
		ls.Spec.FSMDeterminismEnabled = &b
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "FSM_DETERMINISM_ENABLED", "true")
	})

	t.Run("set to false", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		b := false
		ls.Spec.FSMDeterminismEnabled = &b
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "FSM_DETERMINISM_ENABLED", "false")
	})

	t.Run("nil omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "FSM_DETERMINISM_ENABLED")
	})
}

// ---------------------------------------------------------------------------
// Log level
// ---------------------------------------------------------------------------

func TestBuildEnvVars_LogLevel(t *testing.T) {
	t.Parallel()

	t.Run("trace", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.LogLevel = "trace"
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "LOG_LEVEL", "trace")
		// DEBUG is always emitted (default false); LogLevel takes precedence
		// server-side via resolveLogLevel.
		assertEnv(t, envs, "DEBUG", "false")
	})

	t.Run("empty omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "LOG_LEVEL")
	})

	t.Run("debug flag still emitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Debug = true
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "DEBUG", "true")
		assertNoEnv(t, envs, "LOG_LEVEL")
	})
}

// ---------------------------------------------------------------------------
// gRPC compression
// ---------------------------------------------------------------------------

func TestBuildEnvVars_GrpcCompression(t *testing.T) {
	t.Parallel()

	t.Run("enabled", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		b := true
		ls.Spec.GrpcCompression = &b
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "GRPC_COMPRESSION", "true")
	})

	t.Run("nil omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "GRPC_COMPRESSION")
	})
}

// ---------------------------------------------------------------------------
// Query profile threshold & gRPC slow threshold
// ---------------------------------------------------------------------------

func TestBuildEnvVars_QueryProfileThreshold(t *testing.T) {
	t.Parallel()

	ls := newMinimalCluster()
	ls.Spec.QueryProfileThreshold = "50ms"
	envs := buildEnvVars(ls, "disabled", nil)
	assertEnv(t, envs, "QUERY_PROFILE_THRESHOLD", "50ms")
}

func TestBuildEnvVars_GrpcSlowThreshold(t *testing.T) {
	t.Parallel()

	ls := newMinimalCluster()
	ls.Spec.GrpcSlowThreshold = "2s"
	envs := buildEnvVars(ls, "disabled", nil)
	assertEnv(t, envs, "GRPC_SLOW_THRESHOLD", "2s")
}

func TestBuildEnvVars_ThresholdsOmittedWhenEmpty(t *testing.T) {
	t.Parallel()

	ls := newMinimalCluster()
	envs := buildEnvVars(ls, "disabled", nil)
	assertNoEnv(t, envs, "QUERY_PROFILE_THRESHOLD")
	assertNoEnv(t, envs, "GRPC_SLOW_THRESHOLD")
}

// ---------------------------------------------------------------------------
// Numscript cache size & mirror max batch size
// ---------------------------------------------------------------------------

func TestBuildEnvVars_NumscriptCacheSize(t *testing.T) {
	t.Parallel()

	ls := newMinimalCluster()
	v := int32(2048)
	ls.Spec.NumscriptCacheSize = &v
	envs := buildEnvVars(ls, "disabled", nil)
	assertEnv(t, envs, "NUMSCRIPT_CACHE_SIZE", "2048")
}

func TestBuildEnvVars_MirrorMaxBatchSize(t *testing.T) {
	t.Parallel()

	ls := newMinimalCluster()
	v := int32(1000)
	ls.Spec.MirrorMaxBatchSize = &v
	envs := buildEnvVars(ls, "disabled", nil)
	assertEnv(t, envs, "MIRROR_MAX_BATCH_SIZE", "1000")
}

// ---------------------------------------------------------------------------
// Pebble compression
// ---------------------------------------------------------------------------

func TestBuildEnvVars_PebbleCompression(t *testing.T) {
	t.Parallel()

	t.Run("primary store", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Pebble = &ledgerv1alpha1.PebbleConfig{
			Compression: "snappy,snappy,zstd,zstd,zstd,zstd,zstd",
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "PEBBLE_COMPRESSION", "snappy,snappy,zstd,zstd,zstd,zstd,zstd")
	})

	t.Run("read index", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.ReadIndex = &ledgerv1alpha1.ReadIndexConfig{
			Pebble: &ledgerv1alpha1.PebbleConfig{
				Compression: "none,none,zstd,zstd,zstd,zstd,zstd",
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "READ_INDEX_COMPRESSION", "none,none,zstd,zstd,zstd,zstd,zstd")
	})

	t.Run("omitted when empty", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Pebble = &ledgerv1alpha1.PebbleConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "PEBBLE_COMPRESSION")
	})
}

// ---------------------------------------------------------------------------
// Pebble max checkpoints
// ---------------------------------------------------------------------------

func TestBuildEnvVars_PebbleMaxCheckpoints(t *testing.T) {
	t.Parallel()

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		v := int32(5)
		ls.Spec.Pebble = &ledgerv1alpha1.PebbleConfig{
			MaxCheckpoints: &v,
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "PEBBLE_MAX_CHECKPOINTS", "5")
	})

	t.Run("omitted when nil", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Pebble = &ledgerv1alpha1.PebbleConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "PEBBLE_MAX_CHECKPOINTS")
	})
}

// ---------------------------------------------------------------------------
// Pebble value separation
// ---------------------------------------------------------------------------

func TestBuildEnvVars_PebbleValueSeparation(t *testing.T) {
	t.Parallel()

	t.Run("full config", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		bTrue := true
		minSize := resource.MustParse("512")
		maxDepth := int32(8)
		ls.Spec.Pebble = &ledgerv1alpha1.PebbleConfig{
			ValueSeparation: &ledgerv1alpha1.PebbleValueSeparationConfig{
				Enabled:      &bTrue,
				MinSize:      &minSize,
				MaxDepth:     &maxDepth,
				RewriteAge:   "2h",
				GarbageRatio: "0.30",
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "PEBBLE_VALUE_SEPARATION", "true")
		assertEnv(t, envs, "PEBBLE_VALUE_SEPARATION_MIN_SIZE", "512")
		assertEnv(t, envs, "PEBBLE_VALUE_SEPARATION_MAX_DEPTH", "8")
		assertEnv(t, envs, "PEBBLE_VALUE_SEPARATION_REWRITE_AGE", "2h")
		assertEnv(t, envs, "PEBBLE_VALUE_SEPARATION_GARBAGE_RATIO", "0.30")
	})

	t.Run("nil value separation omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Pebble = &ledgerv1alpha1.PebbleConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "PEBBLE_VALUE_SEPARATION")
		assertNoEnv(t, envs, "PEBBLE_VALUE_SEPARATION_MIN_SIZE")
	})

	t.Run("partial config", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		bTrue := true
		ls.Spec.Pebble = &ledgerv1alpha1.PebbleConfig{
			ValueSeparation: &ledgerv1alpha1.PebbleValueSeparationConfig{
				Enabled: &bTrue,
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "PEBBLE_VALUE_SEPARATION", "true")
		assertNoEnv(t, envs, "PEBBLE_VALUE_SEPARATION_MIN_SIZE")
		assertNoEnv(t, envs, "PEBBLE_VALUE_SEPARATION_MAX_DEPTH")
		assertNoEnv(t, envs, "PEBBLE_VALUE_SEPARATION_REWRITE_AGE")
		assertNoEnv(t, envs, "PEBBLE_VALUE_SEPARATION_GARBAGE_RATIO")
	})
}

// ---------------------------------------------------------------------------
// Raft processing tick interval
// ---------------------------------------------------------------------------

func TestBuildEnvVars_RaftProcessingTickInterval(t *testing.T) {
	t.Parallel()

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Raft = &ledgerv1alpha1.RaftConfig{
			ProcessingTickInterval: "10ms",
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "RAFT_PROCESSING_TICK_INTERVAL", "10ms")
	})

	t.Run("omitted when empty", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Raft = &ledgerv1alpha1.RaftConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "RAFT_PROCESSING_TICK_INTERVAL")
	})
}

// ---------------------------------------------------------------------------
// Raft transport buffer size
// ---------------------------------------------------------------------------

func TestBuildEnvVars_RaftTransportBufferSize(t *testing.T) {
	t.Parallel()

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		bufSize := resource.MustParse("20Mi")
		ls.Spec.Raft = &ledgerv1alpha1.RaftConfig{
			Transport: &ledgerv1alpha1.RaftTransportConfig{
				BufferSize: &bufSize,
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "RAFT_TRANSPORT_BUFFER_SIZE", "20Mi")
	})

	t.Run("nil omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Raft = &ledgerv1alpha1.RaftConfig{
			Transport: &ledgerv1alpha1.RaftTransportConfig{},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "RAFT_TRANSPORT_BUFFER_SIZE")
	})
}

// ---------------------------------------------------------------------------
// Auth: issuers, checkScopes, readKeySetMaxRetries
// ---------------------------------------------------------------------------

func TestBuildEnvVars_AuthIssuers(t *testing.T) {
	t.Parallel()

	t.Run("multiple issuers joined", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{
			Issuers: []string{"https://issuer1.example.com", "https://issuer2.example.com"},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "AUTH_ISSUERS", "https://issuer1.example.com,https://issuer2.example.com")
	})

	t.Run("single issuer", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{
			Issuers: []string{"https://auth.example.com"},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "AUTH_ISSUERS", "https://auth.example.com")
	})

	t.Run("empty issuers omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "AUTH_ISSUERS")
	})
}

func TestBuildEnvVars_AuthCheckScopes(t *testing.T) {
	t.Parallel()

	ls := newMinimalCluster()
	b := true
	ls.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{
		CheckScopes: &b,
	}
	envs := buildEnvVars(ls, "disabled", nil)
	assertEnv(t, envs, "AUTH_CHECK_SCOPES", "true")
}

func TestBuildEnvVars_AuthReadKeySetMaxRetries(t *testing.T) {
	t.Parallel()

	ls := newMinimalCluster()
	v := int32(5)
	ls.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{
		ReadKeySetMaxRetries: &v,
	}
	envs := buildEnvVars(ls, "disabled", nil)
	assertEnv(t, envs, "AUTH_READ_KEY_SET_MAX_RETRIES", "5")
}

// ---------------------------------------------------------------------------
// Receipt signing (secretKeyRef)
// ---------------------------------------------------------------------------

func TestBuildEnvVars_ReceiptSigning(t *testing.T) {
	t.Parallel()

	t.Run("with explicit secret key", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.ReceiptSigning = &ledgerv1alpha1.ReceiptSigningConfig{
			SecretName: "receipt-hmac",
			SecretKey:  "hmac-key",
		}
		envs := buildEnvVars(ls, "disabled", nil)
		e := findEnv(envs, "RECEIPT_SIGNING_KEY")
		require.NotNil(t, e, "RECEIPT_SIGNING_KEY should be present")
		require.NotNil(t, e.ValueFrom, "should use ValueFrom")
		require.NotNil(t, e.ValueFrom.SecretKeyRef, "should use SecretKeyRef")
		assert.Equal(t, "receipt-hmac", e.ValueFrom.SecretKeyRef.Name)
		assert.Equal(t, "hmac-key", e.ValueFrom.SecretKeyRef.Key)
	})

	t.Run("default secret key", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.ReceiptSigning = &ledgerv1alpha1.ReceiptSigningConfig{
			SecretName: "receipt-hmac",
		}
		envs := buildEnvVars(ls, "disabled", nil)
		e := findEnv(envs, "RECEIPT_SIGNING_KEY")
		require.NotNil(t, e)
		assert.Equal(t, "key", e.ValueFrom.SecretKeyRef.Key)
	})

	t.Run("nil omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "RECEIPT_SIGNING_KEY")
	})

	t.Run("empty secret name omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.ReceiptSigning = &ledgerv1alpha1.ReceiptSigningConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "RECEIPT_SIGNING_KEY")
	})
}

// ---------------------------------------------------------------------------
// Bloom filters
// ---------------------------------------------------------------------------

func TestBuildEnvVars_BloomFilters(t *testing.T) {
	t.Parallel()

	t.Run("single type", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		expectedKeys := int64(100000)
		ls.Spec.Bloom = &ledgerv1alpha1.BloomConfig{
			Volumes: &ledgerv1alpha1.BloomFilterConfig{
				ExpectedKeys: &expectedKeys,
				FPRate:       "0.01",
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "BLOOM_VOLUMES_EXPECTED_KEYS", "100000")
		assertEnv(t, envs, "BLOOM_VOLUMES_FP_RATE", "0.01")
		// Other types should not be present.
		assertNoEnv(t, envs, "BLOOM_METADATA_EXPECTED_KEYS")
		assertNoEnv(t, envs, "BLOOM_TRANSACTIONS_EXPECTED_KEYS")
	})

	t.Run("all types", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		keys := int64(50000)
		bf := &ledgerv1alpha1.BloomFilterConfig{ExpectedKeys: &keys, FPRate: "0.001"}
		ls.Spec.Bloom = &ledgerv1alpha1.BloomConfig{
			Volumes:      bf,
			Metadata:     bf,
			References:   bf,
			Ledgers:      bf,
			Boundaries:   bf,
			Transactions: bf,
		}
		envs := buildEnvVars(ls, "disabled", nil)
		for _, prefix := range []string{
			"BLOOM_VOLUMES", "BLOOM_METADATA",
			"BLOOM_REFERENCES", "BLOOM_LEDGERS", "BLOOM_BOUNDARIES",
			"BLOOM_TRANSACTIONS",
		} {
			assertEnv(t, envs, prefix+"_EXPECTED_KEYS", "50000")
			assertEnv(t, envs, prefix+"_FP_RATE", "0.001")
		}
	})

	t.Run("nil bloom omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "BLOOM_VOLUMES_EXPECTED_KEYS")
	})

	t.Run("partial — only expectedKeys", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		keys := int64(10000)
		ls.Spec.Bloom = &ledgerv1alpha1.BloomConfig{
			Metadata: &ledgerv1alpha1.BloomFilterConfig{
				ExpectedKeys: &keys,
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "BLOOM_METADATA_EXPECTED_KEYS", "10000")
		assertNoEnv(t, envs, "BLOOM_METADATA_FP_RATE")
	})
}

// ---------------------------------------------------------------------------
// Idempotency TTL and eviction interval
// ---------------------------------------------------------------------------

func TestBuildEnvVars_IdempotencyTTL(t *testing.T) {
	t.Parallel()

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.IdempotencyTTL = "48h"
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "IDEMPOTENCY_TTL", "48h")
	})

	t.Run("omitted when empty", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "IDEMPOTENCY_TTL")
	})
}

func TestBuildEnvVars_IdempotencyEvictionInterval(t *testing.T) {
	t.Parallel()

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.IdempotencyEvictionInterval = "120s"
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "IDEMPOTENCY_EVICTION_INTERVAL", "120s")
	})

	t.Run("omitted when empty", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "IDEMPOTENCY_EVICTION_INTERVAL")
	})
}

// ---------------------------------------------------------------------------
// Extra env vars
// ---------------------------------------------------------------------------

func TestBuildEnvVars_ExtraEnv(t *testing.T) {
	t.Parallel()

	t.Run("appended last", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.ExtraEnv = []corev1.EnvVar{
			{Name: "CUSTOM_VAR", Value: "custom-value"},
			{Name: "ANOTHER", Value: "val"},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "CUSTOM_VAR", "custom-value")
		assertEnv(t, envs, "ANOTHER", "val")
	})

	t.Run("can override standard vars", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.ExtraEnv = []corev1.EnvVar{
			{Name: "DEBUG", Value: "true"},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		// ExtraEnv is appended last, so the last DEBUG wins in container spec
		var debugCount int
		for _, e := range envs {
			if e.Name == "DEBUG" {
				debugCount++
			}
		}
		assert.GreaterOrEqual(t, debugCount, 2, "DEBUG should appear from both standard and extra")
	})

	t.Run("empty list", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.ExtraEnv = []corev1.EnvVar{}
		envs := buildEnvVars(ls, "disabled", nil)
		// Should not panic, standard vars still present
		assertEnv(t, envs, "BIND_ADDR", "0.0.0.0:7777")
	})
}

// ---------------------------------------------------------------------------
// Raft replay batch size
// ---------------------------------------------------------------------------

func TestBuildEnvVars_RaftReplayBatchSize(t *testing.T) {
	t.Parallel()

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		v := int32(5000)
		ls.Spec.Raft = &ledgerv1alpha1.RaftConfig{
			ReplayBatchSize: &v,
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "RAFT_REPLAY_BATCH_SIZE", "5000")
	})

	t.Run("nil omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Raft = &ledgerv1alpha1.RaftConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "RAFT_REPLAY_BATCH_SIZE")
	})
}

// ---------------------------------------------------------------------------
// Pebble incremental compact threshold
// ---------------------------------------------------------------------------

func TestBuildEnvVars_PebbleIncrementalCompactThreshold(t *testing.T) {
	t.Parallel()

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		v := int64(50000)
		ls.Spec.Pebble = &ledgerv1alpha1.PebbleConfig{
			IncrementalCompactThreshold: &v,
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "PEBBLE_INCREMENTAL_COMPACT_THRESHOLD", "50000")
	})

	t.Run("nil omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Pebble = &ledgerv1alpha1.PebbleConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "PEBBLE_INCREMENTAL_COMPACT_THRESHOLD")
	})
}

// ---------------------------------------------------------------------------
// Flight recorder
// ---------------------------------------------------------------------------

func TestBuildEnvVars_FlightRecorder(t *testing.T) {
	t.Parallel()

	t.Run("enabled with all fields", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		maxBytes := resource.MustParse("10Mi")
		ls.Spec.Monitoring = &ledgerv1alpha1.MonitoringConfig{
			FlightRecorder: &ledgerv1alpha1.FlightRecorderConfig{
				Enabled:  true,
				MinAge:   "30s",
				MaxBytes: &maxBytes,
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "FLIGHT_RECORDER_ENABLED", "true")
		assertEnv(t, envs, "FLIGHT_RECORDER_MIN_AGE", "30s")
		assertEnv(t, envs, "FLIGHT_RECORDER_MAX_BYTES", "10Mi")
	})

	t.Run("disabled omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Monitoring = &ledgerv1alpha1.MonitoringConfig{
			FlightRecorder: &ledgerv1alpha1.FlightRecorderConfig{
				Enabled: false,
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "FLIGHT_RECORDER_ENABLED")
	})

	t.Run("nil omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Monitoring = &ledgerv1alpha1.MonitoringConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "FLIGHT_RECORDER_ENABLED")
	})
}

// ---------------------------------------------------------------------------
// Auth scope mapping
// ---------------------------------------------------------------------------

func TestBuildEnvVars_AuthScopeMapping(t *testing.T) {
	t.Parallel()

	t.Run("serialized as JSON", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{
			ScopeMapping: map[string][]string{
				"ledger:read":  {"ledger:TransactionRead", "ledger:AccountRead"},
				"ledger:write": {"ledger:TransactionWrite"},
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		e := findEnv(envs, "AUTH_SCOPE_MAPPING")
		require.NotNil(t, e, "AUTH_SCOPE_MAPPING should be present")
		assert.Contains(t, e.Value, "ledger:read")
		assert.Contains(t, e.Value, "ledger:write")
	})

	t.Run("empty mapping omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "AUTH_SCOPE_MAPPING")
	})
}

// ---------------------------------------------------------------------------
// Auth anonymous scopes (writes-only mode)
// ---------------------------------------------------------------------------

func TestBuildEnvVars_AuthAnonymousScopes(t *testing.T) {
	t.Parallel()

	t.Run("wildcard read", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{
			AnonymousScopes: []string{"*:read"},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		e := findEnv(envs, "AUTH_ANONYMOUS_SCOPES")
		require.NotNil(t, e, "AUTH_ANONYMOUS_SCOPES should be present")
		assert.Equal(t, "*:read", e.Value)
	})

	t.Run("explicit list joined by comma", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{
			AnonymousScopes: []string{"ledger:LedgerRead", "ledger:AccountRead"},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		e := findEnv(envs, "AUTH_ANONYMOUS_SCOPES")
		require.NotNil(t, e)
		assert.Equal(t, "ledger:LedgerRead,ledger:AccountRead", e.Value)
	})

	t.Run("empty slice omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{
			AnonymousScopes: []string{},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "AUTH_ANONYMOUS_SCOPES")
	})

	t.Run("nil auth omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "AUTH_ANONYMOUS_SCOPES")
	})
}

// ---------------------------------------------------------------------------
// Read index batch size
// ---------------------------------------------------------------------------

func TestBuildEnvVars_ReadIndexBatchSize(t *testing.T) {
	t.Parallel()

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		v := int32(2000)
		ls.Spec.ReadIndex = &ledgerv1alpha1.ReadIndexConfig{
			BatchSize: &v,
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "READ_INDEX_BATCH_SIZE", "2000")
	})

	t.Run("nil omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.ReadIndex = &ledgerv1alpha1.ReadIndexConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "READ_INDEX_BATCH_SIZE")
	})
}

// ---------------------------------------------------------------------------
// Health resume thresholds (disk-usage hysteresis)
// ---------------------------------------------------------------------------

func TestBuildEnvVars_HealthResumeThresholds(t *testing.T) {
	t.Parallel()

	t.Run("wal resume threshold set", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Health = &ledgerv1alpha1.HealthConfig{
			WalResumeThreshold: "0.65",
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "HEALTH_WAL_RESUME_THRESHOLD", "0.65")
		assertNoEnv(t, envs, "HEALTH_DATA_RESUME_THRESHOLD")
	})

	t.Run("data resume threshold set", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Health = &ledgerv1alpha1.HealthConfig{
			DataResumeThreshold: "0.70",
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "HEALTH_DATA_RESUME_THRESHOLD", "0.70")
		assertNoEnv(t, envs, "HEALTH_WAL_RESUME_THRESHOLD")
	})

	t.Run("both resume thresholds set alongside block thresholds", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Health = &ledgerv1alpha1.HealthConfig{
			WalThreshold:        "0.90",
			DataThreshold:       "0.85",
			WalResumeThreshold:  "0.75",
			DataResumeThreshold: "0.70",
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "HEALTH_WAL_THRESHOLD", "0.90")
		assertEnv(t, envs, "HEALTH_DATA_THRESHOLD", "0.85")
		assertEnv(t, envs, "HEALTH_WAL_RESUME_THRESHOLD", "0.75")
		assertEnv(t, envs, "HEALTH_DATA_RESUME_THRESHOLD", "0.70")
	})

	t.Run("omitted when empty", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Health = &ledgerv1alpha1.HealthConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "HEALTH_WAL_RESUME_THRESHOLD")
		assertNoEnv(t, envs, "HEALTH_DATA_RESUME_THRESHOLD")
	})

	t.Run("nil health omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "HEALTH_WAL_RESUME_THRESHOLD")
		assertNoEnv(t, envs, "HEALTH_DATA_RESUME_THRESHOLD")
	})
}

// ---------------------------------------------------------------------------
// Full config: all new fields together
// ---------------------------------------------------------------------------

func TestBuildEnvVars_AllNewFields(t *testing.T) {
	t.Parallel()

	ls := newMinimalCluster()

	bTrue := true
	numscriptCache := int32(4096)
	mirrorBatch := int32(200)
	bufSize := resource.MustParse("10Mi")
	authRetries := int32(15)
	bloomKeys := int64(500000)
	vsMinSize := resource.MustParse("1Ki")
	vsMaxDepth := int32(6)

	ls.Spec.SentinelMode = &bTrue
	ls.Spec.GrpcCompression = &bTrue
	ls.Spec.QueryProfileThreshold = "100ms"
	ls.Spec.GrpcSlowThreshold = "5s"
	ls.Spec.NumscriptCacheSize = &numscriptCache
	ls.Spec.MirrorMaxBatchSize = &mirrorBatch
	ls.Spec.ReceiptSigning = &ledgerv1alpha1.ReceiptSigningConfig{
		SecretName: "hmac-secret",
		SecretKey:  "signing-key",
	}
	ls.Spec.Pebble = &ledgerv1alpha1.PebbleConfig{
		Compression: "zstd,zstd,zstd,zstd,zstd,zstd,zstd",
		ValueSeparation: &ledgerv1alpha1.PebbleValueSeparationConfig{
			Enabled:      &bTrue,
			MinSize:      &vsMinSize,
			MaxDepth:     &vsMaxDepth,
			RewriteAge:   "30m",
			GarbageRatio: "0.15",
		},
	}
	ls.Spec.Raft = &ledgerv1alpha1.RaftConfig{
		ProcessingTickInterval: "15ms",
		Transport: &ledgerv1alpha1.RaftTransportConfig{
			BufferSize: &bufSize,
		},
	}
	ls.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{
		Issuers:              []string{"https://a.example.com", "https://b.example.com"},
		CheckScopes:          &bTrue,
		ReadKeySetMaxRetries: &authRetries,
	}
	ls.Spec.Bloom = &ledgerv1alpha1.BloomConfig{
		Volumes: &ledgerv1alpha1.BloomFilterConfig{
			ExpectedKeys: &bloomKeys,
			FPRate:       "0.01",
		},
	}
	ls.Spec.ReadIndex = &ledgerv1alpha1.ReadIndexConfig{
		Pebble: &ledgerv1alpha1.PebbleConfig{
			Compression: "none,snappy,zstd,zstd,zstd,zstd,zstd",
		},
	}

	envs := buildEnvVars(ls, "disabled", nil)

	// Sentinel mode
	assertEnv(t, envs, "SENTINEL_MODE", "true")
	assertNoEnv(t, envs, "VOLUME_ASSERTIONS")

	// gRPC compression
	assertEnv(t, envs, "GRPC_COMPRESSION", "true")

	// Thresholds
	assertEnv(t, envs, "QUERY_PROFILE_THRESHOLD", "100ms")
	assertEnv(t, envs, "GRPC_SLOW_THRESHOLD", "5s")

	// Cache sizes
	assertEnv(t, envs, "NUMSCRIPT_CACHE_SIZE", "4096")
	assertEnv(t, envs, "MIRROR_MAX_BATCH_SIZE", "200")

	// Receipt signing
	e := findEnv(envs, "RECEIPT_SIGNING_KEY")
	require.NotNil(t, e)
	assert.Equal(t, "hmac-secret", e.ValueFrom.SecretKeyRef.Name)
	assert.Equal(t, "signing-key", e.ValueFrom.SecretKeyRef.Key)

	// Pebble compression
	assertEnv(t, envs, "PEBBLE_COMPRESSION", "zstd,zstd,zstd,zstd,zstd,zstd,zstd")

	// Pebble value separation
	assertEnv(t, envs, "PEBBLE_VALUE_SEPARATION", "true")
	assertEnv(t, envs, "PEBBLE_VALUE_SEPARATION_MIN_SIZE", "1Ki")
	assertEnv(t, envs, "PEBBLE_VALUE_SEPARATION_MAX_DEPTH", "6")
	assertEnv(t, envs, "PEBBLE_VALUE_SEPARATION_REWRITE_AGE", "30m")
	assertEnv(t, envs, "PEBBLE_VALUE_SEPARATION_GARBAGE_RATIO", "0.15")

	// Raft additions
	assertEnv(t, envs, "RAFT_PROCESSING_TICK_INTERVAL", "15ms")
	assertEnv(t, envs, "RAFT_TRANSPORT_BUFFER_SIZE", "10Mi")

	// Auth additions
	assertEnv(t, envs, "AUTH_ISSUERS", "https://a.example.com,https://b.example.com")
	assertEnv(t, envs, "AUTH_CHECK_SCOPES", "true")
	assertEnv(t, envs, "AUTH_READ_KEY_SET_MAX_RETRIES", "15")

	// Bloom filters
	assertEnv(t, envs, "BLOOM_VOLUMES_EXPECTED_KEYS", "500000")
	assertEnv(t, envs, "BLOOM_VOLUMES_FP_RATE", "0.01")

	// Read index compression
	assertEnv(t, envs, "READ_INDEX_COMPRESSION", "none,snappy,zstd,zstd,zstd,zstd,zstd")
}

// ---------------------------------------------------------------------------
// appendBloomEnvVars unit test
// ---------------------------------------------------------------------------

func TestAppendBloomEnvVars(t *testing.T) {
	t.Parallel()

	keys1 := int64(100)
	keys2 := int64(200)
	keys3 := int64(300)
	keys4 := int64(400)
	bloom := &ledgerv1alpha1.BloomConfig{
		Volumes: &ledgerv1alpha1.BloomFilterConfig{
			ExpectedKeys: &keys1,
			FPRate:       "0.05",
		},
		Transactions: &ledgerv1alpha1.BloomFilterConfig{
			ExpectedKeys: &keys2,
		},
		LedgerMetadata: &ledgerv1alpha1.BloomFilterConfig{
			ExpectedKeys: &keys3,
			FPRate:       "0.001",
		},
		PreparedQueries: &ledgerv1alpha1.BloomFilterConfig{
			ExpectedKeys: &keys4,
			FPRate:       "0.002",
		},
	}

	envs := appendBloomEnvVars(nil, bloom)

	assertEnv(t, envs, "BLOOM_VOLUMES_EXPECTED_KEYS", "100")
	assertEnv(t, envs, "BLOOM_VOLUMES_FP_RATE", "0.05")
	assertEnv(t, envs, "BLOOM_TRANSACTIONS_EXPECTED_KEYS", "200")
	assertNoEnv(t, envs, "BLOOM_TRANSACTIONS_FP_RATE")
	assertEnv(t, envs, "BLOOM_LEDGER_METADATA_EXPECTED_KEYS", "300")
	assertEnv(t, envs, "BLOOM_LEDGER_METADATA_FP_RATE", "0.001")
	assertEnv(t, envs, "BLOOM_PREPARED_QUERIES_EXPECTED_KEYS", "400")
	assertEnv(t, envs, "BLOOM_PREPARED_QUERIES_FP_RATE", "0.002")
	assertNoEnv(t, envs, "BLOOM_METADATA_EXPECTED_KEYS")
	assertNoEnv(t, envs, "BLOOM_IDEMPOTENCY_EXPECTED_KEYS")
	assertNoEnv(t, envs, "BLOOM_REFERENCES_EXPECTED_KEYS")
	assertNoEnv(t, envs, "BLOOM_LEDGERS_EXPECTED_KEYS")
	assertNoEnv(t, envs, "BLOOM_BOUNDARIES_EXPECTED_KEYS")
}

// ---------------------------------------------------------------------------
// appendPebbleEnvVars compression
// ---------------------------------------------------------------------------

func TestAppendPebbleEnvVars_Compression(t *testing.T) {
	t.Parallel()

	t.Run("PEBBLE prefix", func(t *testing.T) {
		t.Parallel()
		p := &ledgerv1alpha1.PebbleConfig{Compression: "snappy"}
		envs := appendPebbleEnvVars(nil, "PEBBLE", p)
		assertEnv(t, envs, "PEBBLE_COMPRESSION", "snappy")
	})

	t.Run("READ_INDEX prefix", func(t *testing.T) {
		t.Parallel()
		p := &ledgerv1alpha1.PebbleConfig{Compression: "zstd"}
		envs := appendPebbleEnvVars(nil, "READ_INDEX", p)
		assertEnv(t, envs, "READ_INDEX_COMPRESSION", "zstd")
	})

	t.Run("empty compression omitted", func(t *testing.T) {
		t.Parallel()
		p := &ledgerv1alpha1.PebbleConfig{}
		envs := appendPebbleEnvVars(nil, "PEBBLE", p)
		assertNoEnv(t, envs, "PEBBLE_COMPRESSION")
	})
}

// ---------------------------------------------------------------------------
// GOMEMLIMIT
// ---------------------------------------------------------------------------

func TestBuildEnvVars_GoMemLimit(t *testing.T) {
	t.Parallel()

	t.Run("default ratio 90%", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Resources = corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		// 2Gi = 2147483648, 90% = 1932735283
		assertEnv(t, envs, "GOMEMLIMIT", "1932735283")
	})

	t.Run("custom ratio", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ratio := int32(80)
		ls.Spec.GoMemLimitRatio = &ratio
		ls.Spec.Resources = corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		// 2Gi = 2147483648, 80% = 1717986918
		assertEnv(t, envs, "GOMEMLIMIT", "1717986918")
	})

	t.Run("no memory limit", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "GOMEMLIMIT")
	})

	t.Run("ratio zero disables", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ratio := int32(0)
		ls.Spec.GoMemLimitRatio = &ratio
		ls.Spec.Resources = corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "GOMEMLIMIT", "0")
	})
}

// ---------------------------------------------------------------------------
// Hash algorithm
// ---------------------------------------------------------------------------

func TestBuildEnvVars_HashAlgorithm(t *testing.T) {
	t.Parallel()

	t.Run("blake3", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.HashAlgorithm = "blake3"
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "HASH_ALGORITHM", "blake3")
	})

	t.Run("xxh3", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.HashAlgorithm = "xxh3"
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "HASH_ALGORITHM", "xxh3")
	})

	t.Run("omitted when empty", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "HASH_ALGORITHM")
	})
}

// ---------------------------------------------------------------------------
// Unsafe skip config validation
// ---------------------------------------------------------------------------

func TestBuildEnvVars_UnsafeSkipConfigValidation(t *testing.T) {
	t.Parallel()

	t.Run("set to true", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		b := true
		ls.Spec.UnsafeSkipConfigValidation = &b
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "UNSAFE_SKIP_CONFIG_VALIDATION", "true")
	})

	t.Run("set to false", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		b := false
		ls.Spec.UnsafeSkipConfigValidation = &b
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "UNSAFE_SKIP_CONFIG_VALIDATION", "false")
	})

	t.Run("nil omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "UNSAFE_SKIP_CONFIG_VALIDATION")
	})
}

// ---------------------------------------------------------------------------
// Snapshot sync
// ---------------------------------------------------------------------------

func TestBuildEnvVars_Snapshot(t *testing.T) {
	t.Parallel()

	t.Run("full config", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		parallelism := int32(8)
		retryCount := int32(10)
		fileRetryCount := int32(5)
		ls.Spec.Snapshot = &ledgerv1alpha1.SnapshotConfig{
			SessionTTL:     "10m",
			Parallelism:    &parallelism,
			RetryCount:     &retryCount,
			FileRetryCount: &fileRetryCount,
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "SNAPSHOT_SESSION_TTL", "10m")
		assertEnv(t, envs, "SNAPSHOT_PARALLELISM", "8")
		assertEnv(t, envs, "SNAPSHOT_RETRY_COUNT", "10")
		assertEnv(t, envs, "SNAPSHOT_FILE_RETRY_COUNT", "5")
	})

	t.Run("partial config", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Snapshot = &ledgerv1alpha1.SnapshotConfig{
			SessionTTL: "3m",
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "SNAPSHOT_SESSION_TTL", "3m")
		assertNoEnv(t, envs, "SNAPSHOT_PARALLELISM")
		assertNoEnv(t, envs, "SNAPSHOT_RETRY_COUNT")
		assertNoEnv(t, envs, "SNAPSHOT_FILE_RETRY_COUNT")
	})

	t.Run("nil omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "SNAPSHOT_SESSION_TTL")
		assertNoEnv(t, envs, "SNAPSHOT_PARALLELISM")
		assertNoEnv(t, envs, "SNAPSHOT_RETRY_COUNT")
		assertNoEnv(t, envs, "SNAPSHOT_FILE_RETRY_COUNT")
	})
}

// ---------------------------------------------------------------------------
// Bloom filters: new types (sink-configs, numscript-versions, numscript-contents)
// ---------------------------------------------------------------------------

func TestBuildEnvVars_BloomFiltersNewTypes(t *testing.T) {
	t.Parallel()

	t.Run("sink-configs", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		keys := int64(10000)
		ls.Spec.Bloom = &ledgerv1alpha1.BloomConfig{
			SinkConfigs: &ledgerv1alpha1.BloomFilterConfig{
				ExpectedKeys: &keys,
				FPRate:       "0.01",
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "BLOOM_SINK_CONFIGS_EXPECTED_KEYS", "10000")
		assertEnv(t, envs, "BLOOM_SINK_CONFIGS_FP_RATE", "0.01")
	})

	t.Run("numscript-versions", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		keys := int64(5000)
		ls.Spec.Bloom = &ledgerv1alpha1.BloomConfig{
			NumscriptVersions: &ledgerv1alpha1.BloomFilterConfig{
				ExpectedKeys: &keys,
				FPRate:       "0.005",
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "BLOOM_NUMSCRIPT_VERSIONS_EXPECTED_KEYS", "5000")
		assertEnv(t, envs, "BLOOM_NUMSCRIPT_VERSIONS_FP_RATE", "0.005")
	})

	t.Run("numscript-contents", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		keys := int64(8000)
		ls.Spec.Bloom = &ledgerv1alpha1.BloomConfig{
			NumscriptContents: &ledgerv1alpha1.BloomFilterConfig{
				ExpectedKeys: &keys,
				FPRate:       "0.02",
			},
		}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "BLOOM_NUMSCRIPT_CONTENTS_EXPECTED_KEYS", "8000")
		assertEnv(t, envs, "BLOOM_NUMSCRIPT_CONTENTS_FP_RATE", "0.02")
	})
}

// ---------------------------------------------------------------------------
// ADVERTISE_ADDR — regression: must use the Raft port (BindAddr), not the
// service gRPC port. Sending peers to GRPC_PORT routes Raft traffic to the
// BucketService listener, which doesn't register raft_transport — every
// AppendEntries comes back Unimplemented and the cluster degrades silently.
// ---------------------------------------------------------------------------

func TestBuildEnvVars_AdvertiseAddr_UsesRaftPort(t *testing.T) {
	t.Parallel()

	t.Run("default BindAddr extracts 7777", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "ADVERTISE_ADDR",
			"$(POD_NAME).ledger-test-headless.$(POD_NAMESPACE).svc.cluster.local:7777")
	})

	t.Run("custom Raft port is honoured", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.BindAddr = "0.0.0.0:9001"
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "ADVERTISE_ADDR",
			"$(POD_NAME).ledger-test-headless.$(POD_NAMESPACE).svc.cluster.local:9001")
	})

	t.Run("must not use GrpcPort even when BindAddr is empty", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.BindAddr = ""
		ls.Spec.GrpcPort = 12345
		envs := buildEnvVars(ls, "disabled", nil)
		e := findEnv(envs, "ADVERTISE_ADDR")
		require.NotNil(t, e)
		require.NotContains(t, e.Value, "12345",
			"ADVERTISE_ADDR must never reuse GRPC_PORT — Raft transport lives on a different listener")
	})
}

func TestRaftPortFromBindAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bindAddr string
		want     int32
	}{
		{"default", "0.0.0.0:7777", 7777},
		{"localhost", "127.0.0.1:9999", 9999},
		{"empty falls back", "", 7777},
		{"malformed falls back", "not-an-addr", 7777},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, raftPortFromBindAddr(tt.bindAddr))
		})
	}
}
