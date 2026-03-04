package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func TestApplyDefaultsFromRef_EmptySpec(t *testing.T) {
	t.Parallel()

	boolTrue := true
	cacheSize := int64(1073741824)

	defaults := &ledgerv1alpha1.LedgerDefaultsSpec{
		Image: ledgerv1alpha1.ImageSpec{
			Repository: "custom-repo/ledger",
			Tag:        "v2.0",
			PullPolicy: corev1.PullAlways,
		},
		ServiceAccount: ledgerv1alpha1.ServiceAccountSpec{
			Create: &boolTrue,
			Name:   "shared-sa",
		},
		Config: ledgerv1alpha1.LedgerDefaultsConfig{
			Pebble: &ledgerv1alpha1.PebbleConfig{
				CacheSize: &cacheSize,
			},
			TLS: &ledgerv1alpha1.TLSConfig{
				Enabled:    true,
				SecretName: "tls-secret",
			},
			Monitoring: &ledgerv1alpha1.MonitoringConfig{
				ServiceName: "production-ledger",
			},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2000m"),
				corev1.ResourceMemory: resource.MustParse("4Gi"),
			},
		},
		NodeSelector: map[string]string{"tier": "production"},
		Tolerations: []corev1.Toleration{
			{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "ledger"},
		},
	}

	spec := &ledgerv1alpha1.LedgerServiceSpec{}
	applyDefaultsFromRef(spec, defaults)

	assert.Equal(t, "custom-repo/ledger", spec.Image.Repository)
	assert.Equal(t, "v2.0", spec.Image.Tag)
	assert.Equal(t, corev1.PullAlways, spec.Image.PullPolicy)
	assert.Equal(t, &boolTrue, spec.ServiceAccount.Create)
	assert.Equal(t, "shared-sa", spec.ServiceAccount.Name)
	assert.Equal(t, &cacheSize, spec.Config.Pebble.CacheSize)
	assert.True(t, spec.Config.TLS.Enabled)
	assert.Equal(t, "tls-secret", spec.Config.TLS.SecretName)
	assert.Equal(t, "production-ledger", spec.Config.Monitoring.ServiceName)
	assert.Equal(t, resource.MustParse("2000m"), spec.Resources.Requests[corev1.ResourceCPU])
	assert.Equal(t, map[string]string{"tier": "production"}, spec.NodeSelector)
	assert.Len(t, spec.Tolerations, 1)
}

func TestApplyDefaultsFromRef_SpecOverridesDefaults(t *testing.T) {
	t.Parallel()

	cacheSize := int64(1073741824)

	defaults := &ledgerv1alpha1.LedgerDefaultsSpec{
		Image: ledgerv1alpha1.ImageSpec{
			Repository: "default-repo/ledger",
			Tag:        "v1.0",
			PullPolicy: corev1.PullAlways,
		},
		Config: ledgerv1alpha1.LedgerDefaultsConfig{
			Pebble: &ledgerv1alpha1.PebbleConfig{
				CacheSize: &cacheSize,
			},
			TLS: &ledgerv1alpha1.TLSConfig{
				Enabled:    true,
				SecretName: "default-tls",
			},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("1000m"),
			},
		},
		NodeSelector: map[string]string{"tier": "default"},
		Tolerations: []corev1.Toleration{
			{Key: "default-key"},
		},
	}

	localCacheSize := int64(2147483648)
	spec := &ledgerv1alpha1.LedgerServiceSpec{
		Image: ledgerv1alpha1.ImageSpec{
			Repository: "my-repo/ledger",
			Tag:        "v3.0",
		},
		Config: ledgerv1alpha1.LedgerServiceConfig{
			Pebble: &ledgerv1alpha1.PebbleConfig{
				CacheSize: &localCacheSize,
			},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("4000m"),
			},
		},
		NodeSelector: map[string]string{"env": "staging"},
		Tolerations: []corev1.Toleration{
			{Key: "local-key"},
		},
	}

	applyDefaultsFromRef(spec, defaults)

	// Spec-level values win.
	assert.Equal(t, "my-repo/ledger", spec.Image.Repository)
	assert.Equal(t, "v3.0", spec.Image.Tag)
	// PullPolicy was empty in spec, so default fills it.
	assert.Equal(t, corev1.PullAlways, spec.Image.PullPolicy)
	// Pebble is non-nil in spec, whole-block replacement — spec wins.
	assert.Equal(t, &localCacheSize, spec.Config.Pebble.CacheSize)
	// TLS was nil in spec, so default fills it.
	assert.True(t, spec.Config.TLS.Enabled)
	assert.Equal(t, "default-tls", spec.Config.TLS.SecretName)
	// Resources: spec Requests non-nil, so spec wins.
	assert.Equal(t, resource.MustParse("4000m"), spec.Resources.Requests[corev1.ResourceCPU])
	// Maps/slices: spec non-nil wins.
	assert.Equal(t, map[string]string{"env": "staging"}, spec.NodeSelector)
	assert.Len(t, spec.Tolerations, 1)
	assert.Equal(t, "local-key", spec.Tolerations[0].Key)
}

func TestApplyDefaultsFromRef_PartialMerge(t *testing.T) {
	t.Parallel()

	defaults := &ledgerv1alpha1.LedgerDefaultsSpec{
		Image: ledgerv1alpha1.ImageSpec{
			Repository: "default-repo",
		},
		Config: ledgerv1alpha1.LedgerDefaultsConfig{
			Health: &ledgerv1alpha1.HealthConfig{
				Interval: "30s",
			},
		},
	}

	replicas := int32(5)
	spec := &ledgerv1alpha1.LedgerServiceSpec{
		Replicas: &replicas,
		Image: ledgerv1alpha1.ImageSpec{
			Tag: "custom-tag",
		},
	}

	applyDefaultsFromRef(spec, defaults)

	// Repository from defaults, tag from spec, replicas unchanged.
	assert.Equal(t, "default-repo", spec.Image.Repository)
	assert.Equal(t, "custom-tag", spec.Image.Tag)
	assert.Equal(t, int32(5), *spec.Replicas)
	assert.Equal(t, "30s", spec.Config.Health.Interval)
}

func TestApplyDefaultsFromRef_NilSubStructs(t *testing.T) {
	t.Parallel()

	// Empty defaults should not change anything.
	defaults := &ledgerv1alpha1.LedgerDefaultsSpec{}
	replicas := int32(3)
	spec := &ledgerv1alpha1.LedgerServiceSpec{
		Replicas: &replicas,
		Image: ledgerv1alpha1.ImageSpec{
			Repository: "my-repo",
		},
	}

	applyDefaultsFromRef(spec, defaults)

	assert.Equal(t, "my-repo", spec.Image.Repository)
	assert.Equal(t, int32(3), *spec.Replicas)
	assert.Nil(t, spec.Config.Pebble)
	assert.Nil(t, spec.Config.TLS)
	assert.Nil(t, spec.Affinity)
	assert.Nil(t, spec.NodeSelector)
}

func TestApplyDefaultsFromRef_SecurityContext(t *testing.T) {
	t.Parallel()

	defaults := &ledgerv1alpha1.LedgerDefaultsSpec{
		PodSecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot: boolPtr(true),
		},
		SecurityContext: &corev1.SecurityContext{
			ReadOnlyRootFilesystem: boolPtr(true),
		},
	}

	spec := &ledgerv1alpha1.LedgerServiceSpec{}

	applyDefaultsFromRef(spec, defaults)

	// Security contexts from defaults.
	assert.True(t, *spec.PodSecurityContext.RunAsNonRoot)
	assert.True(t, *spec.SecurityContext.ReadOnlyRootFilesystem)
}

func TestApplyDefaultsFromRef_ColdStorageFieldLevelMerge(t *testing.T) {
	t.Parallel()

	defaults := &ledgerv1alpha1.LedgerDefaultsSpec{
		Config: ledgerv1alpha1.LedgerDefaultsConfig{
			ColdStorage: &ledgerv1alpha1.ColdStorageConfig{
				Driver: "s3",
				S3: &ledgerv1alpha1.S3Config{
					Bucket:   "default-bucket",
					Region:   "eu-west-1",
					Endpoint: "http://minio:9000",
				},
			},
		},
	}

	// LedgerService overrides driver and bucketId but not S3 — S3 should come from defaults.
	spec := &ledgerv1alpha1.LedgerServiceSpec{
		Config: ledgerv1alpha1.LedgerServiceConfig{
			ColdStorage: &ledgerv1alpha1.ColdStorageConfig{
				Driver:   "s3",
				BucketID: "my-prefix",
			},
		},
	}

	applyDefaultsFromRef(spec, defaults)

	assert.Equal(t, "s3", spec.Config.ColdStorage.Driver)
	assert.Equal(t, "my-prefix", spec.Config.ColdStorage.BucketID)
	assert.NotNil(t, spec.Config.ColdStorage.S3, "S3 block should be inherited from defaults")
	assert.Equal(t, "default-bucket", spec.Config.ColdStorage.S3.Bucket)
	assert.Equal(t, "eu-west-1", spec.Config.ColdStorage.S3.Region)
	assert.Equal(t, "http://minio:9000", spec.Config.ColdStorage.S3.Endpoint)
}

func TestApplyDefaultsFromRef_ColdStorageSpecS3Wins(t *testing.T) {
	t.Parallel()

	defaults := &ledgerv1alpha1.LedgerDefaultsSpec{
		Config: ledgerv1alpha1.LedgerDefaultsConfig{
			ColdStorage: &ledgerv1alpha1.ColdStorageConfig{
				Driver: "s3",
				S3: &ledgerv1alpha1.S3Config{
					Bucket: "default-bucket",
					Region: "us-east-1",
				},
			},
		},
	}

	// LedgerService has its own S3 block — it should win entirely.
	spec := &ledgerv1alpha1.LedgerServiceSpec{
		Config: ledgerv1alpha1.LedgerServiceConfig{
			ColdStorage: &ledgerv1alpha1.ColdStorageConfig{
				Driver: "s3",
				S3: &ledgerv1alpha1.S3Config{
					Bucket: "my-bucket",
					Region: "eu-west-1",
				},
			},
		},
	}

	applyDefaultsFromRef(spec, defaults)

	assert.Equal(t, "my-bucket", spec.Config.ColdStorage.S3.Bucket)
	assert.Equal(t, "eu-west-1", spec.Config.ColdStorage.S3.Region)
}

func TestApplyDefaultsFromRef_NetworkPolicy(t *testing.T) {
	t.Parallel()

	defaults := &ledgerv1alpha1.LedgerDefaultsSpec{
		NetworkPolicy: &ledgerv1alpha1.NetworkPolicySpec{
			Enabled:            true,
			ExternalCIDRExcept: []string{"10.0.0.0/8"},
		},
	}

	// Empty spec inherits NetworkPolicy from defaults.
	spec := &ledgerv1alpha1.LedgerServiceSpec{}
	applyDefaultsFromRef(spec, defaults)

	require.NotNil(t, spec.NetworkPolicy)
	assert.True(t, spec.NetworkPolicy.Enabled)
	assert.Equal(t, []string{"10.0.0.0/8"}, spec.NetworkPolicy.ExternalCIDRExcept)

	// Spec-level NetworkPolicy wins.
	spec2 := &ledgerv1alpha1.LedgerServiceSpec{
		NetworkPolicy: &ledgerv1alpha1.NetworkPolicySpec{
			Enabled: false,
		},
	}
	applyDefaultsFromRef(spec2, defaults)
	assert.False(t, spec2.NetworkPolicy.Enabled)
}

func boolPtr(v bool) *bool {
	return &v
}
