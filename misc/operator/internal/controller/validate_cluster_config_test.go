package controller

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestValidateSpec_NameLengthBoundary(t *testing.T) {
	t.Parallel()

	// The StatefulSet name ("ledger-" + name) is the tightest derived name:
	// pods carry controller-revision-hash=<statefulset>-<10-char hash>, a
	// label value capped at 63 chars, so the StatefulSet name is capped at 52
	// and a CR name of 45 chars is the longest accepted (7+45 = 52).
	require.Equal(t, 52, statefulSetNameMaxLength)
	name45 := strings.Repeat("a", 45)
	name46 := strings.Repeat("a", 46)

	require.Len(t, clusterBaseName(clusterNamed(name45)), statefulSetNameMaxLength)
	require.LessOrEqual(t, len(headlessServiceName(clusterNamed(name45))), dns1035LabelMaxLength)

	require.NoError(t, validateSpec(&ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: name45},
	}), "a 45-char name yields a 52-char StatefulSet name and must be accepted")

	err := validateSpec(&ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: name46},
	})
	require.Error(t, err, "a 46-char name yields a 53-char StatefulSet name and must be rejected")
	assert.Contains(t, err.Error(), clusterBaseName(clusterNamed(name46)))
	assert.Contains(t, err.Error(), "52-char limit")
	assert.Contains(t, err.Error(), "controller-revision-hash")
}

func TestValidateSpec_ResourceNameLengthBoundary(t *testing.T) {
	t.Parallel()

	// spec.resourceName is the StatefulSet name itself: 52 chars is the
	// longest accepted (controller-revision-hash label limit).
	rn52 := strings.Repeat("b", 52)
	rn53 := strings.Repeat("b", 53)

	require.NoError(t, validateSpec(clusterWithResourceName("x", rn52)),
		"a 52-char resourceName fits the StatefulSet name limit and must be accepted")

	err := validateSpec(clusterWithResourceName("x", rn53))
	require.Error(t, err, "a 53-char resourceName exceeds the StatefulSet name limit and must be rejected")
	assert.Contains(t, err.Error(), "resourceName")
	assert.Contains(t, err.Error(), rn53)
	assert.Contains(t, err.Error(), "52-char limit")
	assert.Contains(t, err.Error(), "controller-revision-hash")

	// The override bounds the derived names, not the CR name: a CR name too
	// long for the default "ledger-<name>" base is accepted with a short one.
	require.NoError(t, validateSpec(clusterWithResourceName(strings.Repeat("a", 60), "payments")))
}

// metadata.name is the app.kubernetes.io/instance label value of every derived
// object, so it is capped at 63 chars even when spec.resourceName keeps every
// derived name short.
func TestValidateSpec_NameLabelValueLimit(t *testing.T) {
	t.Parallel()

	name63 := strings.Repeat("a", 63)
	name64 := strings.Repeat("a", 64)

	require.NoError(t, validateSpec(clusterWithResourceName(name63, "payments")),
		"a 63-char name fits the label value limit and must be accepted with a short resourceName")

	err := validateSpec(clusterWithResourceName(name64, "payments"))
	require.Error(t, err, "a 64-char name exceeds the label value limit even with a short resourceName")
	assert.Contains(t, err.Error(), name64)
	assert.Contains(t, err.Error(), "63-char limit")
	assert.Contains(t, err.Error(), labelInstance)
}

func TestValidateClusterConfig_AcceptsNilAndEmpty(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateClusterConfig(&ledgerv1alpha1.ClusterSpec{}))
	require.NoError(t, validateClusterConfig(&ledgerv1alpha1.ClusterSpec{
		Cache: &ledgerv1alpha1.CacheConfig{},
		Bloom: &ledgerv1alpha1.BloomConfig{},
	}))
}

func TestValidateClusterConfig_AuthRequiresTLS(t *testing.T) {
	t.Parallel()

	enabled := true
	disabled := false

	tests := []struct {
		name    string
		auth    *ledgerv1alpha1.AuthorizationConfig
		tls     *ledgerv1alpha1.TLSConfig
		wantErr bool
	}{
		{
			name:    "auth enabled + tls enabled is accepted",
			auth:    &ledgerv1alpha1.AuthorizationConfig{Enabled: &enabled},
			tls:     &ledgerv1alpha1.TLSConfig{Enabled: true},
			wantErr: false,
		},
		{
			name:    "auth enabled + tls disabled is rejected",
			auth:    &ledgerv1alpha1.AuthorizationConfig{Enabled: &enabled},
			tls:     &ledgerv1alpha1.TLSConfig{Enabled: false},
			wantErr: true,
		},
		{
			name:    "auth enabled + tls nil is rejected",
			auth:    &ledgerv1alpha1.AuthorizationConfig{Enabled: &enabled},
			tls:     nil,
			wantErr: true,
		},
		{
			name:    "auth explicitly disabled + tls disabled is accepted",
			auth:    &ledgerv1alpha1.AuthorizationConfig{Enabled: &disabled},
			tls:     nil,
			wantErr: false,
		},
		{
			name:    "auth nil + tls disabled is accepted",
			auth:    nil,
			tls:     nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateClusterConfig(&ledgerv1alpha1.ClusterSpec{Auth: tt.auth, TLS: tt.tls})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "auth.enabled requires tls.enabled")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateClusterConfig_RejectsNonPositiveRotation(t *testing.T) {
	t.Parallel()
	zero := int32(0)
	neg := int32(-5)
	err := validateClusterConfig(&ledgerv1alpha1.ClusterSpec{
		Cache: &ledgerv1alpha1.CacheConfig{RotationThreshold: &zero},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rotationThreshold")
	err = validateClusterConfig(&ledgerv1alpha1.ClusterSpec{
		Cache: &ledgerv1alpha1.CacheConfig{RotationThreshold: &neg},
	})
	require.Error(t, err)
}

func TestValidateClusterConfig_AcceptsPositiveRotation(t *testing.T) {
	t.Parallel()
	v := int32(1000)
	require.NoError(t, validateClusterConfig(&ledgerv1alpha1.ClusterSpec{
		Cache: &ledgerv1alpha1.CacheConfig{RotationThreshold: &v},
	}))
}

func TestValidateClusterConfig_RejectsNegativeExpectedKeys(t *testing.T) {
	t.Parallel()
	bad := int64(-1)
	err := validateClusterConfig(&ledgerv1alpha1.ClusterSpec{
		Bloom: &ledgerv1alpha1.BloomConfig{
			Volumes: &ledgerv1alpha1.BloomFilterConfig{ExpectedKeys: &bad},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bloom.volumes.expectedKeys")
}

func TestValidateClusterConfig_AcceptsZeroExpectedKeys(t *testing.T) {
	t.Parallel()
	// Zero is explicitly the "disable this filter" sentinel.
	zero := int64(0)
	require.NoError(t, validateClusterConfig(&ledgerv1alpha1.ClusterSpec{
		Bloom: &ledgerv1alpha1.BloomConfig{
			Volumes: &ledgerv1alpha1.BloomFilterConfig{ExpectedKeys: &zero},
		},
	}))
}

func TestValidateClusterConfig_RejectsInvalidFPRate(t *testing.T) {
	t.Parallel()
	cases := []string{"0", "1", "-0.01", "1.01", "not-a-float"}
	for _, c := range cases {
		t.Run("fpRate="+c, func(t *testing.T) {
			t.Parallel()
			err := validateClusterConfig(&ledgerv1alpha1.ClusterSpec{
				Bloom: &ledgerv1alpha1.BloomConfig{
					Volumes: &ledgerv1alpha1.BloomFilterConfig{FPRate: c},
				},
			})
			require.Error(t, err)
		})
	}
}

func TestValidateClusterConfig_AcceptsValidFPRate(t *testing.T) {
	t.Parallel()
	for _, c := range []string{"0.001", "0.01", "0.05", "0.5", "0.999"} {
		require.NoError(t, validateClusterConfig(&ledgerv1alpha1.ClusterSpec{
			Bloom: &ledgerv1alpha1.BloomConfig{
				Volumes: &ledgerv1alpha1.BloomFilterConfig{FPRate: c},
			},
		}), "fpRate %s should be accepted", c)
	}
}

func TestValidateClusterConfig_CoversLedgerMetadata(t *testing.T) {
	t.Parallel()
	// New CRD field — validation must cover it too.
	err := validateClusterConfig(&ledgerv1alpha1.ClusterSpec{
		Bloom: &ledgerv1alpha1.BloomConfig{
			LedgerMetadata: &ledgerv1alpha1.BloomFilterConfig{FPRate: "2.0"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bloom.ledgerMetadata.fpRate")
}

func TestValidateClusterConfig_CoversPreparedQueries(t *testing.T) {
	t.Parallel()
	// New CRD field — validation must cover it too.
	err := validateClusterConfig(&ledgerv1alpha1.ClusterSpec{
		Bloom: &ledgerv1alpha1.BloomConfig{
			PreparedQueries: &ledgerv1alpha1.BloomFilterConfig{FPRate: "2.0"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bloom.preparedQueries.fpRate")
}
