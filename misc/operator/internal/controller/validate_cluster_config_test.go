package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestValidateClusterConfig_AcceptsNilAndEmpty(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateClusterConfig(&ledgerv1alpha1.LedgerServiceSpec{}))
	require.NoError(t, validateClusterConfig(&ledgerv1alpha1.LedgerServiceSpec{
		Cache: &ledgerv1alpha1.CacheConfig{},
		Bloom: &ledgerv1alpha1.BloomConfig{},
	}))
}

func TestValidateClusterConfig_RejectsNonPositiveRotation(t *testing.T) {
	t.Parallel()
	zero := int32(0)
	neg := int32(-5)
	err := validateClusterConfig(&ledgerv1alpha1.LedgerServiceSpec{
		Cache: &ledgerv1alpha1.CacheConfig{RotationThreshold: &zero},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rotationThreshold")
	err = validateClusterConfig(&ledgerv1alpha1.LedgerServiceSpec{
		Cache: &ledgerv1alpha1.CacheConfig{RotationThreshold: &neg},
	})
	require.Error(t, err)
}

func TestValidateClusterConfig_AcceptsPositiveRotation(t *testing.T) {
	t.Parallel()
	v := int32(1000)
	require.NoError(t, validateClusterConfig(&ledgerv1alpha1.LedgerServiceSpec{
		Cache: &ledgerv1alpha1.CacheConfig{RotationThreshold: &v},
	}))
}

func TestValidateClusterConfig_RejectsNegativeExpectedKeys(t *testing.T) {
	t.Parallel()
	bad := int64(-1)
	err := validateClusterConfig(&ledgerv1alpha1.LedgerServiceSpec{
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
	require.NoError(t, validateClusterConfig(&ledgerv1alpha1.LedgerServiceSpec{
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
			err := validateClusterConfig(&ledgerv1alpha1.LedgerServiceSpec{
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
		require.NoError(t, validateClusterConfig(&ledgerv1alpha1.LedgerServiceSpec{
			Bloom: &ledgerv1alpha1.BloomConfig{
				Volumes: &ledgerv1alpha1.BloomFilterConfig{FPRate: c},
			},
		}), "fpRate %s should be accepted", c)
	}
}

func TestValidateClusterConfig_CoversLedgerMetadata(t *testing.T) {
	t.Parallel()
	// New CRD field — validation must cover it too.
	err := validateClusterConfig(&ledgerv1alpha1.LedgerServiceSpec{
		Bloom: &ledgerv1alpha1.BloomConfig{
			LedgerMetadata: &ledgerv1alpha1.BloomFilterConfig{FPRate: "2.0"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bloom.ledgerMetadata.fpRate")
}
