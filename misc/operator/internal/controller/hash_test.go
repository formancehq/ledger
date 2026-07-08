package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestComputeSpecHash_ExcludesNonPodFields(t *testing.T) {
	t.Parallel()

	base := &ledgerv1alpha1.ClusterSpec{
		DataDir: "/data",
	}
	baseHash := computeSpecHash(base)

	// Changing NetworkPolicy should NOT change the hash.
	withNP := base.DeepCopy()
	withNP.NetworkPolicy = &ledgerv1alpha1.NetworkPolicySpec{Enabled: true}
	assert.Equal(t, baseHash, computeSpecHash(withNP), "NetworkPolicy change should not affect hash")

	// Changing Ingress should NOT change the hash.
	withIngress := base.DeepCopy()
	withIngress.Ingress = &ledgerv1alpha1.IngressSpec{Enabled: true}
	assert.Equal(t, baseHash, computeSpecHash(withIngress), "Ingress change should not affect hash")

	// Changing IngressGrpc should NOT change the hash.
	withGrpcIngress := base.DeepCopy()
	withGrpcIngress.IngressGrpc = &ledgerv1alpha1.IngressGrpcSpec{Enabled: true}
	assert.Equal(t, baseHash, computeSpecHash(withGrpcIngress), "IngressGrpc change should not affect hash")

	// Changing DNSEndpoint should NOT change the hash.
	withDNS := base.DeepCopy()
	withDNS.DNSEndpoint = &ledgerv1alpha1.DNSEndpointSpec{Enabled: true}
	assert.Equal(t, baseHash, computeSpecHash(withDNS), "DNSEndpoint change should not affect hash")

	// Changing Replicas should NOT change the hash.
	withReplicas := base.DeepCopy()
	replicas := int32(5)
	withReplicas.Replicas = &replicas
	assert.Equal(t, baseHash, computeSpecHash(withReplicas), "Replicas change should not affect hash")

	// Toggling Persistence.DeletionProtection should NOT change the hash: it only
	// drives PVC/PV label patches, not the pod template, so it must not roll pods.
	withDeletionProtection := base.DeepCopy()
	optOut := false
	withDeletionProtection.Persistence.DeletionProtection = &optOut
	assert.Equal(t, baseHash, computeSpecHash(withDeletionProtection), "DeletionProtection change should not affect hash")
}

func TestComputeSpecHash_IncludesPodFields(t *testing.T) {
	t.Parallel()

	base := &ledgerv1alpha1.ClusterSpec{
		DataDir: "/data",
	}
	baseHash := computeSpecHash(base)

	// Changing Config should change the hash (affects env vars).
	withConfig := base.DeepCopy()
	withConfig.Debug = true
	assert.NotEqual(t, baseHash, computeSpecHash(withConfig), "Config change should affect hash")

	// Changing Monitoring should change the hash (affects env vars).
	withMon := base.DeepCopy()
	withMon.Monitoring = &ledgerv1alpha1.MonitoringConfig{
		Pyroscope: &ledgerv1alpha1.PyroscopeConfig{
			Enabled:       true,
			ServerAddress: "http://pyroscope:4040",
		},
	}
	assert.NotEqual(t, baseHash, computeSpecHash(withMon), "Monitoring change should affect hash")

	// Changing Image should change the hash.
	withImage := base.DeepCopy()
	withImage.Image.Tag = "v2"
	assert.NotEqual(t, baseHash, computeSpecHash(withImage), "Image change should affect hash")

	// Changing NodeSelector should change the hash.
	withNS := base.DeepCopy()
	withNS.NodeSelector = map[string]string{"zone": "us-east-1a"}
	assert.NotEqual(t, baseHash, computeSpecHash(withNS), "NodeSelector change should affect hash")

	// Changing Cache.RotationThreshold should change the hash (drives a rolling restart
	// + cluster-config reconciliation in proposeClusterConfigIfNeeded).
	withRotation := base.DeepCopy()
	threshold := int32(2000)
	withRotation.Cache = &ledgerv1alpha1.CacheConfig{RotationThreshold: &threshold}
	assert.NotEqual(t, baseHash, computeSpecHash(withRotation), "Cache.RotationThreshold change should affect hash")

	// Changing any Bloom field should change the hash.
	withBloomVolumes := base.DeepCopy()
	keys := int64(50000)
	withBloomVolumes.Bloom = &ledgerv1alpha1.BloomConfig{
		Volumes: &ledgerv1alpha1.BloomFilterConfig{ExpectedKeys: &keys},
	}
	assert.NotEqual(t, baseHash, computeSpecHash(withBloomVolumes), "Bloom.Volumes change should affect hash")

	withBloomLedgerMetadata := base.DeepCopy()
	withBloomLedgerMetadata.Bloom = &ledgerv1alpha1.BloomConfig{
		LedgerMetadata: &ledgerv1alpha1.BloomFilterConfig{FPRate: "0.001"},
	}
	assert.NotEqual(t, baseHash, computeSpecHash(withBloomLedgerMetadata), "Bloom.LedgerMetadata change should affect hash")
}
