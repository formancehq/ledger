package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func TestComputeSpecHash_ExcludesNonPodFields(t *testing.T) {
	t.Parallel()

	base := &ledgerv1alpha1.LedgerServiceSpec{
		Config: ledgerv1alpha1.LedgerServiceConfig{
			DataDir: "/data",
		},
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

	// Changing ServiceMonitor should NOT change the hash.
	withSM := base.DeepCopy()
	withSM.ServiceMonitor = &ledgerv1alpha1.ServiceMonitorSpec{Enabled: true}
	assert.Equal(t, baseHash, computeSpecHash(withSM), "ServiceMonitor change should not affect hash")

	// Changing PDB should NOT change the hash.
	withPDB := base.DeepCopy()
	withPDB.PodDisruptionBudget = &ledgerv1alpha1.PodDisruptionBudgetSpec{Enabled: true}
	assert.Equal(t, baseHash, computeSpecHash(withPDB), "PDB change should not affect hash")

	// Changing Replicas should NOT change the hash.
	withReplicas := base.DeepCopy()
	replicas := int32(5)
	withReplicas.Replicas = &replicas
	assert.Equal(t, baseHash, computeSpecHash(withReplicas), "Replicas change should not affect hash")

	// Changing AutoNetworking should NOT change the hash.
	withAN := base.DeepCopy()
	withAN.AutoNetworking = &ledgerv1alpha1.AutoNetworkingSpec{TLD: "example.com"}
	assert.Equal(t, baseHash, computeSpecHash(withAN), "AutoNetworking change should not affect hash")
}

func TestComputeSpecHash_IncludesPodFields(t *testing.T) {
	t.Parallel()

	base := &ledgerv1alpha1.LedgerServiceSpec{
		Config: ledgerv1alpha1.LedgerServiceConfig{
			DataDir: "/data",
		},
	}
	baseHash := computeSpecHash(base)

	// Changing Config should change the hash (affects env vars).
	withConfig := base.DeepCopy()
	withConfig.Config.Debug = true
	assert.NotEqual(t, baseHash, computeSpecHash(withConfig), "Config change should affect hash")

	// Changing Monitoring should change the hash (affects env vars).
	withMon := base.DeepCopy()
	withMon.Config.Monitoring = &ledgerv1alpha1.MonitoringConfig{
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
}
