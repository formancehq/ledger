package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
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

	// Changing DNSEndpoints should NOT change the hash.
	withDNS := base.DeepCopy()
	withDNS.DNSEndpoints = []ledgerv1alpha1.DNSEndpointSpec{{Name: "public", Enabled: true}}
	assert.Equal(t, baseHash, computeSpecHash(withDNS), "DNSEndpoints change should not affect hash")

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

	// Changing AutoExpansion should NOT change the hash: the policy is consumed
	// only by VolumeExpansionReconciler and does not affect the pod template.
	withAutoExpansion := base.DeepCopy()
	withAutoExpansion.Persistence.WAL.AutoExpansion = &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true}
	withAutoExpansion.Persistence.Data.AutoExpansion = &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true}
	assert.Equal(t, baseHash, computeSpecHash(withAutoExpansion), "AutoExpansion change should not affect hash")
	withStorage := base.DeepCopy()
	withStorage.Persistence.Data.Size = resource.MustParse("200Gi")
	withStorage.Persistence.WAL.VolumeAttributesClassName = "fast"
	assert.Equal(t, baseHash, computeSpecHash(withStorage), "PVC size/profile changes should not roll Pods")
	withStorageClass := base.DeepCopy()
	withStorageClass.Persistence.WAL.StorageClass = "wal-class"
	withStorageClass.Persistence.Data.StorageClass = "data-class"
	assert.Equal(t, baseHash, computeSpecHash(withStorageClass), "claim StorageClass declarations do not affect running Pods")
	withAccessMode := base.DeepCopy()
	withAccessMode.Persistence.WAL.AccessMode = string(corev1.ReadWriteOncePod)
	withAccessMode.Persistence.Data.AccessMode = string(corev1.ReadWriteMany)
	assert.Equal(t, baseHash, computeSpecHash(withAccessMode), "claim access-mode declarations do not affect running Pods")
}

func TestPreserveStorageSpecHash_LegacyMigration(t *testing.T) {
	t.Parallel()
	cluster := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "migration", Namespace: "default"}}
	applyDefaults(cluster)
	legacyHash := computePodSpecHash(&cluster.Spec, false)
	sts := &appsv1.StatefulSet{Spec: buildStatefulSetSpec(cluster, legacyHash, nil, tlsModeDisabled)}
	before := sts.Spec.Template.DeepCopy()
	cluster.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
	cluster.Spec.Persistence.WAL.VolumeAttributesClassName = "fast"
	hash := computeSpecHash(&cluster.Spec)
	desired := buildStatefulSetSpec(cluster, hash, nil, tlsModeDisabled)
	got := preserveStorageSpecHash(cluster, sts, desired.Template, hash)
	assert.Equal(t, *before, got, "first size/profile edit must preserve a hash produced by the released operator")

	// The source template floor/profile changes after recreation, so the
	// metadata hash must preserve this decision on every subsequent pass.
	sts.Annotations = map[string]string{annotationPodConfigHash: hash, annotationPodConfigSpecHash: hash}
	sts.Spec.VolumeClaimTemplates = desired.VolumeClaimTemplates
	got = preserveStorageSpecHash(cluster, sts, desired.Template, hash)
	assert.Equal(t, *before, got)
	assert.Equal(t, corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("200Gi")}, desired.VolumeClaimTemplates[1].Spec.Resources.Requests)

	cluster.Spec.Debug = !cluster.Spec.Debug
	newHash := computeSpecHash(&cluster.Spec)
	newDesired := buildStatefulSetSpec(cluster, newHash, nil, tlsModeDisabled)
	got = preserveStorageSpecHash(cluster, sts, newDesired.Template, newHash)
	assert.Equal(t, newHash, got.Annotations[annotationSpecHash], "a real workload change must still roll out")
}

func TestComputeSpecHash_IncludesPodFields(t *testing.T) {
	t.Parallel()

	base := &ledgerv1alpha1.ClusterSpec{
		DataDir: "/data",
	}
	baseHash := computeSpecHash(base)
	withServiceAccount := base.DeepCopy()
	withServiceAccount.ServiceAccount.Annotations = map[string]string{"eks.amazonaws.com/role-arn": "arn:aws:iam::111111111111:role/backup"}
	assert.NotEqual(t, baseHash, computeSpecHash(withServiceAccount), "ServiceAccount annotations require fresh Pod admission")

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
