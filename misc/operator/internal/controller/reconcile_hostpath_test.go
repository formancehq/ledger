//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestReconcile_HostPathDataVolume(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("hp-data", ns)
	ls.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{
		Path: "/mnt/nvme0/data",
	}
	// Add nodeSelector to avoid scheduling warning
	ls.Spec.NodeSelector = map[string]string{"node-type": "nvme"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-hp-data", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	// Only wal and cold-cache should be VolumeClaimTemplates (data is hostPath)
	require.Len(t, sts.Spec.VolumeClaimTemplates, 2)
	assert.Equal(t, "wal", sts.Spec.VolumeClaimTemplates[0].Name)
	assert.Equal(t, "cold-cache", sts.Spec.VolumeClaimTemplates[1].Name)

	// Data should be an inline hostPath volume in pod template
	podVolumes := sts.Spec.Template.Spec.Volumes
	var dataVol *corev1.Volume
	for i := range podVolumes {
		if podVolumes[i].Name == "data" {
			dataVol = &podVolumes[i]
			break
		}
	}
	require.NotNil(t, dataVol, "data volume should exist as inline pod volume")
	require.NotNil(t, dataVol.HostPath, "data volume should use hostPath")
	assert.Equal(t, "/mnt/nvme0/data", dataVol.HostPath.Path)
	assert.Equal(t, corev1.HostPathDirectoryOrCreate, *dataVol.HostPath.Type)

	// Volume mount should use SubPathExpr for per-pod isolation
	container := sts.Spec.Template.Spec.Containers[0]
	var dataMount *corev1.VolumeMount
	for i := range container.VolumeMounts {
		if container.VolumeMounts[i].Name == "data" {
			dataMount = &container.VolumeMounts[i]
			break
		}
	}
	require.NotNil(t, dataMount, "data volume mount should exist")
	assert.Equal(t, "/data/app", dataMount.MountPath)
	assert.Equal(t, "$(POD_INDEX)", dataMount.SubPathExpr)

	// WAL mount should NOT have SubPathExpr (it's PVC-backed)
	var walMount *corev1.VolumeMount
	for i := range container.VolumeMounts {
		if container.VolumeMounts[i].Name == "wal" {
			walMount = &container.VolumeMounts[i]
			break
		}
	}
	require.NotNil(t, walMount)
	assert.Empty(t, walMount.SubPathExpr)
}

func TestReconcile_HostPathAllVolumes(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("hp-all", ns)
	ls.Spec.Persistence.WAL.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{
		Path: "/mnt/nvme0/wal",
	}
	ls.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{
		Path: "/mnt/nvme0/data",
		Type: "Directory",
	}
	ls.Spec.Persistence.ColdCache.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{
		Path: "/mnt/nvme0/cold-cache",
	}
	ls.Spec.NodeSelector = map[string]string{"node-type": "nvme"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-hp-all", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	// No VolumeClaimTemplates when all volumes are hostPath
	assert.Empty(t, sts.Spec.VolumeClaimTemplates)

	// All three should be inline hostPath volumes
	podVolumes := sts.Spec.Template.Spec.Volumes
	hostPathVols := make(map[string]*corev1.HostPathVolumeSource)
	for i := range podVolumes {
		if podVolumes[i].HostPath != nil {
			hostPathVols[podVolumes[i].Name] = podVolumes[i].HostPath
		}
	}
	require.Contains(t, hostPathVols, "wal")
	require.Contains(t, hostPathVols, "data")
	require.Contains(t, hostPathVols, "cold-cache")

	assert.Equal(t, "/mnt/nvme0/wal", hostPathVols["wal"].Path)
	assert.Equal(t, "/mnt/nvme0/data", hostPathVols["data"].Path)
	assert.Equal(t, "/mnt/nvme0/cold-cache", hostPathVols["cold-cache"].Path)

	// Data should use Directory type (not the default DirectoryOrCreate)
	assert.Equal(t, corev1.HostPathDirectory, *hostPathVols["data"].Type)
	// WAL should use default DirectoryOrCreate
	assert.Equal(t, corev1.HostPathDirectoryOrCreate, *hostPathVols["wal"].Type)

	// All mounts should have SubPathExpr
	container := sts.Spec.Template.Spec.Containers[0]
	for _, name := range []string{"wal", "data", "cold-cache"} {
		for _, m := range container.VolumeMounts {
			if m.Name == name {
				assert.Equal(t, "$(POD_INDEX)", m.SubPathExpr, "volume %s should have SubPathExpr", name)
			}
		}
	}
}

func TestReconcile_HostPathValidation_MutualExclusion(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("hp-invalid", ns)
	ls.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{
		Path: "/mnt/nvme0/data",
	}
	ls.Spec.Persistence.Data.StorageClass = "gp3" // conflict!
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Should fail validation
	updated := &ledgerv1alpha1.Cluster{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "hp-invalid", Namespace: ns}, updated); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(updated.Status.Conditions, "ConfigValid")
		return cond != nil && cond.Status == metav1.ConditionFalse
	}, "ConfigValid should be False for hostPath+storageClass")

	cond := meta.FindStatusCondition(updated.Status.Conditions, "ConfigValid")
	assert.Contains(t, cond.Message, "mutually exclusive")

	// StatefulSet should NOT be created
	sts := &appsv1.StatefulSet{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-hp-invalid", Namespace: ns}, sts)
	assert.Error(t, err, "StatefulSet should not be created with invalid config")
}

func TestReconcile_HostPathValidation_EmptyPath(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("hp-nopath", ns)
	ls.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{
		Path: "", // empty path
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	updated := &ledgerv1alpha1.Cluster{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "hp-nopath", Namespace: ns}, updated); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(updated.Status.Conditions, "ConfigValid")
		return cond != nil && cond.Status == metav1.ConditionFalse
	}, "ConfigValid should be False for empty hostPath path")

	cond := meta.FindStatusCondition(updated.Status.Conditions, "ConfigValid")
	assert.Contains(t, cond.Message, "must not be empty")
}

func TestReconcile_HostPathSchedulingWarning(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("hp-warn", ns)
	ls.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{
		Path: "/mnt/nvme0/data",
	}
	// No nodeSelector or affinity → should get a warning
	require.NoError(t, k8sClient.Create(ctx, ls))

	updated := &ledgerv1alpha1.Cluster{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "hp-warn", Namespace: ns}, updated); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(updated.Status.Conditions, "HostPathSchedulingWarning")
		return cond != nil && cond.Status == metav1.ConditionTrue
	}, "HostPathSchedulingWarning should be set")

	cond := meta.FindStatusCondition(updated.Status.Conditions, "HostPathSchedulingWarning")
	assert.Contains(t, cond.Message, "nodeSelector")

	// ConfigValid should still be True (warning, not error)
	validCond := meta.FindStatusCondition(updated.Status.Conditions, "ConfigValid")
	require.NotNil(t, validCond)
	assert.Equal(t, metav1.ConditionTrue, validCond.Status)
}

func TestReconcile_HostPathNoWarningWithNodeSelector(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("hp-nowarn", ns)
	ls.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{
		Path: "/mnt/nvme0/data",
	}
	ls.Spec.NodeSelector = map[string]string{"node-type": "nvme"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for StatefulSet
	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-hp-nowarn", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	updated := &ledgerv1alpha1.Cluster{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "hp-nowarn", Namespace: ns}, updated))

	// HostPathSchedulingWarning should NOT be set
	cond := meta.FindStatusCondition(updated.Status.Conditions, "HostPathSchedulingWarning")
	assert.Nil(t, cond, "HostPathSchedulingWarning should not be set when nodeSelector is present")
}
