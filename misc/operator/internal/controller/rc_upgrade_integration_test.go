//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestRCUpgradeWithStorageTransitionUsesExistingWiring(t *testing.T) {
	for _, direction := range []string{"hostPath-to-PVC", "PVC-to-hostPath"} {
		t.Run(direction, func(t *testing.T) {
			namespace := createTestNamespace(t)
			ledger := newCluster("rc-storage", namespace)
			// Do not create the CR: the background manager must not race this
			// direct API-server-backed reconciliation.
			ledger.UID = types.UID(namespace + "-cluster")
			ledger.Spec.Image.Tag = "v3.0.0-rc.1"
			if direction == "hostPath-to-PVC" {
				ledger.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/data"}
			}
			applyDefaults(ledger)
			reconciler := &ClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := reconciler.reconcileStatefulSetWithExec(ctx, ledger, "initial", nil, nil)
			require.NoError(t, err)

			key := types.NamespacedName{Name: resourceName(ledger.Name), Namespace: namespace}
			sts := &appsv1.StatefulSet{}
			require.NoError(t, k8sClient.Get(ctx, key, sts))
			originalClaims := len(sts.Spec.VolumeClaimTemplates)
			originalVolume := storageVolume(sts.Spec.Template.Spec.Volumes, "data")
			originalMount := storageMount(sts.Spec.Template.Spec.Containers[0].VolumeMounts, "data")
			sts.Status.ObservedGeneration = sts.Generation
			sts.Status.Replicas = *sts.Spec.Replicas
			sts.Status.UpdatedReplicas = *sts.Spec.Replicas
			sts.Status.ReadyReplicas = *sts.Spec.Replicas
			require.NoError(t, k8sClient.Status().Update(ctx, sts))

			ledger.Spec.Image.Tag = "v3.0.0-rc.2"
			if direction == "hostPath-to-PVC" {
				ledger.Spec.Persistence.Data.HostPath = nil
			} else {
				ledger.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/data"}
			}
			// This update reaches real StatefulSet admission. A template with
			// the new storage wiring and old immutable claims is rejected.
			_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "upgraded", nil, nil)
			require.NoError(t, err)
			require.NoError(t, k8sClient.Get(ctx, key, sts))
			require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.2", sts.Spec.Template.Spec.Containers[0].Image)
			require.Len(t, sts.Spec.VolumeClaimTemplates, originalClaims)
			require.Equal(t, originalVolume, storageVolume(sts.Spec.Template.Spec.Volumes, "data"))
			require.Equal(t, originalMount, storageMount(sts.Spec.Template.Spec.Containers[0].VolumeMounts, "data"))

			// A fresh reconciler must still defer recreation during a mixed
			// rollout, then take the existing orphan/recreate path on completion.
			sts.Status.ObservedGeneration = sts.Generation
			sts.Status.UpdatedReplicas = *sts.Spec.Replicas - 1
			sts.Status.ReadyReplicas = *sts.Spec.Replicas - 1
			require.NoError(t, k8sClient.Status().Update(ctx, sts))
			reconciler = &ClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "upgraded", nil, nil)
			require.NoError(t, err)
			require.NoError(t, k8sClient.Get(ctx, key, sts))
			sts.Status.ObservedGeneration = sts.Generation
			sts.Status.UpdatedReplicas = *sts.Spec.Replicas
			sts.Status.ReadyReplicas = *sts.Spec.Replicas
			require.NoError(t, k8sClient.Status().Update(ctx, sts))
			_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "upgraded", nil, nil)
			require.NoError(t, err)
			require.NoError(t, k8sClient.Get(ctx, key, sts))
			// envtest has no garbage collector, so the orphan deletion
			// remains pending instead of completing the replacement.
			require.False(t, sts.DeletionTimestamp.IsZero())
		})
	}
}
