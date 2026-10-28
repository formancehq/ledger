//go:build integration

package controller

import (
	"testing"
	"time"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

// envtest has no StatefulSet/GC controllers or kubelet. This test establishes
// CRD admission, real resourceVersion conflicts and the retention handshake;
// the Chainsaw scenario establishes actual volume/data and Raft recovery.
func TestClusterSuspensionAPI(t *testing.T) {
	t.Parallel()
	ns := createTestNamespace(t)
	cluster := newCluster("suspension", ns)
	cluster.Spec.Disabled = true
	require.NoError(t, k8sClient.Create(ctx, cluster))
	clusterKey := types.NamespacedName{Name: cluster.Name, Namespace: ns}
	stsKey := types.NamespacedName{Name: resourceName(cluster.Name), Namespace: ns}
	require.Eventually(t, func() bool {
		return k8sClient.Get(ctx, clusterKey, cluster) == nil && cluster.Status.Phase == "Suspended" && cluster.Spec.ClusterID != ""
	}, 20*time.Second, 100*time.Millisecond)
	identity := cluster.Spec.ClusterID
	uid := cluster.UID
	require.True(t, apierrors.IsNotFound(k8sClient.Get(ctx, stsKey, &appsv1.StatefulSet{})))
	invalid := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "zero-replicas", Namespace: ns}, Spec: ledgerv1alpha1.ClusterSpec{Disabled: true, Replicas: ptr.To(int32(0))}}
	require.True(t, apierrors.IsInvalid(k8sClient.Create(ctx, invalid)), "disabled does not relax configured topology validation")
	cluster.Spec.Disabled = false
	require.NoError(t, k8sClient.Update(ctx, cluster))
	sts := &appsv1.StatefulSet{}
	require.Eventually(t, func() bool { return k8sClient.Get(ctx, stsKey, sts) == nil }, 20*time.Second, 100*time.Millisecond)
	stsUID := sts.UID
	require.EqualValues(t, 3, *sts.Spec.Replicas)
	for cycle := range 2 {
		require.Eventually(t, func() bool {
			if k8sClient.Get(ctx, clusterKey, cluster) != nil {
				return false
			}
			cluster.Spec.Disabled = true
			return k8sClient.Update(ctx, cluster) == nil
		}, 10*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool {
			if k8sClient.Get(ctx, stsKey, sts) != nil {
				return false
			}
			return sts.Annotations[annotationSuspendedReplicas] == "3"
		}, 20*time.Second, 100*time.Millisecond)
		if cycle == 0 {
			require.EqualValues(t, 3, *sts.Spec.Replicas, "wait for initial StatefulSet observation")
		}
		require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
		require.Eventually(t, func() bool {
			if k8sClient.Get(ctx, stsKey, sts) != nil {
				return false
			}
			sts.Status.ObservedGeneration = sts.Generation
			return k8sClient.Status().Update(ctx, sts) == nil
		}, 10*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool { return k8sClient.Get(ctx, stsKey, sts) == nil && *sts.Spec.Replicas == 0 }, 20*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool {
			// Simulate the absent StatefulSet controller observing the scale.
			if k8sClient.Get(ctx, stsKey, sts) != nil {
				return false
			}
			sts.Status.ObservedGeneration = sts.Generation
			if k8sClient.Status().Update(ctx, sts) != nil {
				return false
			}
			return k8sClient.Get(ctx, clusterKey, cluster) == nil && cluster.Status.Phase == "Suspended"
		}, 20*time.Second, 100*time.Millisecond)
		cluster.Spec.Disabled = false
		require.NoError(t, k8sClient.Update(ctx, cluster))
		require.Eventually(t, func() bool { return k8sClient.Get(ctx, stsKey, sts) == nil && *sts.Spec.Replicas == 3 }, 20*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool {
			if k8sClient.Get(ctx, stsKey, sts) != nil {
				return false
			}
			sts.Status.ObservedGeneration = sts.Generation
			sts.Status.Replicas = 3
			sts.Status.ReadyReplicas = 3
			return k8sClient.Status().Update(ctx, sts) == nil
		}, 10*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool {
			return k8sClient.Get(ctx, stsKey, sts) == nil && sts.Annotations[annotationSuspendedReplicas] == ""
		}, 20*time.Second, 100*time.Millisecond)
		require.NoError(t, k8sClient.Get(ctx, clusterKey, cluster))
		require.Equal(t, identity, cluster.Spec.ClusterID)
		require.Equal(t, uid, cluster.UID)
		require.Equal(t, stsUID, sts.UID)
	}
}
