package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// The source StatefulSet is gone and its claims are detached, so garbage
// collection no longer applies the requested whenDeleted policy to them.
func TestStoppedUpgradeClusterDeletionAppliesRequestedClaimRetention(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		whenDeleted string
		deleted     bool
	}{
		{whenDeleted: "Delete", deleted: true},
		{whenDeleted: "Retain", deleted: false},
	} {
		t.Run(tt.whenDeleted, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := newMinimalCluster()
			cluster.UID = "cluster-uid"
			cluster.Spec.Image.Tag = "v3.0.0-rc.4"
			cluster.Spec.Replicas = new(int32(1))
			cluster.Spec.Persistence.RetentionPolicy = &ledgerv1alpha1.RetentionPolicySpec{WhenDeleted: tt.whenDeleted}
			applyDefaults(cluster)
			stsName := resourceName(cluster.Name)
			walName, dataName := "wal-"+stsName+"-0", "data-"+stsName+"-0"
			cluster.Status.Upgrade = &ledgerv1alpha1.ClusterUpgradeStatus{
				Source: "v3.0.0-rc.3", Target: cluster.Spec.Image.Tag, StatefulSetUID: "old-sts",
				TargetSpecHash: computePodSpecHash(&cluster.Spec, false), ExpectedReplicas: 1,
				ClaimUIDs:  map[string]string{walName: "wal-uid", dataName: "data-uid"},
				VolumeUIDs: map[string]string{"pv-wal": "pv-wal-uid", "pv-data": "pv-data-uid"},
				DrainEpoch: "epoch", Phase: "Deleting",
			}
			claim := func(name, uid, pv string) *corev1.PersistentVolumeClaim {
				return &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, UID: types.UID(uid)},
					Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: pv},
					Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
				}
			}
			volume := func(name, uid, claimName, claimUID string) *corev1.PersistentVolume {
				return &corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid)},
					Spec:       corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: claimName, Namespace: cluster.Namespace, UID: types.UID(claimUID)}},
					Status:     corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
				}
			}
			oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: stsName + "-0", Namespace: cluster.Namespace}}
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(
				cluster, oldPod,
				claim(walName, "wal-uid", "pv-wal"), volume("pv-wal", "pv-wal-uid", walName, "wal-uid"),
				claim(dataName, "data-uid", "pv-data"), volume("pv-data", "pv-data-uid", dataName, "data-uid"),
			).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme, stopVersions: testStopVersions}
			key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}

			// The old Pod keeps the upgrade in Deleting.
			proceed, _, err := r.reconcileRequiredUpgradeWithExec(ctx, cluster, nil, nil)
			require.NoError(t, err)
			require.False(t, proceed)
			require.Equal(t, "Deleting", cluster.Status.Upgrade.Phase)

			require.NoError(t, c.Delete(ctx, cluster))
			// A claim recreated under a recorded name is not the upgrade's claim.
			require.NoError(t, c.Delete(ctx, claim(dataName, "data-uid", "pv-data")))
			require.NoError(t, c.Create(ctx, claim(dataName, "foreign-uid", "")))
			_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			require.NoError(t, err)

			require.True(t, apierrors.IsNotFound(c.Get(ctx, key, &ledgerv1alpha1.Cluster{})), "the finalizer must be released")
			wal := &corev1.PersistentVolumeClaim{}
			err = c.Get(ctx, types.NamespacedName{Name: walName, Namespace: cluster.Namespace}, wal)
			if tt.deleted {
				require.True(t, apierrors.IsNotFound(err), "detached claim must follow whenDeleted=Delete")
			} else {
				require.NoError(t, err)
				require.Equal(t, types.UID("wal-uid"), wal.UID)
			}
			data := &corev1.PersistentVolumeClaim{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Name: dataName, Namespace: cluster.Namespace}, data))
			require.Equal(t, types.UID("foreign-uid"), data.UID)
		})
	}
}
