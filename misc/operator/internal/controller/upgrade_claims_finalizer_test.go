package controller

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// The recorded claims are detached while the source StatefulSet is deleted
// (Deleting) and until the replacement adopts them (Adopting), so garbage
// collection does not apply the requested whenDeleted policy to them. The
// operator applies it to every recorded claim, whatever the workload's owner
// references say at that instant. An owner counts as the workload by UID: a
// live object that merely carries the StatefulSet's or a Pod's name is another
// owner.
func TestStoppedUpgradeClusterDeletionAppliesRequestedClaimRetention(t *testing.T) {
	t.Parallel()
	type testCase struct {
		phase       string
		whenDeleted string
		dataClaim   string
	}
	var tests []testCase
	for _, whenDeleted := range []string{"Delete", "Retain"} {
		for _, dataClaim := range []string{"replaced", "owned", "pod-owned", "stale-statefulset", "stale-pod", "foreign-owner", "foreign-group", "foreign-statefulset", "foreign-pod", "foreign-statefulset-pod"} {
			tests = append(tests, testCase{phase: "Deleting", whenDeleted: whenDeleted, dataClaim: dataClaim})
		}
	}
	for _, dataClaim := range []string{"replaced", "owned", "foreign-owner", "foreign-group"} {
		tests = append(tests, testCase{phase: "Adopting", whenDeleted: "Delete", dataClaim: dataClaim})
	}
	for _, tt := range tests {
		t.Run(tt.phase+"/"+tt.whenDeleted+"/"+tt.dataClaim, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := newMinimalCluster()
			cluster.UID = "cluster-uid"
			cluster.Finalizers = []string{operatorCredentialsFinalizer}
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
				DrainEpoch: "epoch", Phase: tt.phase,
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
			// The old Pod keeps the upgrade in Deleting; the replacement StatefulSet
			// has not adopted the claims in Adopting.
			oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: stsName + "-0", Namespace: cluster.Namespace, UID: "old-pod",
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: stsName, UID: "old-sts", Controller: new(true)}},
			}}
			workload, ownerUID := client.Object(oldPod), types.UID("old-sts")
			var sts *appsv1.StatefulSet
			if tt.phase == "Adopting" {
				sts = rcStatefulSet("ledger:"+cluster.Spec.Image.Tag, true)
				sts.Name, sts.Namespace, sts.UID = stsName, cluster.Namespace, "new-sts"
				sts.OwnerReferences = []metav1.OwnerReference{{APIVersion: "ledger.formance.com/v1alpha1", Kind: "Cluster", Name: cluster.Name, UID: cluster.UID, Controller: new(true)}}
				sts.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType, WhenScaled: appsv1.RetainPersistentVolumeClaimRetentionPolicyType}
				workload, ownerUID = sts, sts.UID
			}
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(
				cluster, workload,
				claim(walName, "wal-uid", "pv-wal"), volume("pv-wal", "pv-wal-uid", walName, "wal-uid"),
				claim(dataName, "data-uid", "pv-data"), volume("pv-data", "pv-data-uid", dataName, "data-uid"),
			).Build()
			r := &ClusterReconciler{Client: staleClaimOwnerReads{Client: c}, APIReader: c, Scheme: scheme, stopVersions: testStopVersions}
			key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}

			proceed, _, err := r.reconcileRequiredUpgradeWithExec(ctx, cluster, sts, nil)
			require.NoError(t, err)
			require.False(t, proceed)
			require.Equal(t, tt.phase, cluster.Status.Upgrade.Phase)

			require.NoError(t, c.Delete(ctx, cluster))
			wantDataUID := types.UID("data-uid")
			switch tt.dataClaim {
			case "replaced":
				// A claim recreated under a recorded name is not the upgrade's claim.
				require.NoError(t, c.Delete(ctx, claim(dataName, "data-uid", "pv-data")))
				require.NoError(t, c.Create(ctx, claim(dataName, "foreign-uid", "")))
				wantDataUID = "foreign-uid"
			default:
				// The workload's owner reference does not decide the claim's
				// fate: the StatefulSet controller may still remove it, or the
				// StatefulSet may already be gone. Any other owner does.
				owners := []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: stsName, UID: ownerUID}}
				switch tt.dataClaim {
				case "pod-owned":
					owners = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: oldPod.Name, UID: oldPod.UID}}
				case "stale-statefulset", "foreign-statefulset":
					// A live StatefulSet of that name that is not the Cluster's.
					// A reference to the earlier incarnation designates nothing.
					foreign := rcStatefulSet("other", true)
					foreign.Name, foreign.Namespace, foreign.UID = stsName, cluster.Namespace, "foreign-sts"
					require.NoError(t, c.Create(ctx, foreign))
					if tt.dataClaim == "foreign-statefulset" {
						owners[0].UID = foreign.UID
					}
				case "stale-pod", "foreign-pod", "foreign-statefulset-pod":
					// A live Pod of that name that no StatefulSet of the Cluster
					// controls. A reference to the earlier incarnation designates
					// nothing.
					foreign := &corev1.Pod{}
					require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(oldPod), foreign))
					foreign.OwnerReferences = nil
					if tt.dataClaim == "foreign-statefulset-pod" {
						foreign.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: stsName, UID: "foreign-sts", Controller: new(true)}}
					}
					require.NoError(t, c.Update(ctx, foreign))
					owners = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: oldPod.Name, UID: oldPod.UID}}
					if tt.dataClaim == "stale-pod" {
						owners[0].UID = "gone-pod"
					}
				case "foreign-owner":
					owners = append(owners, metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "hold", UID: "hold-uid"})
				case "foreign-group":
					owners[0].APIVersion = "example.com/v1"
				}
				owned := &corev1.PersistentVolumeClaim{}
				require.NoError(t, c.Get(ctx, types.NamespacedName{Name: dataName, Namespace: cluster.Namespace}, owned))
				owned.OwnerReferences = owners
				require.NoError(t, c.Update(ctx, owned))
			}
			_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			require.NoError(t, err)

			require.True(t, apierrors.IsNotFound(c.Get(ctx, key, &ledgerv1alpha1.Cluster{})), "the finalizer must be released")
			wal := &corev1.PersistentVolumeClaim{}
			err = c.Get(ctx, types.NamespacedName{Name: walName, Namespace: cluster.Namespace}, wal)
			if tt.whenDeleted == "Delete" {
				require.True(t, apierrors.IsNotFound(err), "detached claim must follow whenDeleted=Delete")
			} else {
				require.NoError(t, err)
				require.Equal(t, types.UID("wal-uid"), wal.UID)
			}
			data := &corev1.PersistentVolumeClaim{}
			err = c.Get(ctx, types.NamespacedName{Name: dataName, Namespace: cluster.Namespace}, data)
			workloadOwned := slices.Contains([]string{"owned", "pod-owned", "stale-statefulset", "stale-pod"}, tt.dataClaim)
			if tt.whenDeleted == "Delete" && workloadOwned {
				require.True(t, apierrors.IsNotFound(err), "a claim owned by the workload must follow whenDeleted=Delete")

				return
			}
			require.NoError(t, err, "a claim with another owner must not be deleted")
			require.Equal(t, wantDataUID, data.UID)
			switch {
			case workloadOwned || tt.dataClaim == "replaced":
				require.Empty(t, data.OwnerReferences, "a retained claim must not stay owned by the workload")
			case tt.dataClaim == "foreign-owner" && tt.whenDeleted == "Delete":
				require.Len(t, data.OwnerReferences, 2)
			case tt.dataClaim == "foreign-owner":
				require.Len(t, data.OwnerReferences, 1)
				require.Equal(t, "ConfigMap", data.OwnerReferences[0].Kind)
			default:
				require.Len(t, data.OwnerReferences, 1, "another owner's reference must be preserved")
			}
		})
	}
}

// staleClaimOwnerReads serves PVCs with the owner reference that a lagging
// cache still holds after the upgrade detached it.
type staleClaimOwnerReads struct{ client.Client }

func (s staleClaimOwnerReads) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := s.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok && len(pvc.OwnerReferences) == 0 {
		pvc.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "stale", UID: "old-sts"}}
	}

	return nil
}

// Adoption completes only while every recorded claim and volume keeps its
// identity: a lost claim would be recreated empty under its recorded name. A
// target edit is rejected rather than applied by ordinary reconciliation.
func TestStoppedUpgradeAdoptionRejectsLostClaims(t *testing.T) {
	t.Parallel()
	for _, whenDeleted := range []string{"Delete", "Retain"} {
		for _, withSTS := range []bool{true, false} {
			for _, loss := range []string{"intact", "missing", "replaced", "deleting", "rebound", "retargeted"} {
				name := whenDeleted + "/" + loss
				if !withSTS {
					name += "/no-statefulset"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					ctx := context.Background()
					cluster := newMinimalCluster()
					cluster.UID = "cluster-uid"
					cluster.Finalizers = []string{upgradeClaimsFinalizer}
					cluster.Spec.Image.Tag = "v3.0.0-rc.4"
					cluster.Spec.Replicas = new(int32(1))
					cluster.Spec.Persistence.RetentionPolicy = &ledgerv1alpha1.RetentionPolicySpec{WhenDeleted: whenDeleted}
					applyDefaults(cluster)
					stsName := resourceName(cluster.Name)
					walName, dataName := "wal-"+stsName+"-0", "data-"+stsName+"-0"
					cluster.Status.Upgrade = &ledgerv1alpha1.ClusterUpgradeStatus{
						Source: "v3.0.0-rc.3", Target: cluster.Spec.Image.Tag, StatefulSetUID: "old-sts",
						TargetSpecHash: computePodSpecHash(&cluster.Spec, false), ExpectedReplicas: 1,
						ClaimUIDs:  map[string]string{walName: "wal-uid", dataName: "data-uid"},
						VolumeUIDs: map[string]string{"pv-wal": "pv-wal-uid", "pv-data": "pv-data-uid"},
						DrainEpoch: "epoch", Phase: "Adopting",
					}
					sts := rcStatefulSet("ledger:"+cluster.Spec.Image.Tag, true)
					sts.Name, sts.Namespace, sts.UID = stsName, cluster.Namespace, "new-sts"
					sts.OwnerReferences = []metav1.OwnerReference{{APIVersion: "ledger.formance.com/v1alpha1", Kind: "Cluster", Name: cluster.Name, UID: cluster.UID, Controller: new(true)}}
					sts.Spec.PersistentVolumeClaimRetentionPolicy = buildRetentionPolicy(cluster)
					// The StatefulSet controller owns the claims only under Delete.
					var owners []metav1.OwnerReference
					if whenDeleted == "Delete" {
						owners = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: stsName, UID: sts.UID}}
					}
					claim := func(name, uid, pv string) *corev1.PersistentVolumeClaim {
						return &corev1.PersistentVolumeClaim{
							ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, UID: types.UID(uid), OwnerReferences: owners},
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
					data := claim(dataName, "data-uid", "pv-data")
					dataVolume := volume("pv-data", "pv-data-uid", dataName, "data-uid")
					wantErr := dataName
					switch loss {
					case "replaced":
						data.UID = "foreign-uid"
					case "deleting":
						data.Finalizers = []string{"kubernetes.io/pvc-protection"}
					case "rebound":
						dataVolume.Spec.ClaimRef.UID = "foreign-uid"
						wantErr = "pv-data"
					case "retargeted":
						cluster.Spec.Image.Tag = "v3.0.0-rc.5"
						wantErr = "restore spec.image.tag"
					}
					objects := []client.Object{cluster, sts, claim(walName, "wal-uid", "pv-wal"), volume("pv-wal", "pv-wal-uid", walName, "wal-uid"), dataVolume}
					if loss != "missing" {
						objects = append(objects, data)
					}
					scheme := runtime.NewScheme()
					require.NoError(t, clientgoscheme.AddToScheme(scheme))
					require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
					c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(objects...).Build()
					if loss == "deleting" {
						require.NoError(t, c.Delete(ctx, data))
					}
					r := &ClusterReconciler{Client: c, APIReader: c, Scheme: scheme, stopVersions: testStopVersions}
					current := sts
					if !withSTS {
						current = nil
					}

					proceed, _, err := r.reconcileRequiredUpgradeWithExec(ctx, cluster, current, nil)
					persisted := &ledgerv1alpha1.Cluster{}
					require.NoError(t, c.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, persisted))
					if loss == "intact" {
						require.NoError(t, err)
						require.True(t, proceed)
						if withSTS {
							require.Nil(t, persisted.Status.Upgrade)
							require.NotContains(t, persisted.Finalizers, upgradeClaimsFinalizer)
						}

						return
					}
					require.ErrorContains(t, err, wantErr)
					require.False(t, proceed)
					require.NotNil(t, persisted.Status.Upgrade)
					require.Equal(t, "Adopting", persisted.Status.Upgrade.Phase)
					require.Contains(t, persisted.Finalizers, upgradeClaimsFinalizer)
				})
			}
		}
	}
}

// Switching whenDeleted from Delete to Retain during Adopting leaves the
// replacement's owner references on the claims until the StatefulSet controller
// observes Retain. Garbage collection would delete owned claims with the
// Cluster, so adoption keeps the finalizer until the controller has observed
// Retain and owns no claim, and Cluster deletion removes those references
// itself once no StatefulSet can add them back.
func TestStoppedUpgradeRetainSwitchReleasesClaims(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"adoption", "cluster-deletion", "cluster-deletion-terminating-statefulset"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := newMinimalCluster()
			cluster.UID = "cluster-uid"
			cluster.Finalizers = []string{operatorCredentialsFinalizer, upgradeClaimsFinalizer}
			cluster.Spec.Image.Tag = "v3.0.0-rc.4"
			cluster.Spec.Replicas = new(int32(1))
			cluster.Spec.Persistence.RetentionPolicy = &ledgerv1alpha1.RetentionPolicySpec{WhenDeleted: "Delete"}
			applyDefaults(cluster)
			stsName := resourceName(cluster.Name)
			walName, dataName := "wal-"+stsName+"-0", "data-"+stsName+"-0"
			cluster.Status.Upgrade = &ledgerv1alpha1.ClusterUpgradeStatus{
				Source: "v3.0.0-rc.3", Target: cluster.Spec.Image.Tag, StatefulSetUID: "old-sts",
				TargetSpecHash: computePodSpecHash(&cluster.Spec, false), ExpectedReplicas: 1,
				ClaimUIDs:  map[string]string{walName: "wal-uid", dataName: "data-uid"},
				VolumeUIDs: map[string]string{"pv-wal": "pv-wal-uid", "pv-data": "pv-data-uid"},
				DrainEpoch: "epoch", Phase: "Adopting",
			}
			sts := rcStatefulSet("ledger:"+cluster.Spec.Image.Tag, true)
			sts.Name, sts.Namespace, sts.UID = stsName, cluster.Namespace, "new-sts"
			sts.Finalizers = []string{"foregroundDeletion"}
			sts.OwnerReferences = []metav1.OwnerReference{{APIVersion: "ledger.formance.com/v1alpha1", Kind: "Cluster", Name: cluster.Name, UID: cluster.UID, Controller: new(true)}}
			sts.Spec.PersistentVolumeClaimRetentionPolicy = buildRetentionPolicy(cluster)
			claim := func(name, uid, pv string) *corev1.PersistentVolumeClaim {
				return &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, UID: types.UID(uid), OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: stsName, UID: sts.UID}}},
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
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(
				cluster, sts,
				claim(walName, "wal-uid", "pv-wal"), volume("pv-wal", "pv-wal-uid", walName, "wal-uid"),
				claim(dataName, "data-uid", "pv-data"), volume("pv-data", "pv-data-uid", dataName, "data-uid"),
			).Build()
			r := &ClusterReconciler{Client: c, APIReader: c, Scheme: scheme, stopVersions: testStopVersions}
			key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
			stsKey := types.NamespacedName{Name: stsName, Namespace: cluster.Namespace}

			persisted := &ledgerv1alpha1.Cluster{}
			require.NoError(t, c.Get(ctx, key, persisted))
			persisted.Spec.Persistence.RetentionPolicy.WhenDeleted = "Retain"
			require.NoError(t, c.Update(ctx, persisted))

			reconcile := func() {
				t.Helper()
				if mode != "adoption" {
					_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
					require.NoError(t, err)

					return
				}
				current := &ledgerv1alpha1.Cluster{}
				require.NoError(t, c.Get(ctx, key, current))
				currentSTS := &appsv1.StatefulSet{}
				require.NoError(t, c.Get(ctx, stsKey, currentSTS))
				_, _, err := r.reconcileRequiredUpgradeWithExec(ctx, current, currentSTS, nil)
				require.NoError(t, err)
			}
			// The StatefulSet controller reports the generation it has acted on.
			setObserved := func(observed bool) {
				t.Helper()
				current := &appsv1.StatefulSet{}
				require.NoError(t, c.Get(ctx, stsKey, current))
				current.Status.ObservedGeneration = current.Generation
				if !observed {
					current.Status.ObservedGeneration--
				}
				require.NoError(t, c.Status().Update(ctx, current))
			}
			owners := func(name string) []metav1.OwnerReference {
				t.Helper()
				pvc := &corev1.PersistentVolumeClaim{}
				require.NoError(t, c.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, pvc))
				require.True(t, pvc.DeletionTimestamp.IsZero())

				return pvc.OwnerReferences
			}
			release := func(name string) {
				t.Helper()
				pvc := &corev1.PersistentVolumeClaim{}
				require.NoError(t, c.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, pvc))
				pvc.OwnerReferences = nil
				require.NoError(t, c.Update(ctx, pvc))
			}
			requireTracked := func(reason string) {
				t.Helper()
				current := &ledgerv1alpha1.Cluster{}
				require.NoError(t, c.Get(ctx, key, current))
				require.Contains(t, current.Finalizers, upgradeClaimsFinalizer, reason)
				require.NotNil(t, current.Status.Upgrade, reason)
				require.Equal(t, "Adopting", current.Status.Upgrade.Phase)
			}
			requirePolicy := func(want appsv1.PersistentVolumeClaimRetentionPolicyType) {
				t.Helper()
				current := &appsv1.StatefulSet{}
				require.NoError(t, c.Get(ctx, stsKey, current))
				require.Equal(t, want, current.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
			}

			switch mode {
			case "adoption":
				reconcile()
				requireTracked("Retain was only just applied to the replacement")
				requirePolicy(appsv1.RetainPersistentVolumeClaimRetentionPolicyType)
				setObserved(true)
				reconcile()
				requireTracked("both claims are still owned")
				release(walName)
				reconcile()
				requireTracked("the data claim is still owned")
				// A controller that has not observed Retain can still own the
				// claims again after they were read.
				release(dataName)
				setObserved(false)
				reconcile()
				requireTracked("the StatefulSet controller has not observed Retain")
				setObserved(true)
				reconcile()
				current := &ledgerv1alpha1.Cluster{}
				require.NoError(t, c.Get(ctx, key, current))
				require.Nil(t, current.Status.Upgrade)
				require.NotContains(t, current.Finalizers, upgradeClaimsFinalizer)
			case "cluster-deletion":
				require.NoError(t, c.Delete(ctx, persisted))
				reconcile()
				requireTracked("Retain was only just applied to the StatefulSet")
				requirePolicy(appsv1.RetainPersistentVolumeClaimRetentionPolicyType)
				setObserved(false)
				reconcile()
				requireTracked("the StatefulSet controller can still add its owner reference")
				require.Len(t, owners(walName), 1)
				require.Len(t, owners(dataName), 1)
				setObserved(true)
				reconcile()
				require.True(t, apierrors.IsNotFound(c.Get(ctx, key, &ledgerv1alpha1.Cluster{})), "the finalizer must be released")
			case "cluster-deletion-terminating-statefulset":
				// A deleting StatefulSet no longer reconciles its claims, so
				// nothing else removes the owner references before garbage
				// collection deletes the claims with it.
				require.NoError(t, c.Delete(ctx, sts))
				require.NoError(t, c.Delete(ctx, persisted))
				reconcile()
				require.True(t, apierrors.IsNotFound(c.Get(ctx, key, &ledgerv1alpha1.Cluster{})), "the finalizer must be released")
				requirePolicy(appsv1.DeletePersistentVolumeClaimRetentionPolicyType)
			}
			require.Empty(t, owners(walName))
			require.Empty(t, owners(dataName))
		})
	}
}
