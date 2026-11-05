package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// Existing automatic-expansion tests keep their production triggers, and now
// provide the workload identity evidence required before any PVC mutation.
func volumeExpansionTestStatefulSet(ledger *ledgerv1alpha1.Cluster, replicas int32) *appsv1.StatefulSet {
	if ledger.UID == "" {
		ledger.UID = types.UID(ledger.Name + "-uid")
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName(ledger.Name), Namespace: ledger.Namespace, UID: types.UID(resourceName(ledger.Name) + "-uid"), OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ledger, ledgerv1alpha1.GroupVersion.WithKind("Cluster"))}},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas, VolumeClaimTemplates: buildVolumeClaimTemplates(ledger)},
	}
}

func volumeExpansionTestObjects(ledger *ledgerv1alpha1.Cluster, objects ...runtime.Object) []runtime.Object {
	replicas := int32(1)
	var sts *appsv1.StatefulSet
	claims := map[string]bool{}
	for _, object := range objects {
		switch object := object.(type) {
		case *appsv1.StatefulSet:
			sts = object
			if object.Spec.Replicas != nil {
				replicas = *object.Spec.Replicas
			}
		case *corev1.PersistentVolumeClaim:
			claims[object.Name] = true
		}
	}
	owned := volumeExpansionTestStatefulSet(ledger, replicas)
	if sts == nil {
		objects = append(objects, owned)
	} else {
		sts.UID = owned.UID
		sts.OwnerReferences = owned.OwnerReferences
		sts.Spec.VolumeClaimTemplates = owned.Spec.VolumeClaimTemplates
	}
	// Tests focused on data expansion still need a ready WAL group now that
	// explicit storage reconciliation operates independently of opt-in policy.
	if ledger.Spec.Persistence.WAL.HostPath == nil {
		for ordinal := range replicas {
			name := fmt.Sprintf("wal-%s-%d", owned.Name, ordinal)
			if !claims[name] {
				objects = append(objects, boundTestPVC(name, "expandable", "5Gi"))
			}
		}
	}

	return objects
}

func persistenceTestFixture(t *testing.T, replicas int32) (*ledgerv1alpha1.Cluster, *appsv1.StatefulSet, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, storagev1.AddToScheme(scheme))
	require.NoError(t, storagev1beta1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(storagev1.SchemeGroupVersion.WithKind("VolumeAttributesClass"), &unstructured.Unstructured{})
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "storage", Namespace: "default", Generation: 7},
		Spec: ledgerv1alpha1.ClusterSpec{Replicas: &replicas, Persistence: ledgerv1alpha1.PersistenceSpec{
			WAL:  ledgerv1alpha1.VolumeSpec{Size: resource.MustParse("5Gi"), StorageClass: "expandable"},
			Data: ledgerv1alpha1.VolumeSpec{Size: resource.MustParse("100Gi"), StorageClass: "expandable"},
		}},
	}
	sts := volumeExpansionTestStatefulSet(ledger, replicas)
	objects := []client.Object{ledger, sts, &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "expandable"}, AllowVolumeExpansion: new(true)}}
	for ordinal := range replicas {
		for kind, size := range map[string]string{"wal": "5Gi", "data": "100Gi"} {
			pvc := boundTestPVC(kind+"-"+sts.Name+"-"+strconv.Itoa(int(ordinal)), "expandable", size)
			objects = append(objects, pvc, &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: pvc.Spec.VolumeName},
				Spec: corev1.PersistentVolumeSpec{
					ClaimRef:               &corev1.ObjectReference{Name: pvc.Name, Namespace: pvc.Namespace, UID: pvc.UID},
					PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: "example.csi", VolumeHandle: pvc.Name}},
				},
			})
		}
	}
	objects = append(objects, &storagev1beta1.VolumeAttributesClass{ObjectMeta: metav1.ObjectMeta{Name: "fast"}, DriverName: "example.csi", Parameters: map[string]string{"iops": "5000"}})
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(ledger, &corev1.PersistentVolumeClaim{}).WithObjects(objects...).Build()

	return ledger, sts, c
}

func readPersistencePVC(t *testing.T, c client.Client, kind string, ordinal int) *corev1.PersistentVolumeClaim {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: fmt.Sprintf("%s-ledger-storage-%d", kind, ordinal)}, pvc))

	return pvc
}

func reconcilePersistence(t *testing.T, c client.Client, ledger *ledgerv1alpha1.Cluster) (ctrl.Result, error) {
	t.Helper()
	r := &VolumeExpansionReconciler{Client: c, APIReader: c, ReadDiskUsage: func(context.Context, *ledgerv1alpha1.Cluster, string, string) (podDiskUsage, error) {
		t.Error("explicit storage reconciliation must not measure disk usage")

		return podDiskUsage{}, errors.New("unexpected measurement")
	}}

	return r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ledger)})
}

func TestPersistenceReconcilesMinimumWithoutAutoExpansion(t *testing.T) {
	t.Parallel()
	ledger, _, c := persistenceTestFixture(t, 2)
	ledger.Spec.Persistence.WAL.Size = resource.MustParse("8Gi")
	ledger.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
	require.NoError(t, c.Update(t.Context(), ledger))
	result, err := reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	require.Equal(t, volumeExpansionRetryInterval, result.RequeueAfter)
	for ordinal := range 2 {
		for kind, size := range map[string]string{"wal": "8Gi", "data": "150Gi"} {
			pvc := readPersistencePVC(t, c, kind, ordinal)
			request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
			require.Equal(t, size, request.String())
			require.Equal(t, types.UID(pvc.Name+"-uid"), pvc.UID)
			require.Equal(t, "pv-"+pvc.Name, pvc.Spec.VolumeName)
			require.Empty(t, pvc.Annotations[annotationLastExpansionAt], "manual floors do not change automatic cooldown")
		}
	}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
	for _, conditionType := range []string{walStorageCondition, dataStorageCondition} {
		condition := meta.FindStatusCondition(ledger.Status.Conditions, conditionType)
		require.NotNil(t, condition)
		require.Equal(t, metav1.ConditionFalse, condition.Status)
		require.Equal(t, "StorageUpdatePending", condition.Reason)
		require.Contains(t, condition.Message, "requested=")
		require.Contains(t, condition.Message, "capacity=")
		require.Equal(t, int64(7), condition.ObservedGeneration)
	}
	require.Nil(t, meta.FindStatusCondition(ledger.Status.Conditions, dataVolumeExpansionCondition))
	// Complete both CSI resizes. A fresh reconciliation observes completion.
	for ordinal := range 2 {
		for _, kind := range []string{"wal", "data"} {
			pvc := readPersistencePVC(t, c, kind, ordinal)
			pvc.Status.Capacity = pvc.Spec.Resources.Requests.DeepCopy()
			require.NoError(t, c.Status().Update(t.Context(), pvc))
		}
	}
	result, err = reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	require.Equal(t, volumeExpansionRequeueInterval, result.RequeueAfter)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
	require.True(t, meta.IsStatusConditionTrue(ledger.Status.Conditions, dataStorageCondition))
}

func TestPersistenceNeverShrinksAnExpandedPVC(t *testing.T) {
	t.Parallel()
	ledger, _, c := persistenceTestFixture(t, 1)
	ledger.Spec.Persistence.Data.Size = resource.MustParse("20Gi")
	require.NoError(t, c.Update(t.Context(), ledger))
	before := readPersistencePVC(t, c, "data", 0)
	_, err := reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	after := readPersistencePVC(t, c, "data", 0)
	require.Equal(t, before.Spec, after.Spec)
	require.Equal(t, before.ResourceVersion, after.ResourceVersion)
}

func TestPersistenceGrowsOwnedPVCsDespiteStorageDeclarationDrift(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name          string
		templateClass *string
		desiredClass  string
		desiredMode   string
		unsupported   string
	}{
		{name: "defaulted class becomes explicit", desiredClass: "gp3"},
		{name: "explicit class becomes omitted", templateClass: new("gp3")},
		{name: "defaulted class remains omitted"},
		{name: "different explicit class", templateClass: new("gp3"), desiredClass: "io2", unsupported: "changing storageClass"},
		{name: "different defaulted class", desiredClass: "io2", unsupported: "changing storageClass"},
		{name: "different access mode", templateClass: new("gp3"), desiredClass: "gp3", desiredMode: "ReadWriteMany", unsupported: "changing accessMode"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ledger, sts, c := persistenceTestFixture(t, 2)
			sts.Spec.VolumeClaimTemplates[1].Spec.StorageClassName = tt.templateClass
			require.NoError(t, c.Update(t.Context(), sts))
			require.NoError(t, c.Create(t.Context(), &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "gp3"}, AllowVolumeExpansion: new(true)}))
			before := make([]*corev1.PersistentVolumeClaim, 2)
			for ordinal := range 2 {
				pvc := readPersistencePVC(t, c, "data", ordinal)
				pvc.Spec.StorageClassName = new("gp3")
				require.NoError(t, c.Update(t.Context(), pvc))
				before[ordinal] = pvc.DeepCopy()
			}
			ledger.Spec.Persistence.Data.StorageClass = tt.desiredClass
			ledger.Spec.Persistence.Data.AccessMode = tt.desiredMode
			ledger.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
			require.NoError(t, c.Update(t.Context(), ledger))

			_, err := reconcilePersistence(t, c, ledger)
			require.NoError(t, err)
			for ordinal := range 2 {
				pvc := readPersistencePVC(t, c, "data", ordinal)
				expected := before[ordinal].Spec.DeepCopy()
				expected.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("150Gi")
				require.Equal(t, *expected, pvc.Spec, "only the storage request may change")
				require.Equal(t, before[ordinal].UID, pvc.UID)
				pvc.Status.Capacity = pvc.Spec.Resources.Requests.DeepCopy()
				require.NoError(t, c.Status().Update(t.Context(), pvc))
			}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
			condition := meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition)
			require.NotNil(t, condition)
			if tt.unsupported != "" {
				require.Equal(t, "StorageUnsupported", condition.Reason)
				require.Contains(t, condition.Message, tt.unsupported)
			} else {
				require.Equal(t, "StorageUpdatePending", condition.Reason)
			}

			// Capacity completion and the next no-op pass must retain any
			// independent migration diagnosis without rewriting immutable fields.
			_, err = reconcilePersistence(t, c, ledger)
			require.NoError(t, err)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
			condition = meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition)
			if tt.unsupported != "" {
				require.Equal(t, metav1.ConditionFalse, condition.Status)
				require.Equal(t, "StorageUnsupported", condition.Reason)
				require.Contains(t, condition.Message, tt.unsupported)
			} else {
				require.Equal(t, metav1.ConditionTrue, condition.Status)
			}
			var current appsv1.StatefulSet
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sts), &current))
			require.Equal(t, sts.Spec, current.Spec)
		})
	}
}

func TestPersistenceValidatesOnlyChangedVolumeAttributesClass(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name         string
		currentClass string
		size         string
		wantError    bool
	}{
		{name: "unchanged profile no-op", currentClass: "fast", size: "100Gi"},
		{name: "unchanged profile capacity growth", currentClass: "fast", size: "150Gi"},
		{name: "actual profile change still validates", currentClass: "slow", size: "150Gi", wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ledger, _, c := persistenceTestFixture(t, 1)
			pvc := readPersistencePVC(t, c, "data", 0)
			pvc.Spec.VolumeAttributesClassName = new(tt.currentClass)
			require.NoError(t, c.Update(t.Context(), pvc))
			pvc.Status.CurrentVolumeAttributesClassName = new(tt.currentClass)
			require.NoError(t, c.Status().Update(t.Context(), pvc))
			before := pvc.DeepCopy()
			ledger.Spec.Persistence.Data.VolumeAttributesClassName = "fast"
			ledger.Spec.Persistence.Data.Size = resource.MustParse(tt.size)
			require.NoError(t, c.Update(t.Context(), ledger))
			outage := errors.New("VolumeAttributesClass API unavailable")
			versions := &volumeAttributesVersionClient{Client: c, stableErr: outage}
			_, err := reconcilePersistence(t, versions, ledger)
			current := readPersistencePVC(t, c, "data", 0)
			if tt.wantError {
				require.ErrorIs(t, err, outage)
				require.Equal(t, 1, versions.stableReads)
				require.Equal(t, before.Spec, current.Spec)
			} else {
				require.NoError(t, err)
				require.Zero(t, versions.stableReads)
				require.Zero(t, versions.betaReads)
				expected := before.Spec.DeepCopy()
				expected.Resources.Requests[corev1.ResourceStorage] = resource.MustParse(tt.size)
				require.Equal(t, *expected, current.Spec)
				if tt.size == "100Gi" {
					require.Equal(t, before.ResourceVersion, current.ResourceVersion)
				}
			}
			require.Equal(t, before.UID, current.UID)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
			condition := meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition)
			require.NotNil(t, condition)
			switch {
			case tt.wantError:
				require.Equal(t, "StorageUpdateFailed", condition.Reason)
			case tt.size == "100Gi":
				require.Equal(t, "Ready", condition.Reason)
			default:
				require.Equal(t, "StorageUpdatePending", condition.Reason)
			}
		})
	}
}

func TestPersistenceChangesVolumeAttributesInPlace(t *testing.T) {
	t.Parallel()
	for _, initial := range []string{"", "slow"} {
		t.Run("from-"+initial, func(t *testing.T) {
			t.Parallel()
			ledger, _, c := persistenceTestFixture(t, 1)
			pvc := readPersistencePVC(t, c, "data", 0)
			pvc.Spec.VolumeAttributesClassName = new(initial)
			require.NoError(t, c.Update(t.Context(), pvc))
			ledger.Spec.Persistence.Data.VolumeAttributesClassName = "fast"
			require.NoError(t, c.Update(t.Context(), ledger))
			_, err := reconcilePersistence(t, c, ledger)
			require.NoError(t, err)
			pvc = readPersistencePVC(t, c, "data", 0)
			require.Equal(t, "fast", *pvc.Spec.VolumeAttributesClassName)
			require.Equal(t, types.UID(pvc.Name+"-uid"), pvc.UID)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
			require.False(t, meta.IsStatusConditionTrue(ledger.Status.Conditions, dataStorageCondition))
			pvc.Status.CurrentVolumeAttributesClassName = new("fast")
			require.NoError(t, c.Status().Update(t.Context(), pvc))
			_, err = reconcilePersistence(t, c, ledger)
			require.NoError(t, err)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
			require.True(t, meta.IsStatusConditionTrue(ledger.Status.Conditions, dataStorageCondition))
		})
	}
}

// Fake clients exercise the replica boundary and request/observation split;
// they do not run StatefulSet/CSI controllers or prove effective throughput.
func TestPersistenceRetainedClaimsConvergeAfterScaleUp(t *testing.T) {
	t.Parallel()
	ledger, sts, c := persistenceTestFixture(t, 3)
	for ordinal := range 3 {
		for _, kind := range []string{"wal", "data"} {
			pvc := readPersistencePVC(t, c, kind, ordinal)
			// Retain-policy claims have no owner reference. A live owned Pod
			// must establish their identity when the ordinal returns.
			pvc.OwnerReferences = nil
			pvc.Spec.VolumeAttributesClassName = new("slow")
			require.NoError(t, c.Update(t.Context(), pvc))
			pvc.Status.CurrentVolumeAttributesClassName = new("slow")
			require.NoError(t, c.Status().Update(t.Context(), pvc))
		}
	}
	setReplicas := func(replicas int32) {
		t.Helper()
		ledger.Spec.Replicas = &replicas
		require.NoError(t, c.Update(t.Context(), ledger))
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sts), sts))
		sts.Spec.Replicas = &replicas
		require.NoError(t, c.Update(t.Context(), sts))
	}
	createPod := func(ordinal int) {
		t.Helper()
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%d", sts.Name, ordinal), Namespace: sts.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(sts, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
		}}
		for _, kind := range []string{"wal", "data"} {
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: kind, VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: readPersistencePVC(t, c, kind, ordinal).Name},
			}})
		}
		require.NoError(t, c.Create(t.Context(), pod))
	}
	setReplicas(1) // Higher ordinals retain their claims but have no live Pods.
	createPod(0)
	ledger.Spec.Persistence.WAL.VolumeAttributesClassName = "fast"
	ledger.Spec.Persistence.Data.VolumeAttributesClassName = "fast"
	require.NoError(t, c.Update(t.Context(), ledger))
	// Simulate installation of the recovery's aligned immutable templates.
	// The recovery/adoption suites separately exercise their durable handoff.
	aligned, changed, err := alignedClaimTemplates(sts.Spec.VolumeClaimTemplates, buildVolumeClaimTemplates(ledger))
	require.NoError(t, err)
	require.True(t, changed)
	sts.Spec.VolumeClaimTemplates = aligned
	require.NoError(t, c.Update(t.Context(), sts))
	for _, claim := range sts.Spec.VolumeClaimTemplates {
		require.Equal(t, "fast", *claim.Spec.VolumeAttributesClassName)
	}
	_, err = reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	for ordinal := range 3 {
		want := "slow"
		if ordinal == 0 {
			want = "fast"
		}
		for _, kind := range []string{"wal", "data"} {
			pvc := readPersistencePVC(t, c, kind, ordinal)
			require.Equal(t, want, *pvc.Spec.VolumeAttributesClassName)
			if ordinal == 0 {
				pvc.Status.CurrentVolumeAttributesClassName = new("fast")
				require.NoError(t, c.Status().Update(t.Context(), pvc))
			}
		}
	}
	_, err = reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
	for _, condition := range []string{walStorageCondition, dataStorageCondition} {
		require.True(t, meta.IsStatusConditionTrue(ledger.Status.Conditions, condition), "readiness covers only live ordinals")
	}
	setReplicas(3)
	createPod(1)
	createPod(2)
	_, err = reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
	for _, condition := range []string{walStorageCondition, dataStorageCondition} {
		require.False(t, meta.IsStatusConditionTrue(ledger.Status.Conditions, condition), "reused claims still await CSI completion")
	}
	for ordinal := range 3 {
		for _, kind := range []string{"wal", "data"} {
			pvc := readPersistencePVC(t, c, kind, ordinal)
			require.Equal(t, "fast", *pvc.Spec.VolumeAttributesClassName)
			require.Equal(t, types.UID(pvc.Name+"-uid"), pvc.UID)
			require.Equal(t, "pv-"+pvc.Name, pvc.Spec.VolumeName)
			pvc.Status.CurrentVolumeAttributesClassName = new("fast")
			require.NoError(t, c.Status().Update(t.Context(), pvc))
		}
	}
	_, err = reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
	for _, condition := range []string{walStorageCondition, dataStorageCondition} {
		require.True(t, meta.IsStatusConditionTrue(ledger.Status.Conditions, condition))
	}
}

func TestPersistenceRejectsUnsupportedChanges(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*testing.T, *ledgerv1alpha1.Cluster, *appsv1.StatefulSet, client.Client)
		message string
	}{
		{name: "fixed StorageClass", mutate: func(t *testing.T, ledger *ledgerv1alpha1.Cluster, _ *appsv1.StatefulSet, c client.Client) {
			var class storagev1.StorageClass
			require.NoError(t, c.Get(t.Context(), types.NamespacedName{Name: "expandable"}, &class))
			class.AllowVolumeExpansion = new(false)
			require.NoError(t, c.Update(t.Context(), &class))
			ledger.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
		}, message: "allowVolumeExpansion"},
		{name: "class reset", mutate: func(t *testing.T, _ *ledgerv1alpha1.Cluster, _ *appsv1.StatefulSet, c client.Client) {
			pvc := readPersistencePVC(t, c, "data", 0)
			pvc.Spec.VolumeAttributesClassName = new("fast")
			require.NoError(t, c.Update(t.Context(), pvc))
		}, message: "resetting a non-empty"},
		{name: "missing attributes class", mutate: func(_ *testing.T, ledger *ledgerv1alpha1.Cluster, _ *appsv1.StatefulSet, _ client.Client) {
			ledger.Spec.Persistence.Data.VolumeAttributesClassName = "missing"
		}, message: "VolumeAttributesClass missing"},
		{name: "incompatible CSI driver", mutate: func(t *testing.T, ledger *ledgerv1alpha1.Cluster, _ *appsv1.StatefulSet, c client.Client) {
			var class storagev1beta1.VolumeAttributesClass
			require.NoError(t, c.Get(t.Context(), types.NamespacedName{Name: "fast"}, &class))
			class.DriverName = "different.csi"
			require.NoError(t, c.Update(t.Context(), &class))
			ledger.Spec.Persistence.Data.VolumeAttributesClassName = "fast"
		}, message: "incompatible"},
		{name: "StorageClass migration", mutate: func(_ *testing.T, ledger *ledgerv1alpha1.Cluster, _ *appsv1.StatefulSet, _ client.Client) {
			ledger.Spec.Persistence.Data.StorageClass = "new"
		}, message: "changing storageClass"},
		{name: "access mode migration", mutate: func(_ *testing.T, ledger *ledgerv1alpha1.Cluster, _ *appsv1.StatefulSet, _ client.Client) {
			ledger.Spec.Persistence.Data.AccessMode = "ReadWriteMany"
		}, message: "changing accessMode"},
		{name: "foreign StatefulSet", mutate: func(t *testing.T, _ *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, c client.Client) {
			sts.OwnerReferences[0].UID = "other-cluster"
			require.NoError(t, c.Update(t.Context(), sts))
		}, message: "resource name conflict"},
		{name: "foreign PVC", mutate: func(t *testing.T, _ *ledgerv1alpha1.Cluster, _ *appsv1.StatefulSet, c client.Client) {
			pvc := readPersistencePVC(t, c, "data", 0)
			pvc.OwnerReferences[0].UID = "other-statefulset"
			require.NoError(t, c.Update(t.Context(), pvc))
		}, message: "foreign owner"},
		{name: "ambiguous retained PVC", mutate: func(t *testing.T, _ *ledgerv1alpha1.Cluster, _ *appsv1.StatefulSet, c client.Client) {
			pvc := readPersistencePVC(t, c, "data", 0)
			pvc.OwnerReferences = nil
			require.NoError(t, c.Update(t.Context(), pvc))
		}, message: "establishing PVC"},
		{name: "foreign PV binding", mutate: func(t *testing.T, ledger *ledgerv1alpha1.Cluster, _ *appsv1.StatefulSet, c client.Client) {
			var pv corev1.PersistentVolume
			require.NoError(t, c.Get(t.Context(), types.NamespacedName{Name: "pv-data-ledger-storage-0"}, &pv))
			pv.Spec.ClaimRef.UID = "other-pvc"
			require.NoError(t, c.Update(t.Context(), &pv))
			ledger.Spec.Persistence.Data.VolumeAttributesClassName = "fast"
		}, message: "does not bind PVC"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ledger, sts, c := persistenceTestFixture(t, 1)
			tt.mutate(t, ledger, sts, c)
			require.NoError(t, c.Update(t.Context(), ledger))
			before := readPersistencePVC(t, c, "data", 0)
			_, _ = reconcilePersistence(t, c, ledger) // the condition carries both permanent and retryable API failures
			after := readPersistencePVC(t, c, "data", 0)
			require.Equal(t, before.Spec, after.Spec)
			require.Equal(t, before.UID, after.UID)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
			condition := meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition)
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Contains(t, condition.Message, tt.message)
		})
	}
}

func TestPersistenceAcceptsRetainedPVCWithOwnedPodEvidence(t *testing.T) {
	t.Parallel()
	ledger, sts, c := persistenceTestFixture(t, 1)
	pvc := readPersistencePVC(t, c, "data", 0)
	pvc.OwnerReferences = nil
	require.NoError(t, c.Update(t.Context(), pvc))
	require.NoError(t, c.Create(t.Context(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger-storage-0", Namespace: ledger.Namespace, UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(sts, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))}},
		Spec:       corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}}}}},
	}))
	ledger.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
	require.NoError(t, c.Update(t.Context(), ledger))
	_, err := reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	current := readPersistencePVC(t, c, "data", 0)
	request := current.Spec.Resources.Requests[corev1.ResourceStorage]
	require.Equal(t, "150Gi", request.String())
	require.Empty(t, current.OwnerReferences, "ownership evidence must not alter retention")
}

func TestPersistencePartialProgressRecoversAfterRestart(t *testing.T) {
	t.Parallel()
	ledger, _, c := persistenceTestFixture(t, 3)
	ledger.Spec.Persistence.WAL.Size = resource.MustParse("8Gi")
	ledger.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
	require.NoError(t, c.Update(t.Context(), ledger))
	failing := &failNthPatchClient{Client: c, failAt: 2}
	_, err := reconcilePersistence(t, failing, ledger)
	require.ErrorContains(t, err, "injected PVC patch failure")
	for ordinal, expected := range []string{"8Gi", "5Gi", "5Gi"} {
		pvc := readPersistencePVC(t, c, "wal", ordinal)
		request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		require.Equal(t, expected, request.String())
	}
	for ordinal := range 3 {
		pvc := readPersistencePVC(t, c, "data", ordinal)
		request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		require.Equal(t, "150Gi", request.String(), "data makes progress independently of the WAL error")
	}
	_, err = reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	for ordinal := range 3 {
		pvc := readPersistencePVC(t, c, "wal", ordinal)
		request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		require.Equal(t, "8Gi", request.String())
	}
}

// Deliberately interpose a real concurrent update between the request read and
// optimistic patch. The failed attempt must not lower its larger size on retry.
type concurrentPersistencePatchClient struct {
	client.Client

	attempts int
}

func (c *concurrentPersistencePatchClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	c.attempts++
	if c.attempts == 1 {
		var concurrent corev1.PersistentVolumeClaim
		if err := c.Get(ctx, client.ObjectKeyFromObject(object), &concurrent); err != nil {
			return err
		}
		concurrent.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("180Gi")
		if err := c.Update(ctx, &concurrent); err != nil {
			return err
		}
	}

	return c.Client.Patch(ctx, object, patch, options...)
}

func TestPersistenceConflictRetryPreservesConcurrentAutomaticGrowth(t *testing.T) {
	t.Parallel()
	ledger, _, c := persistenceTestFixture(t, 1)
	ledger.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
	ledger.Spec.Persistence.Data.VolumeAttributesClassName = "fast"
	require.NoError(t, c.Update(t.Context(), ledger))
	concurrent := &concurrentPersistencePatchClient{Client: c}
	_, err := reconcilePersistence(t, concurrent, ledger)
	require.NoError(t, err)
	require.Equal(t, 2, concurrent.attempts)
	pvc := readPersistencePVC(t, c, "data", 0)
	request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	require.Equal(t, "180Gi", request.String())
	require.Equal(t, "fast", *pvc.Spec.VolumeAttributesClassName)
}

// API rejection and silent feature-gate pruning both need a visible failure.
type unsupportedPersistencePatchClient struct {
	client.Client

	prune bool
}

func (c *unsupportedPersistencePatchClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if !c.prune {
		return k8serrors.NewForbidden(schema.GroupResource{Resource: "persistentvolumeclaims"}, object.GetName(), errors.New("VolumeAttributesClass feature is disabled"))
	}
	if err := c.Client.Patch(ctx, object, patch, options...); err != nil {
		return err
	}
	pvc := object.(*corev1.PersistentVolumeClaim)
	pvc.Spec.VolumeAttributesClassName = nil

	return c.Update(ctx, pvc)
}

func TestPersistenceSurfacesUnavailableVolumeAttributesCapability(t *testing.T) {
	t.Parallel()
	for _, prune := range []bool{false, true} {
		t.Run(fmt.Sprint("prune-", prune), func(t *testing.T) {
			t.Parallel()
			ledger, _, c := persistenceTestFixture(t, 1)
			ledger.Spec.Persistence.Data.VolumeAttributesClassName = "fast"
			require.NoError(t, c.Update(t.Context(), ledger))
			unsupported := &unsupportedPersistencePatchClient{Client: c, prune: prune}
			_, err := reconcilePersistence(t, unsupported, ledger)
			require.Error(t, err)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
			condition := meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition)
			require.NotNil(t, condition)
			require.Equal(t, "StorageUpdateFailed", condition.Reason)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
		})
	}
}

func TestPersistenceFloorSupersedesAutomaticMaximum(t *testing.T) {
	t.Parallel()
	ledger, _, c := persistenceTestFixture(t, 2)
	ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
	maximum := resource.MustParse("150Gi")
	ledger.Spec.Persistence.Data.AutoExpansion = &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: &maximum}
	require.NoError(t, c.Update(t.Context(), ledger))
	_, err := reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	for ordinal := range 2 {
		pvc := readPersistencePVC(t, c, "data", ordinal)
		request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		require.Equal(t, "200Gi", request.String())
	}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
	condition := meta.FindStatusCondition(ledger.Status.Conditions, dataVolumeExpansionCondition)
	require.NotNil(t, condition)
	require.Equal(t, "RequestAboveMaximum", condition.Reason, "the policy is valid; only automatic growth is capped")
	storage := meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition)
	require.NotNil(t, storage)
	require.Equal(t, "StorageUpdatePending", storage.Reason)
}

func TestPersistencePendingFloorDoesNotBlockAutomaticExpansion(t *testing.T) {
	t.Parallel()
	ledger, _, c := persistenceTestFixture(t, 1)
	ledger.Spec.Persistence.WAL.Size = resource.MustParse("8Gi")
	maximum := resource.MustParse("200Gi")
	ledger.Spec.Persistence.Data.AutoExpansion = &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: &maximum}
	require.NoError(t, c.Update(t.Context(), ledger))
	calls := 0
	r := &VolumeExpansionReconciler{Client: c, APIReader: c, ReadDiskUsage: func(context.Context, *ledgerv1alpha1.Cluster, string, string) (podDiskUsage, error) {
		calls++

		return podDiskUsage{Data: freshMeasuredVolume(uint64(testQuantityValue("80Gi")), uint64(testQuantityValue("100Gi")), time.Now())}, nil
	}}
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ledger)})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	pvc := readPersistencePVC(t, c, "data", 0)
	request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	require.Equal(t, "146Gi", request.String())
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
	condition := meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition)
	require.Equal(t, "StorageUpdatePending", condition.Reason)
	require.Contains(t, condition.Message, "requested=146Gi capacity=100Gi")
	wal := readPersistencePVC(t, c, "wal", 0)
	request = wal.Spec.Resources.Requests[corev1.ResourceStorage]
	require.Equal(t, "8Gi", request.String())
}

func TestPersistenceMissingStatefulSetIsRetryableWithoutMutations(t *testing.T) {
	t.Parallel()
	ledger, sts, c := persistenceTestFixture(t, 1)
	require.NoError(t, c.Delete(t.Context(), sts))
	before := readPersistencePVC(t, c, "data", 0)
	result, err := reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	require.Equal(t, volumeExpansionRetryInterval, result.RequeueAfter)
	after := readPersistencePVC(t, c, "data", 0)
	require.Equal(t, before.ResourceVersion, after.ResourceVersion)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
	require.Equal(t, "StatefulSetUnavailable", meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition).Reason)
}

func TestPersistenceStatusOwnershipPreservesMainWriter(t *testing.T) {
	t.Parallel()
	ledger, _, c := persistenceTestFixture(t, 1)
	ledger.Status.Phase = "Running"
	ledger.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Healthy"}}
	require.NoError(t, c.Status().Update(t.Context(), ledger))
	_, err := reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
	require.Equal(t, "Running", ledger.Status.Phase)
	require.True(t, meta.IsStatusConditionTrue(ledger.Status.Conditions, "Ready"))
	for _, kind := range []string{walStorageCondition, dataStorageCondition, walVolumeExpansionCondition, dataVolumeExpansionCondition} {
		require.True(t, isVolumeExpansionCondition(metav1.Condition{Type: kind}))
	}
	require.False(t, isVolumeExpansionCondition(metav1.Condition{Type: "Ready"}))
}

func TestAutomaticPatchConflictPreservesConcurrentManualFloor(t *testing.T) {
	t.Parallel()
	ledger, sts, c := persistenceTestFixture(t, 1)
	pvc := readPersistencePVC(t, c, "data", 0)
	concurrent := &concurrentPersistencePatchClient{Client: c}
	r := &VolumeExpansionReconciler{Client: concurrent, APIReader: c}
	storage := &persistenceStorageReconciler{client: c, reader: c, ledger: ledger, statefulSet: sts}
	err := r.patchPVCGroup(t.Context(), []*corev1.PersistentVolumeClaim{pvc}, testQuantityValue("146Gi"), time.Now(), storage, persistenceVolumeDefinitions(ledger)[1])
	require.NoError(t, err)
	require.Equal(t, 1, concurrent.attempts, "a real optimistic-lock conflict is followed by a fresh read and no lower write")
	current := readPersistencePVC(t, c, "data", 0)
	request := current.Spec.Resources.Requests[corev1.ResourceStorage]
	require.Equal(t, "180Gi", request.String())
	require.Empty(t, current.Annotations[annotationLastExpansionTarget])
}

func TestPersistenceRefusesReplacedPVCIdentity(t *testing.T) {
	t.Parallel()
	ledger, sts, c := persistenceTestFixture(t, 1)
	original := readPersistencePVC(t, c, "data", 0)
	require.NoError(t, c.Delete(t.Context(), original))
	replacement := original.DeepCopy()
	replacement.ResourceVersion = ""
	replacement.UID = "replacement-pvc"
	replacement.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("180Gi")
	require.NoError(t, c.Create(t.Context(), replacement))
	r := &persistenceStorageReconciler{client: c, reader: c, ledger: ledger, statefulSet: sts}
	_, err := r.patchPVC(t.Context(), persistenceVolumeDefinitions(ledger)[1], original, resource.MustParse("150Gi"))
	require.ErrorContains(t, err, "was replaced during reconciliation")
	current := readPersistencePVC(t, c, "data", 0)
	require.Equal(t, replacement.UID, current.UID)
	request := current.Spec.Resources.Requests[corev1.ResourceStorage]
	require.Equal(t, "180Gi", request.String())
}

func TestPersistenceConditionsSurvivePrimaryStatusUpdate(t *testing.T) {
	t.Parallel()
	ledger, _, c := persistenceTestFixture(t, 1)
	stale := ledger.DeepCopy()
	_, err := reconcilePersistence(t, c, ledger)
	require.NoError(t, err)
	primary := &ClusterReconciler{Client: c}
	require.NoError(t, primary.updateStatus(t.Context(), stale))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
	require.True(t, meta.IsStatusConditionTrue(ledger.Status.Conditions, walStorageCondition))
	require.True(t, meta.IsStatusConditionTrue(ledger.Status.Conditions, dataStorageCondition))
}

type volumeAttributesVersionClient struct {
	client.Client

	stableErr   error
	stableReads int
	betaReads   int
}

func (c *volumeAttributesVersionClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if object.GetObjectKind().GroupVersionKind() == storagev1.SchemeGroupVersion.WithKind("VolumeAttributesClass") {
		c.stableReads++
		if c.stableErr != nil {
			return c.stableErr
		}
	}
	if _, ok := object.(*storagev1beta1.VolumeAttributesClass); ok {
		c.betaReads++
	}

	return c.Client.Get(ctx, key, object, options...)
}

func TestPersistenceSupportsStableOnlyVolumeAttributesAPI(t *testing.T) {
	t.Parallel()
	ledger, _, c := persistenceTestFixture(t, 1)
	var beta storagev1beta1.VolumeAttributesClass
	require.NoError(t, c.Get(t.Context(), types.NamespacedName{Name: "fast"}, &beta))
	require.NoError(t, c.Delete(t.Context(), &beta))
	stable := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttributesClass",
		"metadata": map[string]any{"name": "fast"}, "driverName": "example.csi",
		"parameters": map[string]any{"iops": "5000"},
	}}
	require.NoError(t, c.Create(t.Context(), stable))
	ledger.Spec.Persistence.Data.VolumeAttributesClassName = "fast"
	require.NoError(t, c.Update(t.Context(), ledger))
	versions := &volumeAttributesVersionClient{Client: c}
	_, err := reconcilePersistence(t, versions, ledger)
	require.NoError(t, err)
	require.Positive(t, versions.stableReads)
	require.Zero(t, versions.betaReads, "a stable-only server must not need the removed beta API")
	pvc := readPersistencePVC(t, c, "data", 0)
	require.Equal(t, "fast", *pvc.Spec.VolumeAttributesClassName)
}

func TestPersistenceVolumeAttributesVersionFallback(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		err      error
		wantBeta bool
	}{
		{name: "older cluster has no stable API", err: &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "storage.k8s.io", Kind: "VolumeAttributesClass"}, SearchedVersions: []string{"v1"}}, wantBeta: true},
		{name: "authorization error remains visible", err: k8serrors.NewForbidden(schema.GroupResource{Group: "storage.k8s.io", Resource: "volumeattributesclasses"}, "fast", errors.New("permission denied"))},
		{name: "transient API error remains visible", err: errors.New("connection reset")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, c := persistenceTestFixture(t, 1)
			versions := &volumeAttributesVersionClient{Client: c, stableErr: tt.err}
			r := &persistenceStorageReconciler{reader: versions}
			driver, err := r.volumeAttributesDriver(t.Context(), "fast")
			require.Equal(t, 1, versions.stableReads)
			if tt.wantBeta {
				require.NoError(t, err)
				require.Equal(t, "example.csi", driver)
				require.Equal(t, 1, versions.betaReads)
			} else {
				require.ErrorIs(t, err, tt.err)
				require.Zero(t, versions.betaReads)
			}
		})
	}
}

type ownershipChangePatchClient struct {
	client.Client

	attempts         int
	beforeFirstPatch func(context.Context) error
}

func (c *ownershipChangePatchClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	c.attempts++
	if c.attempts == 1 && c.beforeFirstPatch != nil {
		if err := c.beforeFirstPatch(ctx); err != nil {
			return err
		}
	}

	return c.Client.Patch(ctx, object, patch, options...)
}

func TestAutomaticExpansionRevalidatesSameUIDOwnershipAndRecovers(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name           string
		statefulSet    bool
		duringConflict bool
		storageDrift   bool
		wantErr        string
	}{
		{name: "PVC owner changes during measurement", wantErr: "foreign owner"},
		{name: "StatefulSet owner changes during measurement", statefulSet: true, wantErr: "resource name conflict"},
		{name: "PVC owner changes before conflict retry", duringConflict: true, wantErr: "foreign owner"},
		{name: "PVC owner changes during measurement with storage drift", storageDrift: true, wantErr: "foreign owner"},
		{name: "PVC owner changes before conflict retry with storage drift", duringConflict: true, storageDrift: true, wantErr: "foreign owner"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ledger, sts, c := persistenceTestFixture(t, 1)
			maximum := resource.MustParse("200Gi")
			ledger.Spec.Persistence.Data.AutoExpansion = &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: &maximum}
			if tt.storageDrift {
				ledger.Spec.Persistence.Data.StorageClass = "io2"
			}
			require.NoError(t, c.Update(t.Context(), ledger))
			original := readPersistencePVC(t, c, "data", 0)
			var owned client.Object = original
			if tt.statefulSet {
				owned = sts
			}
			originalUID := owned.GetUID()
			originalOwners := append([]metav1.OwnerReference(nil), owned.GetOwnerReferences()...)
			changeOwner := func(ctx context.Context) error {
				if err := c.Get(ctx, client.ObjectKeyFromObject(owned), owned); err != nil {
					return err
				}
				foreign := append([]metav1.OwnerReference(nil), owned.GetOwnerReferences()...)
				foreign[0].UID = "foreign-owner-same-object-uid"
				owned.SetOwnerReferences(foreign)

				return c.Update(ctx, owned)
			}
			patches := &ownershipChangePatchClient{Client: c}
			if tt.duringConflict {
				patches.beforeFirstPatch = changeOwner
			}
			measurementCalls := 0
			read := func(ctx context.Context, _ *ledgerv1alpha1.Cluster, _ string, _ string) (podDiskUsage, error) {
				measurementCalls++
				if measurementCalls == 1 && !tt.duringConflict {
					if err := changeOwner(ctx); err != nil {
						return podDiskUsage{}, err
					}
				}

				return podDiskUsage{Data: freshMeasuredVolume(uint64(testQuantityValue("80Gi")), uint64(testQuantityValue("100Gi")), time.Now())}, nil
			}
			r := &VolumeExpansionReconciler{Client: patches, APIReader: c, ReadDiskUsage: read}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ledger)})
			require.ErrorContains(t, err, tt.wantErr)
			current := readPersistencePVC(t, c, "data", 0)
			request := current.Spec.Resources.Requests[corev1.ResourceStorage]
			require.Equal(t, "100Gi", request.String(), "a new resourceVersion cannot authorize a foreign PVC")
			require.Equal(t, original.UID, current.UID)
			require.Equal(t, original.Spec.VolumeName, current.Spec.VolumeName)
			require.Equal(t, originalUID, owned.GetUID(), "the ownership edit must not replace the object")
			failedAttempts := 0
			if tt.duringConflict {
				failedAttempts = 1
			}
			require.Equal(t, failedAttempts, patches.attempts, "the retry must stop before any patch under the foreign owner")
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
			require.Equal(t, "PatchFailed", meta.FindStatusCondition(ledger.Status.Conditions, dataVolumeExpansionCondition).Reason)
			require.Equal(t, "StorageUnsupported", meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition).Reason)

			// Restore ownership on exactly the same objects and reconstruct the
			// controller. Persisted state must allow the next real write to succeed.
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(owned), owned))
			owned.SetOwnerReferences(originalOwners)
			require.NoError(t, c.Update(t.Context(), owned))
			r = &VolumeExpansionReconciler{Client: patches, APIReader: c, ReadDiskUsage: read}
			_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ledger)})
			require.NoError(t, err)
			require.Equal(t, 2, measurementCalls)
			require.Equal(t, failedAttempts+1, patches.attempts)
			current = readPersistencePVC(t, c, "data", 0)
			request = current.Spec.Resources.Requests[corev1.ResourceStorage]
			require.Equal(t, "146Gi", request.String())
			require.Equal(t, original.UID, current.UID)
			require.Equal(t, original.Spec.VolumeName, current.Spec.VolumeName)
			require.Equal(t, "146Gi", current.Annotations[annotationLastExpansionTarget])
			if tt.storageDrift {
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
				require.Equal(t, "StorageUnsupported", meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition).Reason)
			}
		})
	}
}
