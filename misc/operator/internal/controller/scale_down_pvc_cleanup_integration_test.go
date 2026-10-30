//go:build integration

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The API server must reject the actual stale DELETE, rather than a fake
// reactor only inspecting options. A new reconciler then rereads the same
// authorized identity and observes a successful second DELETE.
func TestScaleDownPVCCleanupMetadataConflictRetriesInAPI(t *testing.T) {
	t.Parallel()
	f := newScaleDownPVCCleanupAPIFixture(t, true)
	intercept := &scaleDownPVCAPIDeleteClient{Interface: f.cs}
	var changed *corev1.PersistentVolumeClaim
	intercept.beforeDelete = func(ctx context.Context, name string) {
		pvc, err := f.cs.CoreV1().PersistentVolumeClaims(f.sts.Namespace).Get(ctx, name, metav1.GetOptions{})
		require.NoError(t, err)
		pvc.Labels = map[string]string{"concurrent-write": "retained"}
		changed, err = f.cs.CoreV1().PersistentVolumeClaims(f.sts.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		require.NoError(t, err)
		require.Equal(t, f.pvc.UID, changed.UID)
		require.NotEqual(t, f.pvc.ResourceVersion, changed.ResourceVersion)
	}
	err := f.resume(t, intercept)
	require.True(t, apierrors.IsConflict(err), "the API must reject the stale resourceVersion: %v", err)
	require.Len(t, intercept.attempts, 1)
	require.Len(t, intercept.results, 1)
	require.True(t, apierrors.IsConflict(intercept.results[0]))
	require.Equal(t, f.pvc.UID, *intercept.attempts[0].Preconditions.UID)
	require.Equal(t, f.pvc.ResourceVersion, *intercept.attempts[0].Preconditions.ResourceVersion)
	pvc, err := f.cs.CoreV1().PersistentVolumeClaims(f.sts.Namespace).Get(t.Context(), f.pvc.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Nil(t, pvc.DeletionTimestamp)
	require.Equal(t, changed.UID, pvc.UID)
	require.Equal(t, "retained", pvc.Labels["concurrent-write"])
	f.requirePersistedInventory(t)

	// resume constructs a new uncached client and reconciler each time. It must
	// decode the original inventory, not capture changed objects as new targets.
	require.NoError(t, f.resume(t, intercept))
	require.Len(t, intercept.attempts, 2)
	require.Len(t, intercept.results, 2)
	require.NoError(t, intercept.results[1])
	require.Equal(t, f.pvc.UID, *intercept.attempts[1].Preconditions.UID)
	require.Equal(t, changed.ResourceVersion, *intercept.attempts[1].Preconditions.ResourceVersion)
	f.requireDeletionAccepted(t)
	sts := f.readStatefulSet(t)
	require.NotContains(t, sts.Annotations, annotationScaleDownPVCFrom)
	require.NotContains(t, sts.Annotations, annotationScaleDownPVCPlan)
	require.NoError(t, f.resume(t, intercept))
	require.Len(t, intercept.attempts, 2, "completed cleanup must not issue another DELETE")
	pv, err := f.cs.CoreV1().PersistentVolumes().Get(t.Context(), f.pv.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, f.pv.UID, pv.UID)
	require.Equal(t, f.pv.Spec.ClaimRef, pv.Spec.ClaimRef)
}

func TestScaleDownPVCCleanupRejectsSameNameReplacementInAPI(t *testing.T) {
	t.Parallel()
	for _, when := range []string{"before cleanup", "after final read"} {
		t.Run(when, func(t *testing.T) {
			t.Parallel()
			f := newScaleDownPVCCleanupAPIFixture(t, false)
			intercept := &scaleDownPVCAPIDeleteClient{Interface: f.cs}
			var replacement *corev1.PersistentVolumeClaim
			if when == "before cleanup" {
				replacement = f.replacePVC(t)
			} else {
				intercept.beforeDelete = func(context.Context, string) { replacement = f.replacePVC(t) }
			}
			err := f.resume(t, intercept)
			if when == "before cleanup" {
				require.ErrorContains(t, err, "identity changed")
				require.Empty(t, intercept.attempts)
			} else {
				require.True(t, apierrors.IsConflict(err), "the API must reject the stale UID: %v", err)
				require.Len(t, intercept.attempts, 1)
				require.Len(t, intercept.results, 1)
				require.True(t, apierrors.IsConflict(intercept.results[0]))
				require.Equal(t, f.pvc.UID, *intercept.attempts[0].Preconditions.UID)
				require.Equal(t, f.pvc.ResourceVersion, *intercept.attempts[0].Preconditions.ResourceVersion)
			}
			require.NotNil(t, replacement)
			require.NotEqual(t, f.pvc.UID, replacement.UID)
			attempts := len(intercept.attempts)
			require.ErrorContains(t, f.resume(t, intercept), "identity changed")
			require.Len(t, intercept.attempts, attempts, "a restart must not adopt the replacement into cleanup")
			live, getErr := f.cs.CoreV1().PersistentVolumeClaims(f.sts.Namespace).Get(t.Context(), f.pvc.Name, metav1.GetOptions{})
			require.NoError(t, getErr)
			require.Equal(t, replacement.UID, live.UID)
			require.Nil(t, live.DeletionTimestamp)
			f.requirePersistedInventory(t)
		})
	}
}

func TestScaleDownPVCCleanupRejectsPVBindingChangeInAPI(t *testing.T) {
	t.Parallel()
	f := newScaleDownPVCCleanupAPIFixture(t, true)
	changed := f.pv.DeepCopy()
	changed.Spec.ClaimRef.UID = types.UID("foreign-claim")
	_, err := f.cs.CoreV1().PersistentVolumes().Update(t.Context(), changed, metav1.UpdateOptions{})
	require.NoError(t, err)
	intercept := &scaleDownPVCAPIDeleteClient{Interface: f.cs}
	require.ErrorContains(t, f.resume(t, intercept), "identity or binding changed")
	require.Empty(t, intercept.attempts)
	pvc, err := f.cs.CoreV1().PersistentVolumeClaims(f.sts.Namespace).Get(t.Context(), f.pvc.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, f.pvc.UID, pvc.UID)
	require.Nil(t, pvc.DeletionTimestamp)
	f.requirePersistedInventory(t)
}

type scaleDownPVCCleanupAPIFixture struct {
	ledger    *ledgerv1alpha1.Cluster
	sts       *appsv1.StatefulSet
	pvc       *corev1.PersistentVolumeClaim
	pv        *corev1.PersistentVolume
	cs        kubernetes.Interface
	inventory string
}

func newScaleDownPVCCleanupAPIFixture(t *testing.T, bound bool) *scaleDownPVCCleanupAPIFixture {
	t.Helper()
	namespace := createTestNamespace(t)
	ledger := newCluster("identity", namespace)
	ledger.UID = types.UID(namespace + "-cluster")
	ledger.Spec.Replicas = new(int32(2))
	applyDefaults(ledger)
	ledger.Spec.Persistence.DeletionProtection = new(false)
	ledger.Spec.Persistence.RetentionPolicy = &ledgerv1alpha1.RetentionPolicySpec{WhenScaled: "Delete", WhenDeleted: "Delete"}
	cs, err := kubernetes.NewForConfig(testEnv.Config)
	require.NoError(t, err)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceName(ledger.Name), Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ledger, schema.GroupVersionKind{Group: "ledger.formance.com", Version: "v1alpha1", Kind: "Cluster"})},
		},
		Spec: buildStatefulSetSpec(ledger, computeSpecHash(&ledger.Spec), nil, "disabled"),
	}
	require.NoError(t, k8sClient.Create(t.Context(), sts))
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("data-%s-1", sts.Name), Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(sts, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}
	if bound {
		pvc.Spec.VolumeName = namespace + "-volume"
	}
	pvc, err = cs.CoreV1().PersistentVolumeClaims(namespace).Create(t.Context(), pvc, metav1.CreateOptions{})
	require.NoError(t, err)
	f := &scaleDownPVCCleanupAPIFixture{ledger: ledger, sts: sts, pvc: pvc, cs: cs}
	if bound {
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvc.Spec.VolumeName},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				PersistentVolumeSource:        corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/test-only"}},
				ClaimRef: &corev1.ObjectReference{
					APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: namespace,
					Name: pvc.Name, UID: pvc.UID, ResourceVersion: pvc.ResourceVersion,
				},
			},
		}
		f.pv, err = cs.CoreV1().PersistentVolumes().Create(t.Context(), pv, metav1.CreateOptions{})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, client.IgnoreNotFound(cs.CoreV1().PersistentVolumes().Delete(ctx, f.pv.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &f.pv.UID}})))
		})
	}
	plan, err := prepareScaleDownPVCPlan(t.Context(), cs, k8sClient, ledger, sts, 1)
	require.NoError(t, err)
	require.Len(t, plan.Claims, 1)
	require.Equal(t, pvc.UID, plan.Claims[0].UID)
	require.Equal(t, pvc.ResourceVersion, plan.Claims[0].InitialResourceVersion)
	if bound {
		require.Equal(t, f.pv.UID, plan.Claims[0].PVUID)
		require.Equal(t, f.pv.Spec.ClaimRef, plan.Claims[0].ClaimRef)
	}
	encoded, err := json.Marshal(plan)
	require.NoError(t, err)
	f.inventory = string(encoded)
	sts.Annotations = map[string]string{annotationScaleDownPVCFrom: "2", annotationScaleDownPVCPlan: f.inventory}
	require.NoError(t, k8sClient.Update(t.Context(), sts))
	require.EqualValues(t, 2, *f.readStatefulSet(t).Spec.Replicas, "inventory must persist before replica reduction")
	f.requirePersistedInventory(t)
	sts.Spec.Replicas = new(int32(1))
	require.NoError(t, k8sClient.Update(t.Context(), sts))
	ledger.Spec.Replicas = new(int32(1))
	return f
}

func (f *scaleDownPVCCleanupAPIFixture) readStatefulSet(t *testing.T) *appsv1.StatefulSet {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(f.sts), sts))
	return sts
}

func (f *scaleDownPVCCleanupAPIFixture) requirePersistedInventory(t *testing.T) {
	t.Helper()
	sts := f.readStatefulSet(t)
	require.Equal(t, "2", sts.Annotations[annotationScaleDownPVCFrom])
	require.Equal(t, f.inventory, sts.Annotations[annotationScaleDownPVCPlan])
	plan, err := readScaleDownPVCPlan(sts, f.ledger)
	require.NoError(t, err)
	require.Len(t, plan.Claims, 1)
	require.Equal(t, f.pvc.UID, plan.Claims[0].UID)
}

func (f *scaleDownPVCCleanupAPIFixture) resume(t *testing.T, cs kubernetes.Interface) error {
	t.Helper()
	direct, err := client.New(testEnv.Config, client.Options{Scheme: k8sClient.Scheme()})
	require.NoError(t, err)
	sts := &appsv1.StatefulSet{}
	require.NoError(t, direct.Get(t.Context(), client.ObjectKeyFromObject(f.sts), sts))
	reconciler := &ClusterReconciler{Client: direct, APIReader: direct, Scheme: direct.Scheme(), Clientset: cs}
	return reconciler.cleanupPendingScaleDownPVCs(t.Context(), f.ledger, sts)
}

func (f *scaleDownPVCCleanupAPIFixture) requireDeletionAccepted(t *testing.T) {
	t.Helper()
	pvc, err := f.cs.CoreV1().PersistentVolumeClaims(f.sts.Namespace).Get(t.Context(), f.pvc.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return
	}
	require.NoError(t, err)
	require.Equal(t, f.pvc.UID, pvc.UID)
	require.NotNil(t, pvc.DeletionTimestamp, "without a protection controller, acceptance is observed as a deletion timestamp")
}

func (f *scaleDownPVCCleanupAPIFixture) replacePVC(t *testing.T) *corev1.PersistentVolumeClaim {
	t.Helper()
	pvcs := f.cs.CoreV1().PersistentVolumeClaims(f.sts.Namespace)
	live, err := pvcs.Get(t.Context(), f.pvc.Name, metav1.GetOptions{})
	require.NoError(t, err)
	// This fixture has no kube-controller-manager. Remove protection only on
	// this disposable, unbound test claim so same-name recreation is real.
	require.Empty(t, live.Spec.VolumeName)
	live.Finalizers = nil
	live, err = pvcs.Update(t.Context(), live, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, pvcs.Delete(t.Context(), live.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &live.UID, ResourceVersion: &live.ResourceVersion}}))
	_, err = pvcs.Get(t.Context(), live.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	replacement, err := pvcs.Create(t.Context(), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: live.Name, Namespace: live.Namespace},
		Spec:       *live.Spec.DeepCopy(),
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	return replacement
}

type scaleDownPVCAPIDeleteClient struct {
	kubernetes.Interface
	beforeDelete func(context.Context, string)
	attempts     []metav1.DeleteOptions
	results      []error
}

func (c *scaleDownPVCAPIDeleteClient) CoreV1() coreclient.CoreV1Interface {
	return &scaleDownPVCAPICore{CoreV1Interface: c.Interface.CoreV1(), parent: c}
}

type scaleDownPVCAPICore struct {
	coreclient.CoreV1Interface
	parent *scaleDownPVCAPIDeleteClient
}

func (c *scaleDownPVCAPICore) PersistentVolumeClaims(namespace string) coreclient.PersistentVolumeClaimInterface {
	return &scaleDownPVCAPIDeletes{PersistentVolumeClaimInterface: c.CoreV1Interface.PersistentVolumeClaims(namespace), parent: c.parent}
}

type scaleDownPVCAPIDeletes struct {
	coreclient.PersistentVolumeClaimInterface
	parent *scaleDownPVCAPIDeleteClient
}

func (c *scaleDownPVCAPIDeletes) Delete(ctx context.Context, name string, options metav1.DeleteOptions) error {
	c.parent.attempts = append(c.parent.attempts, *options.DeepCopy())
	if hook := c.parent.beforeDelete; hook != nil {
		c.parent.beforeDelete = nil
		hook(ctx, name)
	}
	err := c.PersistentVolumeClaimInterface.Delete(ctx, name, options)
	c.parent.results = append(c.parent.results, err)
	return err
}
