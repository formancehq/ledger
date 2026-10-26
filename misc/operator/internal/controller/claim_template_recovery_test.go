package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestAlignedClaimTemplatesPreservesMinimumAndExistingSettings(t *testing.T) {
	t.Parallel()

	existing := []corev1.PersistentVolumeClaim{
		recoveryClaimTemplate("wal", "20Gi"), recoveryClaimTemplate("data", "100Gi"),
	}
	existing[0].Labels = map[string]string{"retain": "label"}
	existing[0].Spec.VolumeAttributesClassName = new("old-profile")
	desired := []corev1.PersistentVolumeClaim{
		recoveryClaimTemplate("data", "150Gi"), recoveryClaimTemplate("wal", "10Gi"),
	}
	desired[1].Spec.VolumeAttributesClassName = new("new-profile")
	beforeExisting, beforeDesired := existing[0].DeepCopy(), desired[1].DeepCopy()

	aligned, changed, err := alignedClaimTemplates(existing, desired)
	require.NoError(t, err)
	require.True(t, changed)
	wantWAL := *existing[0].DeepCopy()
	wantWAL.Spec.VolumeAttributesClassName = new("new-profile")
	wantData := *existing[1].DeepCopy()
	wantData.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("150Gi")
	require.Equal(t, []corev1.PersistentVolumeClaim{wantWAL, wantData}, aligned,
		"match names rather than desired ordering, and change only capacity/profile")
	require.Equal(t, *beforeExisting, existing[0])
	require.Equal(t, *beforeDesired, desired[1])
	aligned[0].Labels["retain"] = "changed"
	aligned[0].Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("999Gi")
	require.Equal(t, *beforeExisting, existing[0], "aligned claims must not alias source claims")

	desired = []corev1.PersistentVolumeClaim{recoveryClaimTemplate("wal", "10Gi")}
	desired[0].Spec.VolumeAttributesClassName = new("old-profile")
	aligned, changed, err = alignedClaimTemplates(existing[:1], desired)
	require.NoError(t, err)
	require.False(t, changed, "a lower configured minimum must not cause repeated recreation")
	require.Equal(t, existing[:1], aligned)
}

func TestAlignedClaimTemplatesPreservesClassAndAccessModeDrift(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		old  func(*corev1.PersistentVolumeClaim)
		edit func(*corev1.PersistentVolumeClaim)
	}{
		{name: "default class made explicit", old: func(p *corev1.PersistentVolumeClaim) { p.Spec.StorageClassName = nil }},
		{name: "explicit class omitted", edit: func(p *corev1.PersistentVolumeClaim) { p.Spec.StorageClassName = nil }},
		{name: "different class", edit: func(p *corev1.PersistentVolumeClaim) { p.Spec.StorageClassName = new("other") }},
		{name: "different access mode", edit: func(p *corev1.PersistentVolumeClaim) {
			p.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			old := recoveryClaimTemplate("data", "100Gi")
			old.Spec.VolumeAttributesClassName = new("old-profile")
			if tt.old != nil {
				tt.old(&old)
			}
			want := recoveryClaimTemplate("data", "150Gi")
			want.Spec.VolumeAttributesClassName = new("new-profile")
			if tt.edit != nil {
				tt.edit(&want)
			}
			beforeOld, beforeWant := old.DeepCopy(), want.DeepCopy()
			aligned, changed, err := alignedClaimTemplates([]corev1.PersistentVolumeClaim{old}, []corev1.PersistentVolumeClaim{want})
			require.NoError(t, err)
			require.True(t, changed, "declaration drift must not block safe floor/profile alignment")
			expected := *old.DeepCopy()
			expected.Spec.Resources.Requests = want.Spec.Resources.Requests.DeepCopy()
			expected.Spec.VolumeAttributesClassName = new("new-profile")
			require.Equal(t, []corev1.PersistentVolumeClaim{expected}, aligned)
			require.Equal(t, *beforeOld, old)
			require.Equal(t, *beforeWant, want)

			want.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("50Gi")
			again, changed, err := alignedClaimTemplates(aligned, []corev1.PersistentVolumeClaim{want})
			require.NoError(t, err)
			require.False(t, changed, "declaration drift and a lower floor must not recreate templates repeatedly")
			require.Equal(t, aligned, again)
		})
	}
}

func TestAlignedClaimTemplatesRejectsUnsupportedProfileAndLayout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		edit func(*corev1.PersistentVolumeClaim)
		want string
	}{
		{"cleared profile", func(p *corev1.PersistentVolumeClaim) { p.Spec.VolumeAttributesClassName = nil }, "clearing volumeAttributesClassName"},
		{"different name", func(p *corev1.PersistentVolumeClaim) { p.Name = "other" }, "absent from desired storage layout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			old := recoveryClaimTemplate("data", "100Gi")
			old.Spec.VolumeAttributesClassName = new("profile")
			want := *old.DeepCopy()
			tt.edit(&want)
			aligned, changed, err := alignedClaimTemplates([]corev1.PersistentVolumeClaim{old}, []corev1.PersistentVolumeClaim{want})
			require.ErrorContains(t, err, tt.want)
			require.Nil(t, aligned)
			require.False(t, changed)
		})
	}
}

// These fake-client tests prove ordering, identity checks and durable retries.
// They explicitly supply replacement identities and adoption/status observations;
// fake deletion is not evidence of Kubernetes garbage collection or Pod adoption.
type claimRecoveryFixture struct {
	scheme *runtime.Scheme
	ledger *ledgerv1alpha1.Cluster
	source *appsv1.StatefulSet
	claims []corev1.PersistentVolumeClaim
	pods   []*corev1.Pod
	pvcs   []*corev1.PersistentVolumeClaim
	pvs    []*corev1.PersistentVolume
	client client.WithWatch
}

func newClaimRecoveryFixture(t *testing.T) *claimRecoveryFixture {
	t.Helper()
	f := &claimRecoveryFixture{scheme: runtime.NewScheme(), ledger: newMinimalCluster()}
	require.NoError(t, clientgoscheme.AddToScheme(f.scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(f.scheme))
	f.ledger.UID = "cluster-uid"
	f.ledger.Generation = 4
	f.ledger.Spec.Replicas = new(int32(3))
	f.ledger.Spec.Image.Tag = "v3.0.0-rc.1"
	applyDefaults(f.ledger)
	f.source = &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceName(f.ledger.Name), Namespace: f.ledger.Namespace,
			UID: "source-sts-uid", ResourceVersion: "11", Generation: 7,
			Labels: map[string]string{"source": "preserved"}, Annotations: map[string]string{"source": "annotation"},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: new(int32(3)), ServiceName: "original-headless",
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "source"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "source"}, Annotations: map[string]string{"restart": "original"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "ledger", Image: "ledger:v3.0.0-rc.1", Env: []corev1.EnvVar{{Name: "TLS_MODE", Value: "disabled"}, {Name: "RAFT_CONFIG", Value: "original"}}}}},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{recoveryClaimTemplate("wal", "10Gi"), recoveryClaimTemplate("data", "100Gi")},
		},
		Status: appsv1.StatefulSetStatus{ObservedGeneration: 7, UpdatedReplicas: 3, ReadyReplicas: 3},
	}
	require.NoError(t, controllerutil.SetControllerReference(f.ledger, f.source, f.scheme))
	f.claims = []corev1.PersistentVolumeClaim{*f.source.Spec.VolumeClaimTemplates[0].DeepCopy(), *f.source.Spec.VolumeClaimTemplates[1].DeepCopy()}
	f.claims[1].Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("150Gi")
	f.claims[1].Spec.VolumeAttributesClassName = new("new-profile")
	objects := []client.Object{f.ledger, f.source}
	for ordinal := range *f.source.Spec.Replicas {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", f.source.Name, ordinal), Namespace: f.source.Namespace, UID: types.UID(fmt.Sprintf("pod-%d", ordinal))}, Spec: *f.source.Spec.Template.Spec.DeepCopy()}
		require.NoError(t, controllerutil.SetControllerReference(f.source, pod, f.scheme))
		for _, template := range f.source.Spec.VolumeClaimTemplates {
			name := template.Name + "-" + pod.Name
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: f.source.Namespace, UID: types.UID("pvc-" + name)}, Spec: *template.Spec.DeepCopy(), Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
			pvc.Spec.VolumeName = "pv-" + name
			pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvc.Spec.VolumeName, UID: types.UID("uid-" + pvc.Spec.VolumeName)}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: pvc.Name, Namespace: pvc.Namespace, UID: pvc.UID}}}
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: template.Name, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: name}}})
			f.pvcs, f.pvs = append(f.pvcs, pvc), append(f.pvs, pv)
			objects = append(objects, pvc, pv)
		}
		f.pods = append(f.pods, pod)
		objects = append(objects, pod)
	}
	f.client = fake.NewClientBuilder().WithScheme(f.scheme).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}, &appsv1.StatefulSet{}, &corev1.PersistentVolumeClaim{}).
		WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			// The fake client does not allocate API-server identities.
			if obj.GetUID() == "" {
				obj.SetUID(types.UID("created-" + obj.GetName()))
				obj.SetGeneration(1)
			}

			return c.Create(ctx, obj, opts...)
		},
	}).Build()

	return f
}

func (f *claimRecoveryFixture) reconciler(c client.Client) *ClusterReconciler {
	return &ClusterReconciler{Client: c, Scheme: f.scheme, APIReader: c}
}

func (f *claimRecoveryFixture) latestLedger(t *testing.T) *ledgerv1alpha1.Cluster {
	t.Helper()
	ledger := &ledgerv1alpha1.Cluster{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.ledger), ledger))

	return ledger
}

func (f *claimRecoveryFixture) journal(t *testing.T) (*corev1.ConfigMap, claimTemplateRecovery) {
	t.Helper()
	cm := &corev1.ConfigMap{}
	pointer := f.latestLedger(t).Status.ClaimTemplateRecovery
	require.NotNil(t, pointer, "journal lookup requires a status-authorized operation")
	require.NoError(t, f.client.Get(t.Context(), types.NamespacedName{Name: pointer.ConfigMapName, Namespace: f.ledger.Namespace}, cm))
	var snapshot claimTemplateRecovery
	require.NoError(t, json.Unmarshal([]byte(cm.Data[claimTemplateRecoveryKey]), &snapshot))

	return cm, snapshot
}

func (f *claimRecoveryFixture) requireStorageUnchanged(t *testing.T) {
	t.Helper()
	for _, original := range f.pvcs {
		actual := &corev1.PersistentVolumeClaim{}
		require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(original), actual))
		require.Equal(t, original.UID, actual.UID)
		require.Equal(t, original.Spec, actual.Spec)
		require.Equal(t, original.Status, actual.Status)
	}
	for _, original := range f.pvs {
		actual := &corev1.PersistentVolume{}
		require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(original), actual))
		require.Equal(t, original.UID, actual.UID)
		require.Equal(t, original.Spec, actual.Spec)
	}
}

func (f *claimRecoveryFixture) replacement(t *testing.T) *appsv1.StatefulSet {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), sts))
	require.NotEmpty(t, sts.UID)
	require.NotEqual(t, f.source.UID, sts.UID)

	return sts
}

func (f *claimRecoveryFixture) adoptPods(t *testing.T, replacement *appsv1.StatefulSet) {
	t.Helper()
	for _, original := range f.pods {
		pod := &corev1.Pod{}
		require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(original), pod))
		pod.OwnerReferences = nil
		require.NoError(t, controllerutil.SetControllerReference(replacement, pod, f.scheme))
		require.NoError(t, f.client.Update(t.Context(), pod))
	}
}

// Check the protected pointer and every digest-derived journal name. Looking
// up the obsolete fixed name could pass while a real recovery exists.
func requireNoClaimTemplateRecovery(t *testing.T, reader client.Reader, ledger *ledgerv1alpha1.Cluster) {
	t.Helper()
	latest := &ledgerv1alpha1.Cluster{}
	require.NoError(t, reader.Get(t.Context(), client.ObjectKeyFromObject(ledger), latest))
	require.Nil(t, latest.Status.ClaimTemplateRecovery)
	requireNoClaimTemplateRecoveryJournals(t, reader, ledger)
}

func requireNoClaimTemplateRecoveryJournals(t *testing.T, reader client.Reader, ledger *ledgerv1alpha1.Cluster) {
	t.Helper()
	journals := &corev1.ConfigMapList{}
	require.NoError(t, reader.List(t.Context(), journals, client.InNamespace(ledger.Namespace)))
	for _, journal := range journals.Items {
		require.False(t, strings.HasPrefix(journal.Name, resourceName(ledger.Name)+"-storage-recovery-"), "unexpected recovery journal %s", journal.Name)
	}
}

func recoveryClaimTemplate(name, size string) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: new("fast"),
		Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)}},
	}}
}

func TestClaimTemplateRecoverySnapshotFailurePreventsDeleteAndRetries(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	attempts, deletes := 0, 0
	failure := errors.New("snapshot write unavailable")
	c := interceptor.NewClient(f.client, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*corev1.ConfigMap); ok {
				attempts++
				if attempts == 1 {
					return failure
				}
			}

			return c.Create(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			deletes++

			return c.Delete(ctx, obj, opts...)
		},
	})
	r := f.reconciler(c)
	_, err := r.startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.ErrorIs(t, err, failure)
	require.ErrorContains(t, err, "persisting storage recovery state")
	require.Equal(t, "PreparationFailed", meta.FindStatusCondition(f.ledger.Status.Conditions, "ClaimTemplatesReady").Reason)
	require.Zero(t, deletes)
	preparing := f.latestLedger(t).Status.ClaimTemplateRecovery
	require.NotNil(t, preparing)
	require.Empty(t, preparing.ConfigMapUID)
	actual := &appsv1.StatefulSet{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), actual))
	require.Equal(t, f.source.Spec, actual.Spec)
	handled, result, err := r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, claimTemplateRequeueInterval, result.RequeueAfter)
	require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
	require.Equal(t, 1, attempts, "abandonment must not retry journal creation in the same pass")
	_, err = r.startClaimTemplateRecovery(t.Context(), f.latestLedger(t), f.source, f.claims)
	wait, ok := errors.AsType[*claimTemplateRecoveryRetryWait](err)
	require.True(t, ok)
	require.Positive(t, wait.after)
	require.Equal(t, 1, attempts, "watches cannot bypass the retry gate")
	requireNoClaimTemplateRecovery(t, f.client, f.ledger)
	// Restart resets only the rate limiter, never durable recovery authority.
	_, err = f.reconciler(c).startClaimTemplateRecovery(t.Context(), f.latestLedger(t), f.source, f.claims)
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.Equal(t, 1, deletes)
	f.journal(t)
	f.requireStorageUnchanged(t)
}

func TestClaimTemplateRecoveryStartPreservesResumeFailure(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"orphan deletion", "replacement creation"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			failure := errors.New("initial recovery operation failed once")
			attempts := 0
			c := interceptor.NewClient(f.client, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if phase == "replacement creation" {
						switch obj.(type) {
						case *corev1.ConfigMap:
							// Simulate the source disappearing after preparation, before
							// the initial resume must restore it from the bound journal.
							if err := c.Delete(ctx, f.source); err != nil {
								return err
							}
						case *appsv1.StatefulSet:
							if len((&client.CreateOptions{}).ApplyOptions(opts).DryRun) == 0 {
								attempts++
								if attempts == 1 {
									return failure
								}
							}
						}
					}

					return c.Create(ctx, obj, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if _, ok := obj.(*appsv1.StatefulSet); ok && phase == "orphan deletion" {
						attempts++
						if attempts == 1 {
							return failure
						}
					}

					return c.Delete(ctx, obj, opts...)
				},
			})
			r := f.reconciler(c)
			_, err := r.startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
			require.ErrorIs(t, err, failure)
			_, fenced := errors.AsType[*claimTemplateJournalWriteError](err)
			require.True(t, fenced, "handoff must preserve the durable-write continuation fence")
			require.Equal(t, "RecoveryFailed", meta.FindStatusCondition(f.ledger.Status.Conditions, "ClaimTemplatesReady").Reason)
			require.Equal(t, 1, attempts)
			journal, snapshot := f.journal(t)
			pointer := f.latestLedger(t).Status.ClaimTemplateRecovery
			require.Equal(t, string(journal.UID), pointer.ConfigMapUID)
			require.False(t, pointer.Completed)
			require.Equal(t, claimTemplateRetryInitial, r.claimTemplateRetries.entries[client.ObjectKeyFromObject(f.ledger)].delay,
				"the initial recovery failure must be recorded only once")
			f.requireStorageUnchanged(t)

			// A fresh reconciler resumes the same active operation and succeeds.
			ledger := f.latestLedger(t)
			handled, result, err := f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), ledger)
			require.NoError(t, err)
			require.True(t, handled)
			require.Equal(t, claimTemplateRequeueInterval, result.RequeueAfter)
			require.Equal(t, 2, attempts)
			after, _ := f.journal(t)
			require.Equal(t, journal.UID, after.UID)
			require.Equal(t, journal.Data, after.Data)
			require.Equal(t, pointer, f.latestLedger(t).Status.ClaimTemplateRecovery)
			if phase == "replacement creation" {
				require.Equal(t, "AwaitingAdoption", meta.FindStatusCondition(ledger.Status.Conditions, "ClaimTemplatesReady").Reason)
				require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec)
			} else {
				require.Equal(t, "Recreating", meta.FindStatusCondition(ledger.Status.Conditions, "ClaimTemplatesReady").Reason)
				require.True(t, apierrors.IsNotFound(f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &appsv1.StatefulSet{})))
			}
			f.requireStorageUnchanged(t)
		})
	}
}

func TestClaimTemplateRecoveryPersistsJournalAndMarkerBeforeOrphanDelete(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	deletes := 0
	c := interceptor.NewClient(f.client, interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		deletes++
		cm, snapshot := f.journal(t)
		require.True(t, metav1.IsControlledBy(cm, f.ledger))
		require.Equal(t, new(true), cm.Immutable)
		require.Equal(t, 1, snapshot.Version)
		require.Equal(t, f.source.UID, snapshot.SourceUID)
		require.Equal(t, f.source.Generation, snapshot.SourceGeneration)
		want := *f.source.Spec.DeepCopy()
		want.VolumeClaimTemplates = f.claims
		require.Equal(t, want, snapshot.StatefulSet.Spec)
		require.Empty(t, snapshot.StatefulSet.UID)
		require.Empty(t, snapshot.StatefulSet.ResourceVersion)
		require.Equal(t, string(f.source.UID), snapshot.StatefulSet.Annotations[claimTemplateRecoveryAnnotation])
		require.Len(t, snapshot.Claims, len(f.pvcs))
		for i, pvc := range f.pvcs {
			require.Equal(t, retainedClaimIdentity{Name: pvc.Name, UID: pvc.UID, VolumeName: pvc.Spec.VolumeName, PVUID: f.pvs[i].UID}, snapshot.Claims[i])
		}
		marker := f.latestLedger(t).Status.ClaimTemplateRecovery
		require.Equal(t, &ledgerv1alpha1.ClaimTemplateRecoveryStatus{ConfigMapName: cm.Name, ConfigMapUID: string(cm.UID), SourceUID: string(f.source.UID), PayloadSHA256: claimTemplateRecoveryDigest(f.ledger, []byte(cm.Data[claimTemplateRecoveryKey]))}, marker)
		options := (&client.DeleteOptions{}).ApplyOptions(opts)
		require.Equal(t, new(metav1.DeletePropagationOrphan), options.PropagationPolicy)
		require.Equal(t, &metav1.Preconditions{UID: &f.source.UID, ResourceVersion: &f.source.ResourceVersion}, options.Preconditions)

		return c.Delete(ctx, obj, opts...)
	}})
	_, err := f.reconciler(c).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.NoError(t, err)
	require.Equal(t, 1, deletes)
	f.requireStorageUnchanged(t)
}

func TestClaimTemplateRecoveryMarkerFailurePreventsDelete(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"Preparing", "Active"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			attempts, deletes := 0, 0
			failed := false
			failure := errors.New("status write unavailable")
			c := interceptor.NewClient(f.client, interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, c client.Client, subresource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					attempts++
					pointer := obj.(*ledgerv1alpha1.Cluster).Status.ClaimTemplateRecovery
					if !failed && ((phase == "Preparing" && pointer.ConfigMapUID == "") || (phase == "Active" && pointer.ConfigMapUID != "")) {
						failed = true

						return failure
					}

					return c.SubResource(subresource).Patch(ctx, obj, patch, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					deletes++

					return c.Delete(ctx, obj, opts...)
				},
			})
			_, err := f.reconciler(c).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
			require.ErrorIs(t, err, failure)
			require.ErrorContains(t, err, "persisting claim-template recovery pointer")
			require.Zero(t, deletes)
			if phase == "Preparing" {
				require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
				journals := &corev1.ConfigMapList{}
				require.NoError(t, f.client.List(t.Context(), journals))
				require.Empty(t, journals.Items, "journal creation requires durable Preparing authorization")
				_, err = f.reconciler(c).startClaimTemplateRecovery(t.Context(), f.latestLedger(t), f.source, f.claims)
			} else {
				cm, _ := f.journal(t)
				require.Empty(t, f.latestLedger(t).Status.ClaimTemplateRecovery.ConfigMapUID)
				handled, _, resumeErr := f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
				err = resumeErr
				require.True(t, handled)
				after, _ := f.journal(t)
				require.Equal(t, cm.Data, after.Data)
			}
			require.NoError(t, err)
			require.Equal(t, 3, attempts)
			require.Equal(t, 1, deletes)
		})
	}
}

func TestClaimTemplateRecoveryRejectsPrunedStatusPointer(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"Preparing", "Active"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			deletes := 0
			c := interceptor.NewClient(f.client, interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, inner client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					pointer := obj.(*ledgerv1alpha1.Cluster).Status.ClaimTemplateRecovery
					if (phase == "Preparing" && pointer.ConfigMapUID == "") || (phase == "Active" && pointer.ConfigMapUID != "") {
						// An older CRD can accept a write while pruning the pointer.
						obj.(*ledgerv1alpha1.Cluster).Status.ClaimTemplateRecovery = nil

						return nil
					}

					return inner.SubResource(sub).Patch(ctx, obj, patch, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					deletes++

					return c.Delete(ctx, obj, opts...)
				},
			})
			_, err := f.reconciler(c).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
			require.ErrorContains(t, err, "install the updated Cluster CRD")
			require.Zero(t, deletes)
			if phase == "Preparing" {
				require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
				journals := &corev1.ConfigMapList{}
				require.NoError(t, f.client.List(t.Context(), journals))
				require.Empty(t, journals.Items)
			} else {
				require.Empty(t, f.latestLedger(t).Status.ClaimTemplateRecovery.ConfigMapUID)
				f.journal(t)
			}
		})
	}
}

func TestClaimTemplateRecoveryRejectsPrunedVolumeAttributesClass(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	deletes := 0
	c := interceptor.NewClient(f.client, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			options := (&client.CreateOptions{}).ApplyOptions(opts)
			if preview, ok := obj.(*appsv1.StatefulSet); ok && len(options.DryRun) > 0 {
				preview.Spec.VolumeClaimTemplates[1].Spec.VolumeAttributesClassName = nil

				return nil
			}

			return c.Create(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			deletes++

			return c.Delete(ctx, obj, opts...)
		},
	})
	_, err := f.reconciler(c).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.ErrorContains(t, err, "did not preserve the requested replacement StatefulSet spec")
	require.Zero(t, deletes)
	requireNoClaimTemplateRecovery(t, f.client, f.ledger)
	f.requireStorageUnchanged(t)
}

func TestClaimTemplateRecoveryRejectsAdmissionWorkloadMutation(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	c := interceptor.NewClient(f.client, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			options := (&client.CreateOptions{}).ApplyOptions(opts)
			if preview, ok := obj.(*appsv1.StatefulSet); ok && len(options.DryRun) > 0 {
				preview.Spec.Template.Spec.Containers[0].Image = "admission-mutated-image"

				return nil
			}

			return c.Create(ctx, obj, opts...)
		},
	})
	_, err := f.reconciler(c).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.ErrorContains(t, err, "requested replacement StatefulSet spec")
	require.ErrorContains(t, err, "mutating admission webhooks")
	requireNoClaimTemplateRecovery(t, f.client, f.ledger)
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &appsv1.StatefulSet{}))
	f.requireStorageUnchanged(t)
}

func TestClaimTemplateRecoveryUnreadySourceReplansAndRepairs(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	f.ledger.Spec.Persistence.WAL.StorageClass = "fast"
	f.ledger.Spec.Persistence.WAL.Size = resource.MustParse("10Gi")
	f.ledger.Spec.Persistence.Data.StorageClass = "fast"
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
	f.source.Spec = buildStatefulSetSpec(f.ledger, computeSpecHash(&f.ledger.Spec), nil, tlsModeDisabled)
	recordPodConfigHash(f.source, computeSpecHash(&f.ledger.Spec))
	f.source.CreationTimestamp = metav1.Now()
	require.NoError(t, f.client.Update(t.Context(), f.source))
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
	f.ledger.Spec.Persistence.Data.VolumeAttributesClassName = "new-profile"
	f.claims = buildVolumeClaimTemplates(f.ledger)
	require.NoError(t, f.client.Update(t.Context(), f.ledger))

	deletes, completions, claimReads := 0, 0, 0
	blockObsoleteClaims := false
	conflict := apierrors.NewConflict(appsv1.Resource("statefulsets"), f.source.Name, errors.New("source resourceVersion changed"))
	completionFailure := errors.New("completed marker temporarily unavailable")
	c := interceptor.NewClient(f.client, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); ok {
				deletes++
				if deletes == 1 {
					return conflict
				}
			}

			return c.Delete(ctx, obj, opts...)
		},
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.PersistentVolumeClaim); ok && blockObsoleteClaims {
				claimReads++

				return errors.New("obsolete retained claim unavailable")
			}

			return c.Get(ctx, key, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, subresource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if cluster, ok := obj.(*ledgerv1alpha1.Cluster); ok && cluster.Status.ClaimTemplateRecovery != nil && cluster.Status.ClaimTemplateRecovery.Completed {
				completions++
				if completions == 1 {
					return completionFailure
				}
			}

			return c.SubResource(subresource).Patch(ctx, obj, patch, opts...)
		},
	})
	_, err := f.reconciler(c).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.ErrorIs(t, err, conflict)
	journal, _ := f.journal(t)
	require.False(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
	source := &appsv1.StatefulSet{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), source))
	source.Status.ReadyReplicas--
	require.NoError(t, f.client.Status().Update(t.Context(), source))
	require.Equal(t, f.source.Generation, source.Generation, "readiness loss must not rely on the existing generation replan")
	require.True(t, source.DeletionTimestamp.IsZero())
	changed := f.latestLedger(t)
	changed.Spec.PodAnnotations = map[string]string{"repair": "new-workload-config"}
	require.NoError(t, f.client.Update(t.Context(), changed))
	blockObsoleteClaims = true

	// A restart must complete the stale journal durably before cleaning it up.
	handled, _, err := f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.True(t, handled)
	require.ErrorIs(t, err, completionFailure)
	require.False(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
	ledger := f.latestLedger(t)
	handled, result, err := f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), ledger)
	require.NoError(t, err)
	require.True(t, handled)
	require.Positive(t, result.RequeueAfter)
	require.Equal(t, "SourceChanged", meta.FindStatusCondition(ledger.Status.Conditions, "ClaimTemplatesReady").Reason)
	require.True(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
	require.Equal(t, 2, completions)
	require.Zero(t, claimReads, "an unready, nondeleting source invalidates the plan before obsolete claim reads")
	require.Equal(t, 1, deletes, "do not orphan the unready source")
	stored, _ := f.journal(t)
	require.Equal(t, journal.Data, stored.Data)
	_, _, err = f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.NoError(t, err)
	require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
	require.True(t, apierrors.IsNotFound(f.client.Get(t.Context(), client.ObjectKeyFromObject(journal), &corev1.ConfigMap{})))
	blockObsoleteClaims = false

	// Normal reconciliation can now apply the correction. Storage recovery
	// starts again only after the repaired workload has actually converged.
	ledger = f.latestLedger(t)
	_, err = f.reconciler(c).reconcileStatefulSet(t.Context(), ledger, computeSpecHash(&ledger.Spec), nil)
	require.NoError(t, err)
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), source))
	require.Equal(t, f.source.UID, source.UID)
	require.Equal(t, "new-workload-config", source.Spec.Template.Annotations["repair"])
	require.Equal(t, f.source.Spec.VolumeClaimTemplates, source.Spec.VolumeClaimTemplates)
	require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
	require.Equal(t, 1, deletes)
	source.Status.ReadyReplicas = *source.Spec.Replicas
	require.NoError(t, f.client.Status().Update(t.Context(), source))
	_, err = f.reconciler(c).reconcileStatefulSet(t.Context(), ledger, computeSpecHash(&ledger.Spec), nil)
	require.NoError(t, err)
	require.Equal(t, 2, deletes, "a fresh plan must retry deletion successfully after repair")
	_, replanned := f.journal(t)
	require.Equal(t, source.Spec.Template, replanned.StatefulSet.Spec.Template)
	require.Equal(t, f.claims, replanned.StatefulSet.Spec.VolumeClaimTemplates)
	f.requireStorageUnchanged(t)
}

func TestClaimTemplateRecoveryUnreadyWorkloadAfterDeletionKeepsSnapshot(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"deleting source", "unobserved replacement"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			if state == "deleting source" {
				f.source.Finalizers = []string{"test.ledger.formance.com/hold-deletion"}
				require.NoError(t, f.client.Update(t.Context(), f.source))
			}
			r := f.reconciler(f.client)
			_, err := r.startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
			require.NoError(t, err)
			journal, snapshot := f.journal(t)
			if state == "unobserved replacement" {
				_, _, err = r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
				require.NoError(t, err)
			}
			live := &appsv1.StatefulSet{}
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), live))
			live.Status.ReadyReplicas = 0
			require.NoError(t, f.client.Status().Update(t.Context(), live))
			changed := f.latestLedger(t)
			changed.Spec.Replicas = new(int32(2)) // Invalid desired spec must not replace the journal.
			changed.Spec.Image.Tag = "v3.0.0-rc.9"
			changed.Spec.Persistence.Data.Size = resource.MustParse("999Gi")
			require.NoError(t, f.client.Update(t.Context(), changed))
			require.Error(t, validateSpec(changed))
			for range 2 {
				result, err := f.reconciler(f.client).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(changed)})
				require.NoError(t, err)
				require.Positive(t, result.RequeueAfter)
				require.False(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
				stored, _ := f.journal(t)
				require.Equal(t, journal.Data, stored.Data)
			}
			after := &appsv1.StatefulSet{}
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(live), after))
			require.Equal(t, live.UID, after.UID)
			require.Equal(t, live.Spec, after.Spec)
			if state == "deleting source" {
				require.False(t, after.DeletionTimestamp.IsZero())
			} else {
				require.NotEqual(t, snapshot.SourceUID, after.UID)
				require.Equal(t, snapshot.StatefulSet.Spec, after.Spec)
			}
			f.requireStorageUnchanged(t)
		})
	}
}

func TestClaimTemplateRecoveryReplansChangedSourceBeforeMissingClaims(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	f.source.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{WhenScaled: appsv1.DeletePersistentVolumeClaimRetentionPolicyType}
	require.NoError(t, f.client.Update(t.Context(), f.source))
	failure := errors.New("delete rejected before orphaning")
	c := interceptor.NewClient(f.client, interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if _, ok := obj.(*appsv1.StatefulSet); ok {
			return failure
		}

		return c.Delete(ctx, obj, opts...)
	}})
	_, err := f.reconciler(c).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.ErrorIs(t, err, failure)
	_, snapshot := f.journal(t)
	// A direct scale-down happens while the original STS still exists. Its
	// removed claims can be gone before the next storage reconciliation.
	f.source.Spec.Replicas = new(int32(1))
	f.source.Generation++
	require.NoError(t, f.client.Update(t.Context(), f.source))
	require.NoError(t, f.client.Delete(t.Context(), f.pvcs[len(f.pvcs)-1]))
	require.Equal(t, int32(3), *snapshot.StatefulSet.Spec.Replicas)
	handled, _, err := f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.NoError(t, err)
	require.True(t, handled)
	require.True(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
	_, _, err = f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.NoError(t, err)
	require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
	actual := &appsv1.StatefulSet{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), actual))
	require.Equal(t, f.source.UID, actual.UID)
	require.Equal(t, int32(1), *actual.Spec.Replicas)
}

func TestClaimTemplateRecoveryPreservesReleasedHashes(t *testing.T) {
	t.Parallel()
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("staleTemplates=%t", stale), func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
			// WAL size is omitted: its legacy hash input is zero while its
			// persisted claim template uses the 5Gi default.
			f.ledger.Spec.Persistence.WAL.Size = resource.Quantity{}
			prior := f.ledger.Spec.DeepCopy()
			if stale {
				prior.Persistence.Data.Size = resource.MustParse("150Gi")
			}
			oldHash := computePodSpecHash(prior, false)
			f.source.Spec = buildStatefulSetSpec(f.ledger, oldHash, nil, tlsModeDisabled)
			require.NoError(t, f.client.Update(t.Context(), f.source))
			originalTemplate := f.source.Spec.Template.DeepCopy()
			if stale {
				installLegacySpecHashProof(t, f.ledger, f.source, prior)
			}
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
			require.NoError(t, f.client.Update(t.Context(), f.ledger))
			_, err := f.reconciler(f.client).reconcileStatefulSet(t.Context(), f.ledger, computeSpecHash(&f.ledger.Spec), nil)
			require.NoError(t, err)
			_, snapshot := f.journal(t)
			require.Equal(t, *originalTemplate, snapshot.StatefulSet.Spec.Template, "storage edit must reach journalization without a Pod rollout")
			for _, pod := range f.pods {
				actual := &corev1.Pod{}
				require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(pod), actual))
				require.Equal(t, pod.UID, actual.UID)
			}
		})
	}
}

func TestClaimTemplateRecoveryPreservesReleasedHashDuringScaleDown(t *testing.T) {
	t.Parallel()
	for _, edit := range []string{"replicas", "capacity", "profile"} {
		t.Run(edit, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			// Reachable RC1 drift: the spec/hash advanced to 150Gi while its
			// claim template remained at 100Gi. WAL size is omitted.
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
			f.ledger.Spec.Persistence.Data.StorageClass = "fast"
			f.ledger.Spec.Persistence.WAL.StorageClass = "fast"
			oldHash := computePodSpecHash(&f.ledger.Spec, false)
			prior := f.ledger.Spec.DeepCopy()
			source := f.ledger.DeepCopy()
			source.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
			f.source.Spec = buildStatefulSetSpec(source, oldHash, nil, tlsModeDisabled)
			f.source.CreationTimestamp = metav1.Now()
			require.NoError(t, f.client.Update(t.Context(), f.source))
			originalTemplate := f.source.Spec.Template.DeepCopy()
			if edit != "replicas" {
				installLegacySpecHashProof(t, f.ledger, f.source, prior)
			}
			f.ledger.Spec.Replicas = new(int32(1))
			if edit == "capacity" {
				f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
			}
			if edit == "profile" {
				f.ledger.Spec.Persistence.Data.VolumeAttributesClassName = "new-profile"
			}
			require.NoError(t, f.client.Update(t.Context(), f.ledger))
			objects := make([]runtime.Object, 0, len(f.pods))
			for _, pod := range f.pods {
				objects = append(objects, pod.DeepCopy())
			}
			r := f.reconciler(f.client)
			r.Config = &rest.Config{}
			r.Clientset = kubefake.NewClientset(objects...)
			members := map[int]bool{1: true, 2: true, 3: true}
			failed := false
			responseLost := errors.New("leadership transfer failed once")
			var removals []int
			exec := func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, command []string) (*execResult, error) {
				live := &appsv1.StatefulSet{}
				require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), live))
				require.Equal(t, *originalTemplate, live.Spec.Template, "the preparatory write must not roll retained Pods during Raft removal")
				require.Equal(t, originalTemplate.Annotations[annotationSpecHash], live.Annotations[annotationPodConfigSpecHash])
				require.Equal(t, computeSpecHash(&f.ledger.Spec), live.Annotations[annotationPodConfigHash])
				cmd := command[2]
				switch {
				case strings.Contains(cmd, "'transfer-leader'"):
					if !failed {
						failed = true

						return nil, responseLost
					}

					return &execResult{}, nil
				case strings.Contains(cmd, "'status'"):
					var nodes []string
					for id := 1; id <= 3; id++ {
						if members[id] {
							nodes = append(nodes, fmt.Sprintf(`{"id":%d}`, id))
						}
					}

					return &execResult{Stdout: `{"state":"Leader","nodes":[` + strings.Join(nodes, ",") + `]}`}, nil
				case strings.Contains(cmd, "'remove-node'"):
					id := 2
					if strings.Contains(cmd, "'remove-node' '3'") {
						id = 3
					}
					delete(members, id)
					removals = append(removals, id)

					return &execResult{}, nil
				default:
					t.Fatalf("unexpected command %s", cmd)

					return nil, errors.New("unexpected command")
				}
			}
			hash := computeSpecHash(&f.ledger.Spec)
			_, err := r.reconcileStatefulSetWithExec(t.Context(), f.ledger, hash, nil, exec)
			require.ErrorIs(t, err, responseLost)
			require.Empty(t, removals)
			_, err = r.reconcileStatefulSetWithExec(t.Context(), f.ledger, hash, nil, exec)
			require.NoError(t, err)
			require.Equal(t, []int{3, 2}, removals)
			require.Equal(t, map[int]bool{1: true}, members)
			live := &appsv1.StatefulSet{}
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), live))
			require.Equal(t, int32(1), *live.Spec.Replicas)
			require.Equal(t, *originalTemplate, live.Spec.Template)
		})
	}
}

func TestClaimTemplateRecoveryScaleDownProofSurvivesPreparatoryWrite(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	// The released hash advanced to 150Gi, while the immutable template stayed
	// at 100Gi. This history requires proof once admission inputs also change.
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
	prior := f.ledger.Spec.DeepCopy()
	oldHash := computePodSpecHash(prior, false)
	source := f.ledger.DeepCopy()
	source.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
	f.source.Spec = buildStatefulSetSpec(source, oldHash, nil, tlsModeDisabled)
	f.source.CreationTimestamp = metav1.Now()
	require.Empty(t, f.source.Annotations[annotationPodConfigHash])
	require.NoError(t, f.client.Update(t.Context(), f.source))
	installLegacySpecHashProof(t, f.ledger, f.source, prior)
	proof := f.ledger.Annotations[annotationLegacySpecHashProof]
	f.ledger.Spec.ServiceAccount.Annotations = map[string]string{"eks.amazonaws.com/role-arn": "arn:aws:iam::111111111111:role/replacement"}
	f.ledger.Spec.Replicas = new(int32(1))
	require.NoError(t, f.client.Update(t.Context(), f.ledger))
	hash := computeSpecHash(&f.ledger.Spec)
	require.NotEqual(t, oldHash, hash)
	objects := make([]runtime.Object, 0, len(f.pods)+len(f.pvcs)+len(f.pvs))
	for _, pod := range f.pods {
		objects = append(objects, pod.DeepCopy())
	}
	for _, pvc := range f.pvcs {
		objects = append(objects, pvc.DeepCopy())
	}
	for _, pv := range f.pvs {
		objects = append(objects, pv.DeepCopy())
	}
	clientset := kubefake.NewClientset(objects...)
	members := map[int]bool{1: true, 2: true, 3: true}
	transferCalls := 0
	var removals []int
	transient := errors.New("leadership transfer failed during rollout")
	exec := func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, command []string) (*execResult, error) {
		live := &appsv1.StatefulSet{}
		require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), live))
		require.Equal(t, hash, live.Spec.Template.Annotations[annotationSpecHash], "validate the historical proof before recording canonical metadata")
		require.Equal(t, live.Spec.Template.Annotations[annotationSpecHash], live.Annotations[annotationPodConfigSpecHash])
		require.Equal(t, hash, live.Annotations[annotationPodConfigHash], "persist canonical metadata atomically with the template before external Raft effects")
		require.Equal(t, "annotation", live.Annotations["source"], "preserve unrelated StatefulSet annotations")
		require.EqualValues(t, 3, *live.Spec.Replicas, "membership must change before replica count")
		cmd := command[2]
		switch {
		case strings.Contains(cmd, "'transfer-leader'"):
			transferCalls++
			if transferCalls == 1 {
				return nil, transient
			}

			return &execResult{}, nil
		case strings.Contains(cmd, "'status'"):
			var nodes []string
			for id := 1; id <= 3; id++ {
				if members[id] {
					nodes = append(nodes, fmt.Sprintf(`{"id":%d}`, id))
				}
			}

			return &execResult{Stdout: `{"state":"Leader","nodes":[` + strings.Join(nodes, ",") + `]}`}, nil
		case strings.Contains(cmd, "'remove-node'"):
			id := 2
			if strings.Contains(cmd, "'remove-node' '3'") {
				id = 3
			}
			require.True(t, members[id], "remove each member once")
			delete(members, id)
			removals = append(removals, id)

			return &execResult{}, nil
		default:
			t.Fatalf("unexpected command %s", cmd)

			return nil, errors.New("unexpected command")
		}
	}
	writeFailure := errors.New("preparatory StatefulSet update rejected once")
	writeAttempts := 0
	wrapped := interceptor.NewClient(f.client, interceptor.Funcs{
		Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			options := (&client.UpdateOptions{}).ApplyOptions(opts)
			if _, ok := obj.(*appsv1.StatefulSet); ok && len(options.DryRun) == 0 {
				writeAttempts++
				if writeAttempts == 1 {
					return writeFailure
				}
			}

			return inner.Update(ctx, obj, opts...)
		},
	})
	r := f.reconciler(wrapped)
	r.Config, r.Clientset = &rest.Config{}, clientset
	_, err := r.reconcileStatefulSetWithExec(t.Context(), f.ledger, hash, nil, exec)
	require.ErrorIs(t, err, writeFailure)
	require.Zero(t, transferCalls, "a failed preparatory write must prevent every Raft effect")
	var unchanged appsv1.StatefulSet
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &unchanged))
	require.Equal(t, f.source.Spec, unchanged.Spec)
	require.Empty(t, unchanged.Annotations[annotationPodConfigHash])
	require.Empty(t, unchanged.Annotations[annotationPodConfigSpecHash])
	_, err = r.reconcileStatefulSetWithExec(t.Context(), f.ledger, hash, nil, exec)
	require.ErrorIs(t, err, transient)
	require.Equal(t, 2, writeAttempts, "the next attempt must commit the complete pair before Raft exec")
	require.Equal(t, 1, transferCalls)
	require.Empty(t, removals)
	var live appsv1.StatefulSet
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &live))
	require.Equal(t, live.Spec.Template.Annotations[annotationSpecHash], live.Annotations[annotationPodConfigSpecHash])
	require.Equal(t, hash, live.Annotations[annotationPodConfigHash])
	require.Equal(t, hash, live.Spec.Template.Annotations[annotationSpecHash])
	require.EqualValues(t, 3, *live.Spec.Replicas)
	require.Equal(t, f.source.UID, live.UID)
	require.Equal(t, f.source.Spec.VolumeClaimTemplates, live.Spec.VolumeClaimTemplates)
	f.requireStorageUnchanged(t)
	// A fresh controller reads durable source state. The still-installed proof
	// now names the old Pod hash and must be ignored after canonical migration.
	latest := f.latestLedger(t)
	require.Equal(t, proof, latest.Annotations[annotationLegacySpecHashProof])
	restarted := f.reconciler(f.client)
	restarted.Config, restarted.Clientset = &rest.Config{}, clientset
	_, err = restarted.reconcileStatefulSetWithExec(t.Context(), latest, hash, nil, exec)
	require.NoError(t, err)
	require.Equal(t, 2, transferCalls)
	require.Equal(t, []int{3, 2}, removals)
	require.Equal(t, map[int]bool{1: true}, members)
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &live))
	require.EqualValues(t, 1, *live.Spec.Replicas)
	require.Equal(t, f.source.UID, live.UID)
	require.Equal(t, live.Spec.Template.Annotations[annotationSpecHash], live.Annotations[annotationPodConfigSpecHash])
	require.Equal(t, hash, live.Annotations[annotationPodConfigHash])
	require.Equal(t, hash, live.Spec.Template.Annotations[annotationSpecHash])
	require.Equal(t, f.source.Spec.VolumeClaimTemplates, live.Spec.VolumeClaimTemplates)
	for _, pvc := range f.pvcs {
		if strings.HasSuffix(pvc.Name, "-0") {
			retained, getErr := clientset.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(t.Context(), pvc.Name, metav1.GetOptions{})
			require.NoError(t, getErr)
			require.Equal(t, pvc.UID, retained.UID)
			require.Equal(t, pvc.Spec, retained.Spec)
		}
	}
	for _, pv := range f.pvs {
		retained, getErr := clientset.CoreV1().PersistentVolumes().Get(t.Context(), pv.Name, metav1.GetOptions{})
		require.NoError(t, getErr)
		require.Equal(t, pv.UID, retained.UID)
		require.Equal(t, pv.Spec, retained.Spec)
	}
}

func TestClaimTemplateRecoveryLostDeleteResponseRestoresSnapshotBeforeValidation(t *testing.T) {
	t.Parallel()
	for _, invalidSpec := range []bool{true, false} {
		t.Run(fmt.Sprintf("invalidSpec=%t", invalidSpec), func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			deletes := 0
			lostResponse := errors.New("orphan delete response lost")
			c := interceptor.NewClient(f.client, interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*appsv1.StatefulSet); ok {
					deletes++
					require.NoError(t, c.Delete(ctx, obj, opts...))

					return lostResponse
				}

				return c.Delete(ctx, obj, opts...)
			}})
			_, err := f.reconciler(c).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
			require.ErrorIs(t, err, lostResponse)
			require.Equal(t, 1, deletes)
			cm, snapshot := f.journal(t)
			changed := f.latestLedger(t)
			changed.Spec.Image.Tag = "v3.0.0-rc.9"
			changed.Spec.Replicas = new(int32(5))
			changed.Spec.Persistence.Data.Size = resource.MustParse("999Gi")
			changed.Spec.TLS = &ledgerv1alpha1.TLSConfig{Enabled: true}
			if invalidSpec {
				changed.Spec.Replicas = new(int32(2))
			}
			require.NoError(t, f.client.Update(t.Context(), changed))
			if invalidSpec {
				require.Error(t, validateSpec(changed))
			} else {
				require.NoError(t, validateSpec(changed))
			}
			require.ErrorContains(t, validateRCUpgrade(changed.Spec.Image.Tag, f.source), "immediately next RC")

			// A new reconciler and a newly fetched Cluster have no process-local plan.
			r := f.reconciler(f.client)
			result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(changed)})
			require.NoError(t, err)
			require.Positive(t, result.RequeueAfter)
			replacement := f.replacement(t)
			require.Equal(t, snapshot.StatefulSet.Spec, replacement.Spec)
			require.Equal(t, f.source.Spec.Template, replacement.Spec.Template)
			require.Equal(t, f.source.Spec.Replicas, replacement.Spec.Replicas)
			after, _ := f.journal(t)
			require.Equal(t, cm.Data, after.Data, "later desired edits must not replace the durable snapshot")
			require.Nil(t, meta.FindStatusCondition(f.latestLedger(t).Status.Conditions, "ConfigValid"))
			f.requireStorageUnchanged(t)
		})
	}
}

func TestClaimTemplateRecoveryCreateFailureRetriesExactSnapshot(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	_, err := f.reconciler(f.client).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.NoError(t, err)
	cm, snapshot := f.journal(t)
	attempts := 0
	failure := errors.New("replacement create unavailable")
	c := interceptor.NewClient(f.client, interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if sts, ok := obj.(*appsv1.StatefulSet); ok {
			attempts++
			require.Equal(t, snapshot.StatefulSet.Spec, sts.Spec)
			if attempts == 1 {
				return failure
			}
		}

		return c.Create(ctx, obj, opts...)
	}})
	handled, _, err := f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.True(t, handled)
	require.ErrorIs(t, err, failure)
	require.ErrorContains(t, err, "restoring StatefulSet from storage recovery state")
	_, _, err = f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec)
	after, _ := f.journal(t)
	require.Equal(t, cm.Data, after.Data)
	f.requireStorageUnchanged(t)
}

func TestClaimTemplateRecoveryDependencyFailureDoesNotBlockRestoration(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	_, err := f.reconciler(f.client).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.NoError(t, err)
	journal, snapshot := f.journal(t)
	clusterReads := 0
	failure := errors.New("dependency Cluster read failed once")
	c := interceptor.NewClient(f.client, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*ledgerv1alpha1.Cluster); ok {
			clusterReads++
			if clusterReads == 2 {
				// Resume first refreshes its pointer, then the dependency hook
				// reads current metadata. Fail only that second read.
				return failure
			}
		}

		return c.Get(ctx, key, obj, opts...)
	}})
	ledger := f.latestLedger(t)
	handled, result, err := f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), ledger)
	require.True(t, handled)
	require.Positive(t, result.RequeueAfter)
	require.ErrorIs(t, err, failure)
	require.ErrorContains(t, err, "reading Cluster for storage recovery dependencies")
	require.Equal(t, 2, clusterReads)
	require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec, "dependency failure must not strand the absent StatefulSet")
	require.Equal(t, "RecoveryFailed", meta.FindStatusCondition(ledger.Status.Conditions, "ClaimTemplatesReady").Reason)
	handled, _, err = f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, 4, clusterReads, "retry must refresh dependencies again")
	require.False(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
	stored, _ := f.journal(t)
	require.Equal(t, journal.Data, stored.Data)
	f.requireStorageUnchanged(t)
}

func TestClaimTemplateRecoveryWaitsForObservedGenerationAndAdoptionThenRetriesCleanup(t *testing.T) {
	t.Parallel()
	for _, lostResponse := range []bool{false, true} {
		t.Run(fmt.Sprintf("cleanupLostResponse=%t", lostResponse), func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			r := f.reconciler(f.client)
			_, err := r.startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
			require.NoError(t, err)
			_, _, err = r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.NoError(t, err)
			replacement := f.replacement(t)
			_, _, err = r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.NoError(t, err)
			require.False(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
			replacement.Status = appsv1.StatefulSetStatus{ObservedGeneration: replacement.Generation}
			require.NoError(t, f.client.Status().Update(t.Context(), replacement))
			_, _, err = r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.NoError(t, err)
			require.False(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed, "observing the generation alone does not prove adoption")
			f.adoptPods(t, replacement)
			_, _, err = r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.NoError(t, err)
			require.True(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
			cm, _ := f.journal(t)
			attempts := 0
			failure := errors.New("journal cleanup unavailable")
			c := interceptor.NewClient(f.client, interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				attempts++
				options := (&client.DeleteOptions{}).ApplyOptions(opts)
				require.Equal(t, &metav1.Preconditions{UID: &cm.UID, ResourceVersion: &cm.ResourceVersion}, options.Preconditions)
				if attempts == 1 {
					if lostResponse {
						require.NoError(t, c.Delete(ctx, obj, opts...))
					}

					return failure
				}

				return c.Delete(ctx, obj, opts...)
			}})
			_, _, err = f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.ErrorIs(t, err, failure)
			require.True(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
			_, _, err = f.reconciler(c).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.NoError(t, err)
			if lostResponse {
				require.Equal(t, 1, attempts)
			} else {
				require.Equal(t, 2, attempts)
			}
			require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
			require.True(t, apierrors.IsNotFound(f.client.Get(t.Context(), client.ObjectKeyFromObject(cm), &corev1.ConfigMap{})))
			require.Equal(t, replacement.Spec, f.replacement(t).Spec)
			f.requireStorageUnchanged(t)
		})
	}
}

func TestClaimTemplateRecoveryUnreadyReplacementStillRequiresStableAdoption(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"unobserved generation", "missing Pod", "foreign Pod", "orphan Pod", "source-owned Pod", "deleting Pod"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			r := f.reconciler(f.client)
			_, err := r.startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
			require.NoError(t, err)
			_, _, err = r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.NoError(t, err)
			replacement := f.replacement(t)
			replacement.Status = appsv1.StatefulSetStatus{ObservedGeneration: replacement.Generation}
			f.adoptPods(t, replacement)
			pod := &corev1.Pod{}
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pods[0]), pod))
			switch state {
			case "unobserved generation":
				replacement.Status.ObservedGeneration--
			case "missing Pod":
				require.NoError(t, f.client.Delete(t.Context(), pod))
			case "foreign Pod":
				pod.OwnerReferences[0].UID = "foreign-statefulset"
				require.NoError(t, f.client.Update(t.Context(), pod))
			case "orphan Pod":
				pod.OwnerReferences = nil
				require.NoError(t, f.client.Update(t.Context(), pod))
			case "source-owned Pod":
				pod.OwnerReferences[0].UID = f.source.UID
				require.NoError(t, f.client.Update(t.Context(), pod))
			case "deleting Pod":
				pod.Finalizers = []string{"test.ledger.formance.com/hold-pod"}
				require.NoError(t, f.client.Update(t.Context(), pod))
				require.NoError(t, f.client.Delete(t.Context(), pod))
			}
			require.NoError(t, f.client.Status().Update(t.Context(), replacement))
			journal, _ := f.journal(t)
			ledger := f.latestLedger(t)
			handled, result, err := r.resumeClaimTemplateRecovery(t.Context(), ledger)
			require.NoError(t, err)
			require.Equal(t, "AwaitingAdoption", meta.FindStatusCondition(ledger.Status.Conditions, "ClaimTemplatesReady").Reason)
			require.True(t, handled)
			require.Equal(t, claimTemplateRequeueInterval, result.RequeueAfter)
			require.False(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
			stored, _ := f.journal(t)
			require.Equal(t, journal.UID, stored.UID)
			require.Equal(t, journal.Data, stored.Data)
			require.Equal(t, replacement.Spec, f.replacement(t).Spec)
			f.requireStorageUnchanged(t)
		})
	}
}

func TestClaimTemplateRecoveryMissingPodWaitsThenCompletesAndCleansUp(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	r := f.reconciler(f.client)
	_, err := r.startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.NoError(t, err)
	_, _, err = r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.NoError(t, err)
	replacement := f.replacement(t)
	replacement.Status = appsv1.StatefulSetStatus{ObservedGeneration: replacement.Generation}
	require.NoError(t, f.client.Status().Update(t.Context(), replacement))
	f.adoptPods(t, replacement)
	pod := &corev1.Pod{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pods[1]), pod))
	require.NoError(t, f.client.Delete(t.Context(), pod))
	journal, _ := f.journal(t)
	pointer := *f.latestLedger(t).Status.ClaimTemplateRecovery

	// Restart the reconciler at each stage. Missing and unadopted ordinals must
	// keep the exact durable operation until the replacement controls every Pod.
	for _, missing := range []bool{true, false} {
		if !missing {
			pod.UID = "restored-pod"
			pod.ResourceVersion = ""
			pod.OwnerReferences = nil
			require.NoError(t, f.client.Create(t.Context(), pod))
		}
		ledger := f.latestLedger(t)
		handled, result, err := f.reconciler(f.client).resumeClaimTemplateRecovery(t.Context(), ledger)
		require.NoError(t, err)
		require.True(t, handled)
		require.Equal(t, claimTemplateRequeueInterval, result.RequeueAfter)
		condition := meta.FindStatusCondition(ledger.Status.Conditions, "ClaimTemplatesReady")
		require.NotNil(t, condition)
		require.Equal(t, metav1.ConditionFalse, condition.Status)
		require.Equal(t, "AwaitingAdoption", condition.Reason)
		if missing {
			require.Contains(t, condition.Message, pod.Name)
		}
		require.Equal(t, &pointer, f.latestLedger(t).Status.ClaimTemplateRecovery)
		stored, _ := f.journal(t)
		require.Equal(t, journal, stored, "waiting must not mutate or replace the journal")
		require.Equal(t, replacement, f.replacement(t))
		f.requireStorageUnchanged(t)
	}

	require.NoError(t, controllerutil.SetControllerReference(replacement, pod, f.scheme))
	require.NoError(t, f.client.Update(t.Context(), pod))
	handled, result, err := f.reconciler(f.client).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, claimTemplateRequeueInterval, result.RequeueAfter)
	completed := pointer
	completed.Completed = true
	require.Equal(t, &completed, f.latestLedger(t).Status.ClaimTemplateRecovery)
	stored, _ := f.journal(t)
	require.Equal(t, journal, stored, "completion must persist before journal cleanup")
	_, _, err = f.reconciler(f.client).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.NoError(t, err)
	require.Nil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
	require.True(t, apierrors.IsNotFound(f.client.Get(t.Context(), client.ObjectKeyFromObject(journal), &corev1.ConfigMap{})))
	require.Equal(t, replacement, f.replacement(t))
	f.requireStorageUnchanged(t)
}

func TestClaimTemplateRecoveryEmptyAuthAndUnreadyAdoptionPermitNormalRepair(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	f.ledger.Spec.Persistence.WAL.StorageClass = "fast"
	f.ledger.Spec.Persistence.WAL.Size = resource.MustParse("10Gi")
	f.ledger.Spec.Persistence.Data.StorageClass = "fast"
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
	f.ledger.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{Enabled: new(true), Issuer: "https://issuer.example"}
	require.NoError(t, f.client.Update(t.Context(), f.ledger))
	cred := matchingCredentials("recovery-credential", nil, true, f.ledger.Namespace, "recovery-key")
	require.NoError(t, f.client.Create(t.Context(), cred))
	require.NoError(t, f.client.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "recovery-key", Namespace: f.ledger.Namespace},
		Data:       map[string][]byte{"key-id": []byte("original-key"), "pubkey.hex": []byte("original-public-key")},
	}))
	r := f.reconciler(f.client)
	keys, pending, err := r.reconcileAuthKeys(t.Context(), f.ledger)
	require.NoError(t, err)
	require.False(t, pending.pending())
	require.Len(t, keys, 1)
	f.source.Spec = buildStatefulSetSpec(f.ledger, computeSpecHash(&f.ledger.Spec), keys, tlsModeDisabled)
	f.source.CreationTimestamp = metav1.Now()
	require.NoError(t, f.client.Update(t.Context(), f.source))
	f.ledger.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
	f.ledger.Spec.Persistence.Data.VolumeAttributesClassName = "new-profile"
	require.NoError(t, f.client.Update(t.Context(), f.ledger))
	_, err = r.startClaimTemplateRecovery(t.Context(), f.ledger, f.source, buildVolumeClaimTemplates(f.ledger))
	require.NoError(t, err)
	journal, snapshot := f.journal(t)
	cm := &corev1.ConfigMap{}
	cmKey := client.ObjectKey{Namespace: f.ledger.Namespace, Name: authKeysConfigMapName(f.ledger.Name)}
	require.NoError(t, f.client.Get(t.Context(), cmKey, cm))
	cmUID := cm.UID

	// Explicit credential removal leaves the saved key-file wiring mounted.
	// A crashing Pod must not freeze the later auth/configuration correction.
	require.NoError(t, f.client.Delete(t.Context(), cred))
	desired := f.latestLedger(t)
	desired.Spec.Auth.Enabled = new(false)
	desired.Spec.PodAnnotations = map[string]string{"repair": "remove-empty-key-file"}
	require.NoError(t, f.client.Update(t.Context(), desired))
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(desired)}
	_, err = r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	replacement := f.replacement(t)
	require.Equal(t, snapshot.StatefulSet.Spec, replacement.Spec)
	replacement.CreationTimestamp = metav1.Now() // Supply the API identity omitted by the fake client.
	require.NoError(t, f.client.Update(t.Context(), replacement))
	replacement.Status = appsv1.StatefulSetStatus{ObservedGeneration: replacement.Generation, Replicas: 3, ReadyReplicas: 2}
	require.NoError(t, f.client.Status().Update(t.Context(), replacement))
	f.adoptPods(t, replacement)
	pod := &corev1.Pod{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pods[0]), pod))
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "ledger", RestartCount: 1,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
	}}
	require.NoError(t, f.client.Status().Update(t.Context(), pod))
	require.NoError(t, f.client.Get(t.Context(), cmKey, cm))
	require.Equal(t, cmUID, cm.UID)
	require.JSONEq(t, `{"keys":[]}`, cm.Data["auth-keys.json"])

	// The completed marker is durable before the journal or pointer disappears.
	_, err = f.reconciler(f.client).Reconcile(t.Context(), request)
	require.NoError(t, err)
	require.True(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
	require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec)
	stored, _ := f.journal(t)
	require.Equal(t, journal.Data, stored.Data)
	_, err = f.reconciler(f.client).Reconcile(t.Context(), request)
	require.NoError(t, err)
	latest := f.latestLedger(t)
	require.Nil(t, latest.Status.ClaimTemplateRecovery)
	require.True(t, apierrors.IsNotFound(f.client.Get(t.Context(), client.ObjectKeyFromObject(journal), &corev1.ConfigMap{})))
	require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec, "cleanup must preserve the saved workload")
	require.Equal(t, "Degraded", latest.Status.Phase)
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(latest.Status.Conditions, "Ready").Status)
	require.NotNil(t, meta.FindStatusCondition(latest.Status.Conditions, "AuthKeysRolloutDeferred"))

	// Execute the real normal controller path, including auth projection and the
	// StatefulSet write, with the same unready Pod and replica counters.
	_, err = f.reconciler(f.client).Reconcile(t.Context(), request)
	require.NoError(t, err)
	repaired := f.replacement(t)
	require.Equal(t, replacement.UID, repaired.UID)
	require.Equal(t, snapshot.StatefulSet.Spec.VolumeClaimTemplates, repaired.Spec.VolumeClaimTemplates)
	require.Equal(t, "remove-empty-key-file", repaired.Spec.Template.Annotations["repair"])
	for _, volume := range repaired.Spec.Template.Spec.Volumes {
		require.NotEqual(t, "auth-keys", volume.Name, "normal repair must remove the empty key-file mount")
	}
	for _, env := range repaired.Spec.Template.Spec.Containers[0].Env {
		require.NotEqual(t, "AUTH_ED25519_KEYS", env.Name, "normal repair must remove the empty key-file input")
	}
	latest = f.latestLedger(t)
	require.Nil(t, latest.Status.ClaimTemplateRecovery)
	require.Nil(t, meta.FindStatusCondition(latest.Status.Conditions, "AuthKeysRolloutDeferred"))
	require.Equal(t, "Degraded", latest.Status.Phase)
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(latest.Status.Conditions, "Ready").Status)
	require.Equal(t, int32(2), repaired.Status.ReadyReplicas)
	require.Zero(t, repaired.Status.UpdatedReplicas)
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(pod), pod))
	require.True(t, metav1.IsControlledBy(pod, repaired))
	require.Equal(t, corev1.ConditionFalse, pod.Status.Conditions[0].Status)
	require.Equal(t, "CrashLoopBackOff", pod.Status.ContainerStatuses[0].State.Waiting.Reason)
	f.requireStorageUnchanged(t)
}

func TestClaimTemplateRecoveryRefusesChangedRetainedIdentity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		edit func(*testing.T, *claimRecoveryFixture)
		want string
	}{
		{"foreign journal owner", func(t *testing.T, f *claimRecoveryFixture) {
			cm, _ := f.journal(t)
			cm.OwnerReferences[0].UID = "foreign-cluster"
			require.NoError(t, f.client.Update(t.Context(), cm))
		}, "foreign owner"},
		{"changed journal UID", func(t *testing.T, f *claimRecoveryFixture) {
			cm, _ := f.journal(t)
			cm.UID = "other-journal"
			require.NoError(t, f.client.Update(t.Context(), cm))
		}, "differs from committed pointer"},
		{"replaced PVC", func(t *testing.T, f *claimRecoveryFixture) {
			pvc := f.pvcs[0].DeepCopy()
			pvc.UID = "other-pvc"
			require.NoError(t, f.client.Update(t.Context(), pvc))
		}, "identity changed"},
		{"rebound PVC", func(t *testing.T, f *claimRecoveryFixture) {
			pvc := f.pvcs[0].DeepCopy()
			pvc.Spec.VolumeName = "other-volume"
			require.NoError(t, f.client.Update(t.Context(), pvc))
		}, "identity changed"},
		{"replaced PV", func(t *testing.T, f *claimRecoveryFixture) {
			pv := f.pvs[0].DeepCopy()
			pv.UID = "other-pv"
			require.NoError(t, f.client.Update(t.Context(), pv))
		}, "identity or binding changed"},
		{"rebound PV", func(t *testing.T, f *claimRecoveryFixture) {
			pv := f.pvs[0].DeepCopy()
			pv.Spec.ClaimRef.UID = "other-pvc"
			require.NoError(t, f.client.Update(t.Context(), pv))
		}, "identity or binding changed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for _, adopted := range []bool{false, true} {
				t.Run(fmt.Sprintf("replacementAdopted=%t", adopted), func(t *testing.T) {
					t.Parallel()
					f := newClaimRecoveryFixture(t)
					r := f.reconciler(f.client)
					_, err := r.startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
					require.NoError(t, err)
					if adopted {
						_, _, err = r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
						require.NoError(t, err)
						replacement := f.replacement(t)
						replacement.Status.ObservedGeneration = replacement.Generation
						require.NoError(t, f.client.Status().Update(t.Context(), replacement))
						f.adoptPods(t, replacement)
					}
					tt.edit(t, f)
					handled, _, err := r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
					require.True(t, handled)
					require.ErrorContains(t, err, tt.want)
					require.False(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
					f.journal(t)
					if !adopted {
						require.True(t, apierrors.IsNotFound(f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &appsv1.StatefulSet{})))
					}
				})
			}
		})
	}
}

func TestClaimTemplateRecoveryMissingActiveJournalFailsClosed(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	_, err := f.reconciler(f.client).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.NoError(t, err)
	cm, _ := f.journal(t)
	require.NoError(t, f.client.Delete(t.Context(), cm))
	handled, _, err := f.reconciler(f.client).resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
	require.True(t, handled)
	require.ErrorContains(t, err, "active storage recovery journal")
	require.ErrorContains(t, err, "is missing")
	require.NotNil(t, f.latestLedger(t).Status.ClaimTemplateRecovery)
	require.True(t, apierrors.IsNotFound(f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &appsv1.StatefulSet{})))
}

func TestClaimTemplateRecoveryRequiresStableSourceAndBoundClaims(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		edit func(*testing.T, *claimRecoveryFixture)
		want string
	}{
		{"source has no UID", func(_ *testing.T, f *claimRecoveryFixture) { f.source.UID = "" }, "not controlled by Cluster"},
		{"foreign source owner", func(_ *testing.T, f *claimRecoveryFixture) { f.source.OwnerReferences[0].UID = "other-cluster" }, "not controlled by Cluster"},
		{"foreign Pod owner", func(t *testing.T, f *claimRecoveryFixture) {
			pod := f.pods[0].DeepCopy()
			pod.OwnerReferences[0].UID = "other-sts"
			require.NoError(t, f.client.Update(t.Context(), pod))
		}, "not a stable dependent"},
		{"wrong Pod claim reference", func(t *testing.T, f *claimRecoveryFixture) {
			pod := f.pods[0].DeepCopy()
			pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "other-claim"
			require.NoError(t, f.client.Update(t.Context(), pod))
		}, "does not reference retained claim"},
		{"claim has no UID", func(t *testing.T, f *claimRecoveryFixture) {
			pvc := f.pvcs[0].DeepCopy()
			pvc.UID = ""
			require.NoError(t, f.client.Update(t.Context(), pvc))
		}, "not stably bound"},
		{"claim has no volume", func(t *testing.T, f *claimRecoveryFixture) {
			pvc := f.pvcs[0].DeepCopy()
			pvc.Spec.VolumeName = ""
			require.NoError(t, f.client.Update(t.Context(), pvc))
		}, "not stably bound"},
		{"claim is unbound", func(t *testing.T, f *claimRecoveryFixture) {
			pvc := &corev1.PersistentVolumeClaim{}
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pvcs[0]), pvc))
			pvc.Status.Phase = corev1.ClaimPending
			require.NoError(t, f.client.Status().Update(t.Context(), pvc))
		}, "not stably bound"},
		{"foreign PVC owner", func(t *testing.T, f *claimRecoveryFixture) {
			pvc := f.pvcs[0].DeepCopy()
			pvc.OwnerReferences = []metav1.OwnerReference{{APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "StatefulSet", Name: "foreign-statefulset", UID: "foreign-sts-uid", Controller: new(true)}}
			require.NoError(t, f.client.Update(t.Context(), pvc))
		}, "foreign owner"},
		{"PV points at different PVC UID", func(t *testing.T, f *claimRecoveryFixture) {
			pv := f.pvs[0].DeepCopy()
			pv.Spec.ClaimRef.UID = "other-pvc"
			require.NoError(t, f.client.Update(t.Context(), pv))
		}, "does not retain the expected claim binding"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			tt.edit(t, f)
			_, err := f.reconciler(f.client).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
			require.ErrorContains(t, err, tt.want)
			requireNoClaimTemplateRecovery(t, f.client, f.ledger)
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &appsv1.StatefulSet{}), "refused recovery must not delete the source")
		})
	}
}

func TestClaimTemplateRecoveryChecksUncachedPodIdentity(t *testing.T) {
	t.Parallel()
	f := newClaimRecoveryFixture(t)
	pod := f.pods[0].DeepCopy()
	pod.OwnerReferences[0].UID = "foreign-sts"
	require.NoError(t, f.client.Update(t.Context(), pod))
	cached := interceptor.NewClient(f.client, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if cachedPod, ok := obj.(*corev1.Pod); ok && key == client.ObjectKeyFromObject(pod) {
			f.pods[0].DeepCopyInto(cachedPod)

			return nil
		}

		return c.Get(ctx, key, obj, opts...)
	}})
	r := &ClusterReconciler{Client: cached, APIReader: f.client, Scheme: f.scheme}
	_, err := r.startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.ErrorContains(t, err, "not a stable dependent")
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &appsv1.StatefulSet{}))
}

func TestClaimTemplateRecoveryPreservesEveryRetentionPolicy(t *testing.T) {
	t.Parallel()
	for _, whenDeleted := range []appsv1.PersistentVolumeClaimRetentionPolicyType{appsv1.RetainPersistentVolumeClaimRetentionPolicyType, appsv1.DeletePersistentVolumeClaimRetentionPolicyType} {
		for _, whenScaled := range []appsv1.PersistentVolumeClaimRetentionPolicyType{appsv1.RetainPersistentVolumeClaimRetentionPolicyType, appsv1.DeletePersistentVolumeClaimRetentionPolicyType} {
			t.Run(fmt.Sprintf("deleted=%s/scaled=%s", whenDeleted, whenScaled), func(t *testing.T) {
				t.Parallel()
				f := newClaimRecoveryFixture(t)
				f.source.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{WhenDeleted: whenDeleted, WhenScaled: whenScaled}
				require.NoError(t, f.client.Update(t.Context(), f.source))
				_, err := f.reconciler(f.client).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
				require.NoError(t, err)
				_, snapshot := f.journal(t)
				require.Equal(t, f.source.Spec.PersistentVolumeClaimRetentionPolicy, snapshot.StatefulSet.Spec.PersistentVolumeClaimRetentionPolicy)
				require.True(t, apierrors.IsNotFound(f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &appsv1.StatefulSet{})))
				f.requireStorageUnchanged(t)
			})
		}
	}
}

func TestClaimTemplateRecoveryRejectsForeignReplacement(t *testing.T) {
	t.Parallel()
	for _, foreignOwner := range []bool{true, false} {
		t.Run(fmt.Sprintf("foreignOwner=%t", foreignOwner), func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			r := f.reconciler(f.client)
			_, err := r.startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
			require.NoError(t, err)
			_, _, err = r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.NoError(t, err)
			replacement := f.replacement(t)
			want := "does not match committed storage recovery state"
			if foreignOwner {
				replacement.OwnerReferences[0].UID = "foreign-cluster"
				want = "foreign owner during storage recovery"
			} else {
				replacement.Spec.Template.Spec.Containers[0].Image = "unrelated-image"
			}
			require.NoError(t, f.client.Update(t.Context(), replacement))
			_, _, err = r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.ErrorContains(t, err, want)
			f.journal(t)
			require.False(t, f.latestLedger(t).Status.ClaimTemplateRecovery.Completed)
			f.requireStorageUnchanged(t)
		})
	}
}
