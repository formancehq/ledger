//go:build integration

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestClaimTemplateRecoveryMissingPodWaitsThenCleansUpIntegration(t *testing.T) {
	t.Parallel()
	// Use a real API without the shared manager so every intermediate durable
	// state remains observable. The test supplies controller observations; it
	// does not prove Kubernetes garbage collection, adoption, kubelet or CSI.
	isolated := &envtest.Environment{
		CRDDirectoryPaths: testEnv.CRDDirectoryPaths,
		Scheme:            k8sClient.Scheme(),
	}
	config, err := isolated.Start()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, isolated.Stop()) })
	c, err := client.NewWithWatch(config, client.Options{Scheme: isolated.Scheme})
	require.NoError(t, err)
	namespace := "missing-pod-recovery"
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}))
	cluster := newCluster("missing-pod", namespace)
	cluster.Spec.Replicas = new(int32(1))
	require.NoError(t, c.Create(t.Context(), cluster))
	labels := map[string]string{"test.ledger.formance.com/workload": cluster.Name}
	replacement := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceName(cluster.Name), Namespace: namespace,
			Annotations: map[string]string{claimTemplateRecoveryAnnotation: "source-sts-uid"},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: new(int32(1)), ServiceName: "original-headless",
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "ledger", Image: "ledger:v3.0.0-rc.1"}}},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{recoveryClaimTemplate("data", "150Gi")},
		},
	}
	require.NoError(t, controllerutil.SetControllerReference(cluster, replacement, c.Scheme()))
	require.NoError(t, c.Create(t.Context(), replacement))
	require.NotEmpty(t, replacement.UID)
	require.NotEqualValues(t, "source-sts-uid", replacement.UID)
	replacement.Status.ObservedGeneration = replacement.Generation
	require.NoError(t, c.Status().Update(t.Context(), replacement))
	claim := replacement.Spec.VolumeClaimTemplates[0].DeepCopy()
	claim.Name, claim.Namespace = "data-"+replacement.Name+"-0", namespace
	claim.Spec.VolumeName = "missing-pod-data"
	require.NoError(t, c.Create(t.Context(), claim))
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: claim.Spec.VolumeName},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: claim.Spec.Resources.Requests.DeepCopy(), AccessModes: claim.Spec.AccessModes,
			StorageClassName: *claim.Spec.StorageClassName,
			ClaimRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim",
				Name: claim.Name, Namespace: namespace, UID: claim.UID},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
				Driver: "storage-recovery.example.com", VolumeHandle: claim.Spec.VolumeName,
			}},
		},
	}
	require.NoError(t, c.Create(t.Context(), pv))
	claim.Status.Phase = corev1.ClaimBound
	require.NoError(t, c.Status().Update(t.Context(), claim))
	snapshot := claimTemplateRecovery{
		Version: 1, SourceUID: "source-sts-uid", SourceGeneration: 1,
		StatefulSet: appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: replacement.Name, Namespace: namespace,
				OwnerReferences: replacement.OwnerReferences, Annotations: replacement.Annotations},
			Spec: *replacement.Spec.DeepCopy(),
		},
		Claims: []retainedClaimIdentity{{Name: claim.Name, UID: claim.UID, VolumeName: pv.Name, PVUID: pv.UID}},
	}
	data, err := json.Marshal(snapshot)
	require.NoError(t, err)
	digest := claimTemplateRecoveryDigest(cluster, data)
	pointer := &ledgerv1alpha1.ClaimTemplateRecoveryStatus{ConfigMapName: claimTemplateRecoveryDigestName(cluster, digest), SourceUID: string(snapshot.SourceUID), PayloadSHA256: digest}
	r := &ClusterReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
	require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), cluster, nil, pointer))
	journal := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: pointer.ConfigMapName, Namespace: namespace},
		Immutable:  new(true), Data: map[string]string{claimTemplateRecoveryKey: string(data)},
	}
	require.NoError(t, controllerutil.SetControllerReference(cluster, journal, c.Scheme()))
	require.NoError(t, c.Create(t.Context(), journal))
	preparing := pointer.DeepCopy()
	pointer.ConfigMapUID = string(journal.UID)
	require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), cluster, preparing, pointer))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: replacement.Name + "-0", Namespace: namespace, Labels: labels},
		Spec:       *replacement.Spec.Template.Spec.DeepCopy(),
	}
	pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim.Name},
	}}}
	for _, missing := range []bool{true, false} {
		if !missing {
			require.NoError(t, c.Create(t.Context(), pod))
		}
		restarted := &ClusterReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
		handled, result, err := restarted.resumeClaimTemplateRecovery(t.Context(), cluster)
		require.NoError(t, err)
		require.True(t, handled)
		require.Equal(t, claimTemplateRequeueInterval, result.RequeueAfter)
		condition := meta.FindStatusCondition(cluster.Status.Conditions, "ClaimTemplatesReady")
		require.NotNil(t, condition)
		require.Equal(t, metav1.ConditionFalse, condition.Status)
		require.Equal(t, "AwaitingAdoption", condition.Reason)
		if missing {
			require.Contains(t, condition.Message, pod.Name)
		}
		current := &ledgerv1alpha1.Cluster{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), current))
		require.Equal(t, pointer, current.Status.ClaimTemplateRecovery)
		stored := &corev1.ConfigMap{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(journal), stored))
		require.Equal(t, journal, stored, "waiting must retain the exact API-persisted journal")
	}
	require.NoError(t, controllerutil.SetControllerReference(replacement, pod, c.Scheme()))
	require.NoError(t, c.Update(t.Context(), pod))
	_, _, err = r.resumeClaimTemplateRecovery(t.Context(), cluster)
	require.NoError(t, err)
	current := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), current))
	completed := *pointer
	completed.Completed = true
	require.Equal(t, &completed, current.Status.ClaimTemplateRecovery, "completion must be durable before cleanup")
	stored := &corev1.ConfigMap{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(journal), stored))
	require.Equal(t, journal, stored)
	_, _, err = r.resumeClaimTemplateRecovery(t.Context(), current)
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), current))
	require.Nil(t, current.Status.ClaimTemplateRecovery)
	require.True(t, apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(journal), &corev1.ConfigMap{})))
	live := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(replacement), live))
	require.Equal(t, replacement, live)
	require.Zero(t, live.Status.ReadyReplicas)
	storedClaim := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(claim), storedClaim))
	require.Equal(t, claim, storedClaim)
	storedPV := &corev1.PersistentVolume{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pv), storedPV))
	require.Equal(t, pv, storedPV)
}

func TestClaimTemplateRecoveryPersistsSnapshotBeforeOrphanDeleteIntegration(t *testing.T) {
	t.Parallel()
	namespace := createTestNamespace(t)
	c, err := client.NewWithWatch(testEnv.Config, client.Options{Scheme: k8sClient.Scheme()})
	require.NoError(t, err)

	// The shared suite runs the custom operator manager. Even replicas keep its
	// normal reconciliation behind validateSpec while fixtures are prepared.
	// Once the journal exists, either reconciler may resume that same journal;
	// the source finalizer below holds both at the real orphan-delete boundary.
	cluster := &ledgerv1alpha1.Cluster{
		TypeMeta: metav1.TypeMeta{APIVersion: ledgerv1alpha1.GroupVersion.String(), Kind: "Cluster"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "claim-template-recovery", Namespace: namespace,
			Finalizers: []string{"test.ledger.formance.com/storage-recovery"},
		},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: new(int32(2)), ClusterID: "c56b1aa4-85e8-4d73-9f3b-5e25c6906153",
		},
	}
	require.NoError(t, c.Create(ctx, cluster))
	require.NotEmpty(t, cluster.UID)
	objects := []client.Object{cluster}
	t.Cleanup(func() {
		// Stop recovery before releasing the source finalizer. envtest has no
		// garbage collector or protection controllers to clear these finalizers.
		for _, obj := range objects {
			if err := c.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
				assert.NoError(t, err)
				continue
			}
			latest := obj.DeepCopyObject().(client.Object)
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), latest); apierrors.IsNotFound(err) {
				continue
			} else if !assert.NoError(t, err) {
				continue
			}
			if len(latest.GetFinalizers()) != 0 {
				base := latest.DeepCopyObject().(client.Object)
				latest.SetFinalizers(nil)
				assert.NoError(t, c.Patch(ctx, latest, client.MergeFrom(base)))
			}
		}
	})

	labels := map[string]string{"test.ledger.formance.com/workload": cluster.Name}
	source := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceName(cluster.Name), Namespace: namespace,
			Labels: labels, Annotations: map[string]string{"test.ledger.formance.com/source": "retained"},
			Finalizers: []string{"test.ledger.formance.com/hold-source"},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: new(int32(3)), ServiceName: "original-headless",
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						annotationSpecHash: "original-spec-hash", annotationKubectlRestartedAt: "2026-10-01T08:00:00Z",
					},
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name: "ledger", Image: "ghcr.io/formancehq/ledger:v3.0.0-rc.1",
					Args:         []string{"serve", "--original-setting"},
					Env:          []corev1.EnvVar{{Name: "TLS_MODE", Value: "required"}},
					VolumeMounts: []corev1.VolumeMount{{Name: "wal", MountPath: "/data/raft"}, {Name: "data", MountPath: "/data/app"}},
				}}},
			},
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
		},
	}
	for _, volume := range []struct{ name, size string }{{"wal", "5Gi"}, {"data", "50Gi"}} {
		source.Spec.VolumeClaimTemplates = append(source.Spec.VolumeClaimTemplates, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: volume.name},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: new(""), AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(volume.size)},
				},
			},
		})
	}
	require.NoError(t, controllerutil.SetControllerReference(cluster, source, c.Scheme()))
	require.NoError(t, c.Create(ctx, source))
	objects = append(objects, source)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(source), source))
	require.NotEmpty(t, source.UID)
	require.Positive(t, source.Generation)
	require.True(t, metav1.IsControlledBy(source, cluster))
	original := source.DeepCopy()

	for _, test := range []struct {
		name   string
		mutate func(*appsv1.StatefulSet)
	}{
		{name: "size", mutate: func(sts *appsv1.StatefulSet) {
			sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("10Gi")
		}},
		{name: "volumeAttributesClassName", mutate: func(sts *appsv1.StatefulSet) {
			sts.Spec.VolumeClaimTemplates[1].Spec.VolumeAttributesClassName = new("high-iops")
		}},
	} {
		t.Run("immutable-"+test.name, func(t *testing.T) {
			candidate := &appsv1.StatefulSet{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(source), candidate))
			test.mutate(candidate)
			err := c.Update(ctx, candidate)
			require.True(t, apierrors.IsInvalid(err), "claim-template mutation must fail StatefulSet validation: %v", err)
			require.Contains(t, err.Error(), "updates to statefulset spec for fields other than")
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(source), source))
			require.True(t, apiequality.Semantic.DeepEqual(original.Spec, source.Spec))
			require.Equal(t, original.Generation, source.Generation)
		})
	}

	var retained []retainedClaimIdentity
	var pods []*corev1.Pod
	for ordinal := range *source.Spec.Replicas {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", source.Name, ordinal), Namespace: namespace, Labels: labels},
			Spec:       *source.Spec.Template.Spec.DeepCopy(),
		}
		require.NoError(t, controllerutil.SetControllerReference(source, pod, c.Scheme()))
		for _, template := range source.Spec.VolumeClaimTemplates {
			claim := template.DeepCopy()
			claim.Name, claim.Namespace = template.Name+"-"+pod.Name, namespace
			claim.Spec.VolumeName = namespace + "-" + claim.Name
			require.NoError(t, c.Create(ctx, claim))
			objects = append(objects, claim)
			require.NotEmpty(t, claim.UID)
			pv := &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: claim.Spec.VolumeName},
				Spec: corev1.PersistentVolumeSpec{
					Capacity: claim.Spec.Resources.Requests.DeepCopy(), AccessModes: claim.Spec.AccessModes,
					PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
					ClaimRef:                      &corev1.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: claim.Name, Namespace: namespace, UID: claim.UID},
					PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
						Driver: "storage-recovery.example.com", VolumeHandle: claim.Spec.VolumeName,
					}},
				},
			}
			require.NoError(t, c.Create(ctx, pv))
			objects = append(objects, pv)
			require.NotEmpty(t, pv.UID)
			pv.Status.Phase = corev1.VolumeBound
			require.NoError(t, c.Status().Update(ctx, pv))
			claim.Status.Phase = corev1.ClaimBound
			claim.Status.Capacity = claim.Spec.Resources.Requests.DeepCopy()
			require.NoError(t, c.Status().Update(ctx, claim))
			retained = append(retained, retainedClaimIdentity{Name: claim.Name, UID: claim.UID, VolumeName: pv.Name, PVUID: pv.UID})
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
				Name:         template.Name,
				VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim.Name}},
			})
		}
		require.NoError(t, c.Create(ctx, pod))
		objects = append(objects, pod)
		require.NotEmpty(t, pod.UID)
		pods = append(pods, pod)
	}

	desired := original.DeepCopy().Spec.VolumeClaimTemplates
	desired[0].Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("10Gi")
	desired[1].Spec.VolumeAttributesClassName = new("high-iops")
	expectedSpec := original.Spec.DeepCopy()
	expectedSpec.VolumeClaimTemplates = desired
	journalKey := client.ObjectKey{Namespace: namespace, Name: source.Name + "-storage-recovery"}
	journalCreated := false
	wrapped := interceptor.NewClient(c, interceptor.Funcs{
		Create: func(callCtx context.Context, inner client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Data[claimTemplateRecoveryKey] != "" {
				journalKey = client.ObjectKeyFromObject(cm)
				live := &appsv1.StatefulSet{}
				require.NoError(t, c.Get(callCtx, client.ObjectKeyFromObject(source), live))
				require.True(t, live.DeletionTimestamp.IsZero(), "source must still exist before the journal is persisted")
				if err := inner.Create(callCtx, obj, opts...); err != nil {
					return err
				}
				objects = append(objects, obj)
				journalCreated = true
				return nil
			}
			return inner.Create(callCtx, obj, opts...)
		},
		Delete: func(callCtx context.Context, inner client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if sts, ok := obj.(*appsv1.StatefulSet); ok {
				var options client.DeleteOptions
				options.ApplyOptions(opts)
				require.NotNil(t, options.PropagationPolicy)
				require.Equal(t, metav1.DeletePropagationOrphan, *options.PropagationPolicy)
				require.NotNil(t, options.Preconditions)
				require.Equal(t, &original.UID, options.Preconditions.UID)
				require.Equal(t, &sts.ResourceVersion, options.Preconditions.ResourceVersion)
				// Read independently from the real API at the destructive boundary.
				journal := &corev1.ConfigMap{}
				require.NoError(t, c.Get(callCtx, journalKey, journal))
				current := &ledgerv1alpha1.Cluster{}
				require.NoError(t, c.Get(callCtx, client.ObjectKeyFromObject(cluster), current))
				require.Equal(t, &ledgerv1alpha1.ClaimTemplateRecoveryStatus{
					ConfigMapName: journal.Name, ConfigMapUID: string(journal.UID), SourceUID: string(original.UID), PayloadSHA256: claimTemplateRecoveryDigest(cluster, []byte(journal.Data[claimTemplateRecoveryKey])),
				}, current.Status.ClaimTemplateRecovery, "journal pointer must be durable before source deletion")
			}
			return inner.Delete(callCtx, obj, opts...)
		},
	})
	r := &ClusterReconciler{Client: wrapped, APIReader: c, Scheme: c.Scheme()}
	// envtest has no StatefulSet controller. Publish the settled source status
	// before committing the journal; an unready source now invalidates a plan
	// while deletion has not started. The finalizer holds the accepted delete.
	source.Status.ObservedGeneration = source.Generation
	source.Status.Replicas = *source.Spec.Replicas
	source.Status.UpdatedReplicas = *source.Spec.Replicas
	source.Status.ReadyReplicas = *source.Spec.Replicas
	require.NoError(t, c.Status().Update(ctx, source))
	// Exercise the recovery helper directly. The invalid desired replica count
	// isolates this API contract; normal Reconcile admission is tested elsewhere.
	_, err = r.startClaimTemplateRecovery(ctx, cluster, original, desired)
	require.True(t, err == nil || apierrors.IsConflict(err) || strings.Contains(err.Error(), "storage recovery pointer changed"), "only a concurrent status transition may need retry: %v", err)

	// A conflict can precede the first journal create. Retry an existing journal
	// or repeat full preparation if no operation remains. The source finalizer
	// keeps the accepted delete observable while real durable records are read.
	journal := &corev1.ConfigMap{}
	current := &ledgerv1alpha1.Cluster{}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		require.NoError(collect, c.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
		if cluster.Status.ClaimTemplateRecovery == nil {
			// A concurrent reader can abandon Preparing before the first CM
			// create commits. A new full preparation may reuse exact late bytes.
			_, err := r.startClaimTemplateRecovery(ctx, cluster, original, desired)
			require.NoError(collect, err)
		}
		handled, result, err := r.resumeClaimTemplateRecovery(ctx, cluster)
		require.NoError(collect, err)
		require.True(collect, handled)
		require.Equal(collect, claimTemplateRequeueInterval, result.RequeueAfter)
		require.NoError(collect, c.Get(ctx, journalKey, journal))
		require.NoError(collect, c.Get(ctx, client.ObjectKeyFromObject(cluster), current))
		require.Equal(collect, &ledgerv1alpha1.ClaimTemplateRecoveryStatus{
			ConfigMapName: journal.Name, ConfigMapUID: string(journal.UID), SourceUID: string(original.UID), PayloadSHA256: claimTemplateRecoveryDigest(cluster, []byte(journal.Data[claimTemplateRecoveryKey])),
		}, current.Status.ClaimTemplateRecovery)
		require.NoError(collect, c.Get(ctx, client.ObjectKeyFromObject(source), source))
		require.False(collect, source.DeletionTimestamp.IsZero(), "accepted orphan deletion must retain the journal and marker")
	}, 10*time.Second, 100*time.Millisecond)

	require.True(t, journalCreated)
	require.NotEmpty(t, journal.UID)
	require.Equal(t, new(true), journal.Immutable)
	require.True(t, metav1.IsControlledBy(journal, cluster))
	var snapshot claimTemplateRecovery
	require.NoError(t, json.Unmarshal([]byte(journal.Data[claimTemplateRecoveryKey]), &snapshot))
	require.Equal(t, 1, snapshot.Version)
	require.Equal(t, original.UID, snapshot.SourceUID)
	require.Equal(t, original.Generation, snapshot.SourceGeneration)
	require.True(t, apiequality.Semantic.DeepEqual(*expectedSpec, snapshot.StatefulSet.Spec), "only claim templates may change in the durable source snapshot")
	require.Equal(t, original.Spec.Template, snapshot.StatefulSet.Spec.Template)
	require.Equal(t, original.Spec.Replicas, snapshot.StatefulSet.Spec.Replicas)
	require.Equal(t, original.Labels, snapshot.StatefulSet.Labels)
	require.Equal(t, "retained", snapshot.StatefulSet.Annotations["test.ledger.formance.com/source"])
	require.Equal(t, string(original.UID), snapshot.StatefulSet.Annotations[claimTemplateRecoveryAnnotation])
	require.True(t, metav1.IsControlledBy(&snapshot.StatefulSet, cluster))
	require.ElementsMatch(t, retained, snapshot.Claims)

	require.Equal(t, original.UID, source.UID)
	require.Contains(t, source.Finalizers, "test.ledger.formance.com/hold-source")
	require.Contains(t, source.Finalizers, metav1.FinalizerOrphanDependents)
	require.True(t, apiequality.Semantic.DeepEqual(original.Spec, source.Spec))

	// Readiness loss after accepted deletion must not cancel the journal, even
	// though the source UID is still present. The invalid desired spec remains
	// irrelevant to restoring this saved workload.
	source.Status.ReadyReplicas = 0
	require.NoError(t, c.Status().Update(ctx, source))
	// A fresh reconciler must keep the same journal while deletion is stalled.
	// There is no Kubernetes GC, StatefulSet controller or kubelet in envtest:
	// do not simulate orphaning/adoption or claim a completed storage rollout.
	restarted := &ClusterReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
	handled, result, err := restarted.resumeClaimTemplateRecovery(ctx, current)
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, claimTemplateRequeueInterval, result.RequeueAfter)
	stillStored := &corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, journalKey, stillStored))
	require.Equal(t, journal.UID, stillStored.UID)
	require.Equal(t, journal.Data, stillStored.Data)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cluster), current))
	require.NotNil(t, current.Status.ClaimTemplateRecovery)
	require.False(t, current.Status.ClaimTemplateRecovery.Completed)
	for _, originalPod := range pods {
		live := &corev1.Pod{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(originalPod), live))
		require.Equal(t, originalPod.UID, live.UID)
		require.True(t, live.DeletionTimestamp.IsZero())
		require.True(t, metav1.IsControlledBy(live, original))
		require.Equal(t, originalPod.Spec.Volumes, live.Spec.Volumes)
	}
	for _, identity := range retained {
		claim := &corev1.PersistentVolumeClaim{}
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: identity.Name}, claim))
		require.Equal(t, identity.UID, claim.UID)
		require.Equal(t, identity.VolumeName, claim.Spec.VolumeName)
		require.Equal(t, corev1.ClaimBound, claim.Status.Phase)
		require.True(t, claim.DeletionTimestamp.IsZero())
		pv := &corev1.PersistentVolume{}
		require.NoError(t, c.Get(ctx, client.ObjectKey{Name: identity.VolumeName}, pv))
		require.Equal(t, identity.PVUID, pv.UID)
		require.NotNil(t, pv.Spec.ClaimRef)
		require.Equal(t, identity.UID, pv.Spec.ClaimRef.UID)
		require.True(t, pv.DeletionTimestamp.IsZero())
	}
}
