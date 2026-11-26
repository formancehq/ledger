//go:build integration

package controller

import (
	"sync/atomic"
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
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestClaimTemplateRetryQuotaThroughManagerIntegration(t *testing.T) {
	t.Parallel()
	cluster, source, _, identity := storageManagerFixture(t, false, false)
	c, err := client.NewWithWatch(testEnv.Config, client.Options{Scheme: k8sClient.Scheme()})
	require.NoError(t, err)
	clusterKey, sourceKey := client.ObjectKeyFromObject(cluster), client.ObjectKeyFromObject(source)
	before := cluster.DeepCopy()
	cluster.Spec.Replicas = new(int32(1))
	require.NoError(t, c.Patch(t.Context(), cluster, client.MergeFrom(before)))
	requireEventually(t, func() bool {
		if c.Get(t.Context(), clusterKey, cluster) != nil || c.Get(t.Context(), sourceKey, source) != nil {
			return false
		}
		condition := meta.FindStatusCondition(cluster.Status.Conditions, "ClaimTemplatesReady")
		return condition != nil && condition.Status == metav1.ConditionTrue && condition.ObservedGeneration == cluster.Generation && rolloutConverged(source)
	}, "settle unchanged storage through the actual manager before denying journals")
	original := source.DeepCopy()
	occupant := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: cluster.Namespace, Name: "retry-quota-occupant"}}
	require.NoError(t, c.Create(t.Context(), occupant))
	t.Cleanup(func() { assert.NoError(t, client.IgnoreNotFound(c.Delete(ctx, occupant))) })
	quotaResource := corev1.ResourceName("count/configmaps")
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: cluster.Namespace, Name: "deny-recovery-journal"},
		Spec:       corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{quotaResource: resource.MustParse("1")}},
	}
	require.NoError(t, c.Create(t.Context(), quota))
	t.Cleanup(func() { assert.NoError(t, client.IgnoreNotFound(c.Delete(ctx, quota))) })
	// envtest has admission but no quota controller. Publish the real occupied
	// count explicitly; the API server's ResourceQuota admission rejects writes.
	quota.Status.Hard = quota.Spec.Hard.DeepCopy()
	quota.Status.Used = corev1.ResourceList{quotaResource: resource.MustParse("1")}
	require.NoError(t, c.Status().Update(t.Context(), quota))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(quota), quota))
	require.Equal(t, quota.Spec.Hard, quota.Status.Hard)
	require.Equal(t, quota.Spec.Hard, quota.Status.Used)
	probe := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: cluster.Namespace, Name: "quota-denied-preview"}}
	err = c.Create(t.Context(), probe, client.DryRunAll)
	require.True(t, apierrors.IsForbidden(err))
	require.ErrorContains(t, err, "exceeded quota")

	require.NoError(t, c.Get(t.Context(), clusterKey, cluster))
	stream, err := c.Watch(t.Context(), &ledgerv1alpha1.ClusterList{}, client.InNamespace(cluster.Namespace),
		client.MatchingFields{"metadata.name": cluster.Name}, &client.ListOptions{Raw: &metav1.ListOptions{ResourceVersion: cluster.ResourceVersion}})
	require.NoError(t, err)
	defer stream.Stop()
	var events, statusChanges, intents, clears atomic.Int64
	watchErrors := make(chan watch.Event, 1)
	watchDone := make(chan struct{})
	previousStatus := cluster.Status.DeepCopy()
	go func() {
		defer close(watchDone)
		hadIntent := false
		for event := range stream.ResultChan() {
			if event.Type == watch.Error {
				select {
				case watchErrors <- event:
				default:
				}
				return
			}
			current, ok := event.Object.(*ledgerv1alpha1.Cluster)
			if !ok {
				continue
			}
			events.Add(1)
			if !apiequality.Semantic.DeepEqual(previousStatus, &current.Status) {
				statusChanges.Add(1)
				previousStatus = current.Status.DeepCopy()
			}
			hasIntent := current.Status.ClaimTemplateRecovery != nil
			if hasIntent && !hadIntent {
				intents.Add(1)
			} else if !hasIntent && hadIntent {
				clears.Add(1)
			}
			hadIntent = hasIntent
		}
	}()
	defer func() {
		stream.Stop()
		<-watchDone
	}()
	before = cluster.DeepCopy()
	cluster.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
	require.NoError(t, c.Patch(t.Context(), cluster, client.MergeFrom(before)))
	requireEventually(t, func() bool {
		if c.Get(t.Context(), clusterKey, cluster) != nil {
			return false
		}
		return clears.Load() >= 1 && cluster.Status.ClaimTemplateRecovery == nil
	}, "a rejected journal must reach the existing safe Preparing absence-abort")

	// Bound this fixed-code defensive regression to five seconds. Real manager
	// watches, not a direct helper, must obey the 2s/4s/... preparation gate.
	require.Never(t, func() bool { return events.Load() > 24 || intents.Load() > 3 || clears.Load() > 3 }, 5*time.Second, 25*time.Millisecond,
		"watch events must not drive an intent/abort loop under persistent quota denial")
	select {
	case event := <-watchErrors:
		t.Fatalf("Cluster watch failed: %#v", event)
	default:
	}
	t.Logf("quota-denied manager window: Cluster events=%d, status changes=%d, intents=%d, safe clears=%d", events.Load(), statusChanges.Load(), intents.Load(), clears.Load())
	require.GreaterOrEqual(t, intents.Load(), int64(2), "exercise an actual failed retry, not just a stalled controller")
	require.NoError(t, c.Get(t.Context(), sourceKey, source))
	require.Equal(t, original.UID, source.UID)
	require.True(t, source.DeletionTimestamp.IsZero(), "denied journal must never orphan the source")
	require.Equal(t, original.Spec, source.Spec)
	requireNoClaimTemplateRecoveryJournals(t, c, cluster)
	assertRetryRetainedStorage(t, c, cluster.Namespace, identity)

	// A scheduled hold must permit independently safe mutable work after the
	// caller's direct same-UID/nil-pointer fence, even while quota remains full.
	requireEventually(t, func() bool {
		return c.Get(t.Context(), clusterKey, cluster) == nil && cluster.Status.ClaimTemplateRecovery == nil
	})
	before = cluster.DeepCopy()
	cluster.Spec.Replicas = new(int32(3))
	require.NoError(t, c.Patch(t.Context(), cluster, client.MergeFrom(before)))
	requireEventually(t, func() bool {
		return c.Get(t.Context(), sourceKey, source) == nil && *source.Spec.Replicas == 3 && source.DeletionTimestamp.IsZero()
	}, "quota denial must not freeze fenced mutable scale-up")
	require.Equal(t, original.Spec.Template, source.Spec.Template)
	// The shared manager has no Raft exec client or StatefulSet controller.
	// Restore this single-Pod fixture before completing the storage operation;
	// this does not validate real Raft scale-down or creation of new Pods.
	require.NoError(t, c.Get(t.Context(), clusterKey, cluster))
	before = cluster.DeepCopy()
	cluster.Spec.Replicas = new(int32(1))
	require.NoError(t, c.Patch(t.Context(), cluster, client.MergeFrom(before)))
	requireEventually(t, func() bool {
		return c.Get(t.Context(), sourceKey, source) == nil && *source.Spec.Replicas == 1 && source.DeletionTimestamp.IsZero()
	})
	storageManagerSetConverged(t, source)
	require.NoError(t, c.Delete(t.Context(), quota))
	journal := &corev1.ConfigMap{}
	requireEventually(t, func() bool {
		if c.Get(t.Context(), clusterKey, cluster) != nil || cluster.Status.ClaimTemplateRecovery == nil || c.Get(t.Context(), sourceKey, source) != nil {
			return false
		}
		return !source.DeletionTimestamp.IsZero() && cluster.Status.ClaimTemplateRecovery.ConfigMapUID != "" &&
			c.Get(t.Context(), client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Status.ClaimTemplateRecovery.ConfigMapName}, journal) == nil
	}, "lifting quota must allow authenticated journal creation and the real orphan-delete request")
	require.Equal(t, original.UID, source.UID)
	require.True(t, metav1.IsControlledBy(journal, cluster))
	require.Equal(t, string(journal.UID), cluster.Status.ClaimTemplateRecovery.ConfigMapUID)
	require.Equal(t, claimTemplateRecoveryDigest(cluster, []byte(journal.Data[claimTemplateRecoveryKey])), cluster.Status.ClaimTemplateRecovery.PayloadSHA256)
	journalKey := client.ObjectKeyFromObject(journal)
	assertRetryRetainedStorage(t, c, cluster.Namespace, identity)

	// envtest has no GC, StatefulSet controller, CSI or kubelet. Explicitly
	// simulate orphaning, source deletion and controller adoption/observation.
	// These steps prove operator continuation, not real-controller/CSI behavior.
	pod := &corev1.Pod{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: cluster.Namespace, Name: source.Name + "-0"}, pod))
	podUID := pod.UID
	pod.OwnerReferences = nil
	require.NoError(t, c.Update(t.Context(), pod))
	beforeSource := source.DeepCopy()
	source.Finalizers = nil
	require.NoError(t, c.Patch(t.Context(), source, client.MergeFrom(beforeSource)))
	replacement := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return c.Get(t.Context(), sourceKey, replacement) == nil && replacement.UID != original.UID
	})
	require.Equal(t, resource.MustParse("200Gi"), replacement.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage])
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pod), pod))
	require.Equal(t, podUID, pod.UID)
	require.NoError(t, controllerutil.SetControllerReference(replacement, pod, c.Scheme()))
	require.NoError(t, c.Update(t.Context(), pod))
	storageManagerSetConverged(t, replacement)
	requireEventually(t, func() bool {
		return c.Get(t.Context(), clusterKey, cluster) == nil && cluster.Status.ClaimTemplateRecovery == nil && apierrors.IsNotFound(c.Get(t.Context(), journalKey, &corev1.ConfigMap{}))
	}, "normal Completed cleanup must follow simulated controller adoption")
	requireNoClaimTemplateRecovery(t, c, cluster)
	assertRetryRetainedStorage(t, c, cluster.Namespace, identity)
}

func assertRetryRetainedStorage(t *testing.T, reader client.Reader, namespace string, identity retainedClaimIdentity) {
	t.Helper()
	claim := &corev1.PersistentVolumeClaim{}
	require.NoError(t, reader.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: identity.Name}, claim))
	require.Equal(t, identity.UID, claim.UID)
	require.Equal(t, identity.VolumeName, claim.Spec.VolumeName)
	require.True(t, claim.DeletionTimestamp.IsZero())
	pv := &corev1.PersistentVolume{}
	require.NoError(t, reader.Get(t.Context(), client.ObjectKey{Name: identity.VolumeName}, pv))
	require.Equal(t, identity.PVUID, pv.UID)
	require.NotNil(t, pv.Spec.ClaimRef)
	require.Equal(t, identity.UID, pv.Spec.ClaimRef.UID)
	require.Equal(t, identity.Name, pv.Spec.ClaimRef.Name)
	require.Equal(t, namespace, pv.Spec.ClaimRef.Namespace)
	require.True(t, pv.DeletionTimestamp.IsZero())
}
