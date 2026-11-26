//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// This simulates the released writer's API effects with inert annotations. It
// proves manager ordering and rollout requests, without running IAM admission,
// a released operator binary, or the StatefulSet controller.
func TestCanonicalHashBindingReleasedWriterThroughManagerIntegration(t *testing.T) {
	t.Parallel()
	cluster, source, _, identity := storageManagerFixture(t, false, false)
	canonicalA := source.Annotations[annotationPodConfigHash]
	require.True(t, hasPodConfigHashBinding(source))
	priorB := cluster.DeepCopy()
	applyDefaults(priorB)
	priorB.Spec.ServiceAccount.Annotations = map[string]string{"fixture.example/admission": "B"}
	installedB := computePodSpecHash(&priorB.Spec, false)
	account := &corev1.ServiceAccount{}
	accountKey := client.ObjectKey{Namespace: cluster.Namespace, Name: serviceAccountName(cluster)}
	require.NoError(t, k8sClient.Get(ctx, accountKey, account))
	account.Annotations = priorB.Spec.ServiceAccount.Annotations
	require.NoError(t, k8sClient.Update(ctx, account))
	source.Spec.Template.Annotations[annotationSpecHash] = installedB
	// Unknown A metadata survives the released operator's B write.
	require.NoError(t, k8sClient.Update(ctx, source))
	storageManagerSetConverged(t, source)
	require.Equal(t, canonicalA, source.Annotations[annotationPodConfigHash])
	require.NotEqual(t, installedB, source.Annotations[annotationPodConfigSpecHash])
	original := source.DeepCopy()
	before := cluster.DeepCopy()
	cluster.Spec.Replicas = new(int32(1))
	require.Empty(t, cluster.Spec.ServiceAccount.Annotations)
	require.NoError(t, k8sClient.Patch(ctx, cluster, client.MergeFrom(before)))
	requireEventually(t, func() bool {
		if k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster) != nil {
			return false
		}
		condition := meta.FindStatusCondition(cluster.Status.Conditions, "ClaimTemplatesReady")
		return condition != nil && condition.Reason == "HashMigrationRequired" && condition.ObservedGeneration == cluster.Generation
	}, "A metadata left by the released B writer must require historical proof")
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(source), source))
	require.Equal(t, original.Spec, source.Spec)
	require.Equal(t, original.Generation, source.Generation)
	require.Equal(t, original.Annotations, source.Annotations)
	requireNoClaimTemplateRecovery(t, k8sClient, cluster)
	before = cluster.DeepCopy()
	installLegacySpecHashProof(t, cluster, source, &priorB.Spec)
	require.NoError(t, k8sClient.Patch(ctx, cluster, client.MergeFrom(before)))
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, client.ObjectKeyFromObject(source), source) == nil &&
			source.Spec.Template.Annotations[annotationSpecHash] == canonicalA &&
			source.Annotations[annotationPodConfigHash] == canonicalA && hasPodConfigHashBinding(source)
	}, "authentic B proof must request an actual A rollout and bind the installed A hash")
	require.Greater(t, source.Generation, original.Generation)
	require.Equal(t, original.UID, source.UID)
	require.Equal(t, original.Spec.VolumeClaimTemplates, source.Spec.VolumeClaimTemplates)
	require.True(t, source.DeletionTimestamp.IsZero())
	requireNoClaimTemplateRecovery(t, k8sClient, cluster)
	claim := &corev1.PersistentVolumeClaim{}
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: identity.Name}, claim))
	require.Equal(t, identity.UID, claim.UID)
	require.Equal(t, identity.VolumeName, claim.Spec.VolumeName)
	pv := &corev1.PersistentVolume{}
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Name: identity.VolumeName}, pv))
	require.Equal(t, identity.PVUID, pv.UID)
	require.Equal(t, identity.UID, pv.Spec.ClaimRef.UID)
	// Observe a second real manager pass after the pair has been written.
	generation, template := source.Generation, source.Spec.Template.DeepCopy()
	storageManagerSetConverged(t, source)
	requireEventually(t, func() bool {
		if k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster) != nil {
			return false
		}
		condition := meta.FindStatusCondition(cluster.Status.Conditions, "ClaimTemplatesReady")
		return condition != nil && condition.Reason == "Ready" && condition.ObservedGeneration == cluster.Generation
	}, "the valid pair ignores the leftover B proof on the next pass")
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(source), source))
	require.Equal(t, generation, source.Generation)
	require.Equal(t, *template, source.Spec.Template)
}
