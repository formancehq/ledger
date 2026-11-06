//go:build integration

package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestStorageReconstructedHashAdmissionRetriesWithAPIDefaultsIntegration(t *testing.T) {
	t.Parallel()
	cluster, source, _, identity := storageManagerFixture(t, true, false)
	oldHash := source.Spec.Template.Annotations[annotationSpecHash]
	apiClient, err := client.NewWithWatch(testEnv.Config, client.Options{Scheme: k8sClient.Scheme()})
	require.NoError(t, err)
	// The shared manager remains paused on the fixture's invalid two replicas.
	// Drive only this StatefulSet pass with its original effective declaration.
	effective := cluster.DeepCopy()
	effective.Spec.Replicas = new(int32(1))
	applyDefaults(effective)
	require.True(t, legacyStorageSpecHashMatches(effective, source))
	require.Empty(t, source.Annotations[annotationPodConfigHash])
	require.Empty(t, source.Annotations[annotationPodConfigSpecHash])
	require.EqualValues(t, 1, source.Spec.Template.Spec.Containers[0].LivenessProbe.TimeoutSeconds,
		"exercise the API-defaulted template, not fake-client equality")
	before := source.DeepCopy()
	previews, writes := 0, 0
	denied := errors.New("temporary StatefulSet admission preview failure")
	c := interceptor.NewClient(apiClient, interceptor.Funcs{
		Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); ok {
				var options client.UpdateOptions
				options.ApplyOptions(opts)
				if len(options.DryRun) != 0 {
					previews++
					if previews == 1 {
						return denied
					}
				} else {
					writes++
				}
			}

			return inner.Update(ctx, obj, opts...)
		},
	})
	r := &ClusterReconciler{Client: c, APIReader: k8sClient, Scheme: k8sClient.Scheme()}
	canonical := computeSpecHash(&effective.Spec)
	_, err = r.reconcileStatefulSet(t.Context(), effective, canonical, nil)
	require.ErrorIs(t, err, denied)
	require.Zero(t, writes)
	condition := meta.FindStatusCondition(effective.Status.Conditions, "ClaimTemplatesReady")
	require.NotNil(t, condition)
	require.Equal(t, "AdmissionCheckFailed", condition.Reason)
	var live appsv1.StatefulSet
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(source), &live))
	require.Equal(t, before, &live)
	requireNoClaimTemplateRecovery(t, k8sClient, cluster)
	_, err = r.reconcileStatefulSet(t.Context(), effective, canonical, nil)
	require.NoError(t, err)
	require.Equal(t, 2, previews)
	require.Equal(t, 1, writes)
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(source), &live))
	require.Equal(t, before.UID, live.UID)
	require.Equal(t, before.Generation, live.Generation, "metadata-only certification must not request a Pod rollout")
	require.Equal(t, before.Spec, live.Spec, "API defaults and the complete installed template remain intact")
	require.Equal(t, oldHash, live.Spec.Template.Annotations[annotationSpecHash])
	require.Equal(t, canonical, live.Annotations[annotationPodConfigHash])
	require.Equal(t, oldHash, live.Annotations[annotationPodConfigSpecHash])
	require.True(t, hasPodConfigHashBinding(&live))
	requireNoClaimTemplateRecovery(t, k8sClient, cluster)
	assertRetryRetainedStorage(t, k8sClient, cluster.Namespace, identity)
	// The injected first error proves control-flow ordering. The successful
	// retry uses real StatefulSet UPDATE dry-run/defaulting; envtest does not
	// exercise a Pod CREATE webhook, kubelet or actual StatefulSet rollout.
}
