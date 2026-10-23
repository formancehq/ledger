//go:build integration

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestStorageJournalTrustStatusCASIntegration(t *testing.T) {
	t.Parallel()
	// No manager runs in this API server. These assertions prove CRD/status
	// preservation and resourceVersion conflicts, not workload controllers or RBAC.
	isolated := &envtest.Environment{CRDDirectoryPaths: testEnv.CRDDirectoryPaths, Scheme: k8sClient.Scheme()}
	config, err := isolated.Start()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, isolated.Stop()) })
	c, err := client.NewWithWatch(config, client.Options{Scheme: isolated.Scheme})
	require.NoError(t, err)
	namespace := "storage-journal-trust"
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}))

	for _, boundary := range []string{"binder wins", "abandon wins", "completed clear loses", "optimistic resourceVersion", "initial generation changes"} {
		t.Run(boundary, func(t *testing.T) {
			cluster := newCluster(strings.ToLower(strings.ReplaceAll(boundary, " ", "-")), namespace)
			require.NoError(t, c.Create(t.Context(), cluster))
			r := &ClusterReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
			read := func() *ledgerv1alpha1.Cluster {
				latest := &ledgerv1alpha1.Cluster{}
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), latest))
				return latest
			}
			preparing := &ledgerv1alpha1.ClaimTemplateRecoveryStatus{
				SourceUID: "synthetic-source", PayloadSHA256: strings.Repeat("a", 64),
			}
			preparing.ConfigMapName = claimTemplateRecoveryDigestName(cluster, preparing.PayloadSHA256)
			if boundary == "initial generation changes" {
				changed := read()
				changed.Spec.PodAnnotations = map[string]string{"fixture-note": "new-plan"}
				require.NoError(t, c.Update(t.Context(), changed))
				require.Greater(t, changed.Generation, cluster.Generation)
				err := r.persistClaimTemplateRecovery(t.Context(), cluster, nil, preparing)
				require.ErrorContains(t, err, "generation")
				require.Nil(t, read().Status.ClaimTemplateRecovery)
				return
			}
			require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), cluster, nil, preparing))
			stored := read()
			require.Equal(t, preparing, stored.Status.ClaimTemplateRecovery,
				"the installed CRD must preserve source UID, full digest, derived name and an absent ConfigMap UID")
			require.Empty(t, stored.Status.ClaimTemplateRecovery.ConfigMapUID)
			require.False(t, stored.Status.ClaimTemplateRecovery.Completed)
			bound := preparing.DeepCopy()
			bound.ConfigMapUID = "synthetic-journal-uid"
			switch boundary {
			case "binder wins":
				require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), read(), preparing, bound))
				err := r.persistClaimTemplateRecovery(t.Context(), stored, preparing, nil)
				require.Error(t, err)
				require.Equal(t, bound, read().Status.ClaimTemplateRecovery)
			case "abandon wins":
				require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), read(), preparing, nil))
				err := r.persistClaimTemplateRecovery(t.Context(), stored, preparing, bound)
				require.Error(t, err)
				require.Nil(t, read().Status.ClaimTemplateRecovery)
			case "completed clear loses":
				completed := bound.DeepCopy()
				completed.Completed = true
				require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), read(), preparing, completed))
				staleCompleted := read()
				require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), read(), completed, nil))
				next := preparing.DeepCopy()
				next.PayloadSHA256 = strings.Repeat("b", 64)
				next.ConfigMapName = claimTemplateRecoveryDigestName(cluster, next.PayloadSHA256)
				require.NoError(t, r.persistClaimTemplateRecovery(t.Context(), read(), nil, next))
				err := r.persistClaimTemplateRecovery(t.Context(), staleCompleted, completed, nil)
				require.Error(t, err)
				require.Equal(t, next, read().Status.ClaimTemplateRecovery)
			case "optimistic resourceVersion":
				attempts := 0
				wrapped := interceptor.NewClient(c, interceptor.Funcs{
					SubResourcePatch: func(ctx context.Context, inner client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
						attempts++
						// Interpose a real concurrent status writer after the helper's
						// direct read and before its optimistic patch reaches the API.
						newer := read()
						newer.Status.ClaimTemplateRecovery = bound.DeepCopy()
						newer.Status.Conditions = []metav1.Condition{{
							Type: "FixtureObservation", Status: metav1.ConditionTrue,
							Reason: "ConcurrentWriter", Message: "preserve this observation",
							LastTransitionTime: metav1.Now(),
						}}
						if err := c.Status().Update(ctx, newer); err != nil {
							return err
						}
						return inner.SubResource(sub).Patch(ctx, obj, patch, opts...)
					},
				})
				staleWriter := &ClusterReconciler{Client: wrapped, APIReader: c, Scheme: c.Scheme()}
				err := staleWriter.persistClaimTemplateRecovery(t.Context(), stored, preparing, nil)
				require.True(t, apierrors.IsConflict(err), "the real API must reject the stale resourceVersion: %v", err)
				_, fenced := errors.AsType[*claimTemplateJournalWriteError](err)
				require.True(t, fenced)
				require.Equal(t, 1, attempts, "conflicts must be observed again in another reconciliation pass")
				latest := read()
				require.Equal(t, bound, latest.Status.ClaimTemplateRecovery)
				require.Len(t, latest.Status.Conditions, 1)
				require.Equal(t, "FixtureObservation", latest.Status.Conditions[0].Type)
			}
		})
	}
}
