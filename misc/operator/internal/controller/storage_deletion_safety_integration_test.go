//go:build integration

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// Change the real API object after the authoritative read. The API server,
// not a mock's option inspection, must reject the stale DELETE.
func TestStorageLayoutDeletionPreconditionsIntegration(t *testing.T) {
	t.Parallel()
	for _, race := range []string{"UID replacement", "foreign owner", "metadata update"} {
		t.Run(race, func(t *testing.T) {
			t.Parallel()
			namespace := createTestNamespace(t)
			cluster := newCluster("layout", namespace)
			cluster.UID = types.UID(namespace + "-cluster")
			applyDefaults(cluster)
			cluster.Spec.Persistence.DeletionProtection = new(false)
			cluster.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{Path: "/data"}
			r := &ClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.reconcileStatefulSetWithExec(ctx, cluster, computeSpecHash(&cluster.Spec), nil, nil)
			require.NoError(t, err)
			key := client.ObjectKey{Namespace: namespace, Name: resourceName(cluster.Name)}
			source := &appsv1.StatefulSet{}
			require.NoError(t, k8sClient.Get(ctx, key, source))
			cluster.Spec.Persistence.Data.HostPath = nil
			attempts := 0
			var changed *appsv1.StatefulSet
			withWatch, err := client.NewWithWatch(testEnv.Config, client.Options{Scheme: k8sClient.Scheme()})
			require.NoError(t, err)
			c := interceptor.NewClient(withWatch, interceptor.Funcs{Delete: func(ctx context.Context, delegate client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				attempts++
				if attempts > 1 {
					return delegate.Delete(ctx, obj, opts...)
				}
				live := &appsv1.StatefulSet{}
				require.NoError(t, delegate.Get(ctx, key, live))
				switch race {
				case "UID replacement":
					require.NoError(t, delegate.Delete(ctx, live, client.PropagationPolicy(metav1.DeletePropagationBackground)))
					replacement := source.DeepCopy()
					replacement.UID, replacement.ResourceVersion = "", ""
					replacement.OwnerReferences[0].UID = "foreign"
					require.NoError(t, delegate.Create(ctx, replacement))
				case "foreign owner":
					live.OwnerReferences[0].UID = "foreign"
					require.NoError(t, delegate.Update(ctx, live))
				case "metadata update":
					live.Labels["concurrent-update"] = "preserved"
					require.NoError(t, delegate.Update(ctx, live))
				}
				changed = &appsv1.StatefulSet{}
				require.NoError(t, delegate.Get(ctx, key, changed))
				return delegate.Delete(ctx, obj, opts...)
			}})
			r.Client = c
			_, err = r.reconcileStatefulSetWithExec(ctx, cluster, computeSpecHash(&cluster.Spec), nil, nil)
			require.True(t, apierrors.IsConflict(err), "the real API must reject the stale delete: %v", err)
			require.Equal(t, 1, attempts)
			got := &appsv1.StatefulSet{}
			require.NoError(t, k8sClient.Get(ctx, key, got))
			require.Nil(t, got.DeletionTimestamp)
			require.Equal(t, changed.UID, got.UID)
			require.Equal(t, changed.OwnerReferences, got.OwnerReferences)
			require.Equal(t, changed.Spec, got.Spec)
			if race == "metadata update" {
				// A fresh observation can safely retry the benign update race.
				r = &ClusterReconciler{Client: c, Scheme: k8sClient.Scheme()}
				_, err = r.reconcileStatefulSetWithExec(ctx, cluster, computeSpecHash(&cluster.Spec), nil, nil)
				require.NoError(t, err)
				require.Equal(t, 2, attempts, "the fresh retry must issue the guarded DELETE")
				final := &appsv1.StatefulSet{}
				err = k8sClient.Get(ctx, key, final)
				if !apierrors.IsNotFound(err) {
					require.NoError(t, err)
					require.Equal(t, changed.UID, final.UID)
					require.NotNil(t, final.DeletionTimestamp, "the API must accept deletion on the successful retry")
				}
			}
		})
	}
}
