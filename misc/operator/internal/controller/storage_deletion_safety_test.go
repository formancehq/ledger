package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestStorageLayoutDeletionRequiresCurrentOwnedIdentity(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"owned", "foreign cached owner", "foreign live owner", "replaced UID", "changed resourceVersion", "terminating live source"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			cluster := newMinimalCluster()
			cluster.UID = "cluster"
			applyDefaults(cluster)
			old := cluster.DeepCopy()
			old.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{Path: "/data"}
			hash := computeSpecHash(&old.Spec)
			source := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
				Name: resourceName(cluster.Name), Namespace: cluster.Namespace, UID: "source", ResourceVersion: "10", CreationTimestamp: metav1.Now(),
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cluster, ledgerv1alpha1.GroupVersion.WithKind("Cluster"))},
			}, Spec: buildStatefulSetSpec(old, hash, nil, tlsModeDisabled)}
			recordPodConfigHash(source, hash)
			if scenario == "foreign cached owner" {
				source.OwnerReferences[0].UID = "foreign"
			}
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, source).Build()
			reads, deletes := 0, 0
			reader := interceptor.NewClient(c, interceptor.Funcs{Get: func(ctx context.Context, delegate client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := delegate.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if live, ok := obj.(*appsv1.StatefulSet); ok {
					reads++
					switch scenario {
					case "foreign live owner":
						live.OwnerReferences[0].UID = "foreign"
					case "replaced UID":
						live.UID = "replacement"
					case "changed resourceVersion":
						live.ResourceVersion = "11"
					case "terminating live source":
						now := metav1.Now()
						live.DeletionTimestamp = &now
					}
				}

				return nil
			}})
			writer := interceptor.NewClient(c, interceptor.Funcs{Delete: func(ctx context.Context, delegate client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				deletes++
				if scenario != "owned" {
					return errors.New("unexpected StatefulSet delete")
				}
				options := (&client.DeleteOptions{}).ApplyOptions(opts)
				require.NotNil(t, options.Preconditions, "the API must fence a race after the final direct read")
				require.Equal(t, source.UID, *options.Preconditions.UID)
				require.Equal(t, source.ResourceVersion, *options.Preconditions.ResourceVersion)
				require.Equal(t, metav1.DeletePropagationOrphan, *options.PropagationPolicy)

				return delegate.Delete(ctx, obj, opts...)
			}})
			r := &ClusterReconciler{Client: writer, APIReader: reader, Scheme: scheme}
			_, err := r.reconcileStatefulSetWithExec(t.Context(), cluster, computeSpecHash(&cluster.Spec), nil, nil)
			if scenario == "owned" {
				require.NoError(t, err)
				require.Equal(t, 1, reads)
				require.Equal(t, 1, deletes)
			} else {
				require.ErrorContains(t, err, "StatefulSet")
				require.Zero(t, deletes)
				var got appsv1.StatefulSet
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(source), &got))
				require.Equal(t, source.UID, got.UID)
				require.Equal(t, source.Spec, got.Spec)
			}
		})
	}
}
