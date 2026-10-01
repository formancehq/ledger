package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

type rejectClusterIDUpdate struct{ client.Client }

func (c rejectClusterIDUpdate) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return errors.New("identity write rejected")
}

func TestClusterIDCommittedBeforeWorkloads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	cluster := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "ns"}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	request := reconcile.Request{NamespacedName: key}

	_, err := (&ClusterReconciler{Client: rejectClusterIDUpdate{base}, Scheme: scheme}).Reconcile(ctx, request)
	require.ErrorContains(t, err, "identity write rejected")
	require.NoError(t, base.Get(ctx, key, cluster))
	require.Empty(t, cluster.Spec.ClusterID)
	require.Error(t, base.Get(ctx, types.NamespacedName{Name: "ledger-cluster", Namespace: "ns"}, &appsv1.StatefulSet{}))
}
