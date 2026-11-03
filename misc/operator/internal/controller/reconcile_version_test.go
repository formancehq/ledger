package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestReconcile_RejectsNewerLedgerBeforeWorkloadCreation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cluster := newMinimalCluster()
	cluster.Spec.Image.Tag = "v3.0.0-rc.2"
	replicas := int32(3)
	cluster.Spec.Replicas = &replicas
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme, OperatorVersion: "v3.0.0-rc.1"}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, cluster))
	condition := meta.FindStatusCondition(cluster.Status.Conditions, "VersionCompatible")
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "LedgerVersionAheadOfOperator", condition.Reason)
	require.Equal(t, "Error", cluster.Status.Phase)
	sts := &appsv1.StatefulSet{}
	err = c.Get(ctx, types.NamespacedName{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}, sts)
	require.True(t, apierrors.IsNotFound(err), "newer image must not create a StatefulSet: %v", err)
}

func TestReconcile_RejectsNewerLedgerWithoutChangingExistingWorkload(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cluster := newMinimalCluster()
	cluster.Spec.Image.Tag = "v3.0.0-rc.2"
	replicas := int32(3)
	cluster.Spec.Replicas = &replicas
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName(cluster.Name), Namespace: cluster.Namespace},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ledger", Image: "ghcr.io/formancehq/ledger:v3.0.0-rc.1"}}}},
		},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: replicas},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}).WithObjects(cluster, sts).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme, OperatorVersion: "v3.0.0-rc.1"}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	stsKey := types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}
	before := sts.DeepCopy()
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, stsKey, sts))
	require.Equal(t, before.Spec, sts.Spec)
	require.Equal(t, before.ResourceVersion, sts.ResourceVersion)
	require.NoError(t, c.Get(ctx, key, cluster))
	ready := meta.FindStatusCondition(cluster.Status.Conditions, "Ready")
	require.NotNil(t, ready)
	require.Equal(t, "LedgerVersionAheadOfOperator", ready.Reason)
}
