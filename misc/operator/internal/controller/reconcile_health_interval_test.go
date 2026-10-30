package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestReconcile_RejectsInvalidHealthInterval(t *testing.T) {
	t.Parallel()

	for _, interval := range []string{"0s", "-1s", "invalid", "999999999999999999h", "0.1ns"} {
		t.Run(interval, func(t *testing.T) {
			t.Parallel()
			cluster := newMinimalCluster()
			replicas := int32(3)
			cluster.Spec.Replicas = &replicas
			cluster.Spec.Health = &ledgerv1alpha1.HealthConfig{Interval: interval}
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&ledgerv1alpha1.Cluster{}).WithObjects(cluster).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme}
			key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}

			result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
			require.Zero(t, result)
			require.NoError(t, c.Get(context.Background(), key, cluster))
			condition := meta.FindStatusCondition(cluster.Status.Conditions, "ConfigValid")
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Equal(t, "ValidationFailed", condition.Reason)
			require.Contains(t, condition.Message, "health.interval")
			require.Equal(t, "Error", cluster.Status.Phase)

			sts := &appsv1.StatefulSet{}
			err = c.Get(context.Background(), types.NamespacedName{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}, sts)
			require.True(t, apierrors.IsNotFound(err), "invalid interval must prevent workload creation: %v", err)
		})
	}
}

func TestReconcile_HealthIntervalPreservesWorkloadAndRecovers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := newMinimalCluster()
	replicas := int32(3)
	cluster.Spec.Replicas = &replicas
	cluster.Spec.Health = &ledgerv1alpha1.HealthConfig{Interval: "30s"}
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	request := ctrl.Request{NamespacedName: key}
	stsKey := types.NamespacedName{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}

	_, err := r.Reconcile(ctx, request)
	require.NoError(t, err)
	before := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, stsKey, before))
	assertEnv(t, before.Spec.Template.Spec.Containers[0].Env, "HEALTH_CHECK_INTERVAL", "30s")

	require.NoError(t, c.Get(ctx, key, cluster))
	cluster.Spec.Health.Interval = "0s"
	require.NoError(t, c.Update(ctx, cluster))
	result, err := r.Reconcile(ctx, request)
	require.NoError(t, err)
	require.Zero(t, result)
	after := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, stsKey, after))
	require.Equal(t, before.Spec, after.Spec, "invalid edit must preserve existing workload")
	require.Equal(t, before.ResourceVersion, after.ResourceVersion, "invalid edit must not update the workload")
	require.NoError(t, c.Get(ctx, key, cluster))
	condition := meta.FindStatusCondition(cluster.Status.Conditions, "ConfigValid")
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Contains(t, condition.Message, "health.interval")
	require.Equal(t, "Error", cluster.Status.Phase)

	cluster.Spec.Health.Interval = "1ns"
	require.NoError(t, c.Update(ctx, cluster))
	_, err = r.Reconcile(ctx, request)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, cluster))
	condition = meta.FindStatusCondition(cluster.Status.Conditions, "ConfigValid")
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionTrue, condition.Status)
	require.Equal(t, "Valid", condition.Reason)
	require.NotEqual(t, "Error", cluster.Status.Phase)
	require.NoError(t, c.Get(ctx, stsKey, after))
	assertEnv(t, after.Spec.Template.Spec.Containers[0].Env, "HEALTH_CHECK_INTERVAL", "1ns")
}
