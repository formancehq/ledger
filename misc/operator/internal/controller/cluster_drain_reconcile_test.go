package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestAnnotatedDrainPersistsEpochAndReleaseIntentAcrossReconcile(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "annotated", Namespace: "test", UID: "annotated-uid", Annotations: map[string]string{ClusterDrainAnnotation: "true"}},
		Spec:       ledgerv1alpha1.ClusterSpec{Replicas: new(int32(1)), GrpcPort: 8888},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName(cluster.Name), Namespace: cluster.Namespace},
		Spec:       appsv1.StatefulSetSpec{Replicas: new(int32(1)), Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ledger", Ports: []corev1.ContainerPort{{Name: "grpc", ContainerPort: 8888, Protocol: corev1.ProtocolTCP}}, Env: []corev1.EnvVar{{Name: "TLS_MODE", Value: tlsModeDisabled}}}}}}},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}
	controlledByCluster(sts, cluster)
	r := &ClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster, sts).WithObjects(cluster, sts).Build()}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	load := func() *ledgerv1alpha1.Cluster {
		fresh := &ledgerv1alpha1.Cluster{}
		require.NoError(t, r.Get(t.Context(), key, fresh))

		return fresh
	}
	preflight := func(context.Context, *ledgerv1alpha1.Cluster, string) error { return nil }
	barrierCalls, releaseCalls := 0, 0
	barrier := func(ctx context.Context, c *ledgerv1alpha1.Cluster, epoch, tlsMode string) (bool, error) {
		barrierCalls++
		require.Equal(t, tlsModeDisabled, tlsMode)
		require.Equal(t, int32(8888), c.Spec.GrpcPort)
		require.Equal(t, int32(1), *c.Spec.Replicas)
		persisted := load()
		require.Equal(t, epoch, persisted.Status.Drain.Epoch, "epoch must be durable before BeginDrain")

		return true, nil
	}
	release := func(ctx context.Context, c *ledgerv1alpha1.Cluster, epoch, tlsMode string) (bool, error) {
		releaseCalls++
		require.Equal(t, int32(8888), c.Spec.GrpcPort, "release must use the live Pod port")
		require.Equal(t, int32(1), *c.Spec.Replicas, "leader discovery must use the live replica count")
		persisted := load()
		require.Equal(t, drainPhaseReleasing, persisted.Status.Drain.Phase, "release intent must be durable before EndDrain")
		require.Equal(t, epoch, persisted.Status.Drain.Epoch)

		return true, nil
	}

	first := load()
	result, err := r.reconcileAnnotatedDrain(t.Context(), first, preflight, barrier, release)
	require.NoError(t, err)
	require.True(t, result.Requeue)
	require.Zero(t, barrierCalls)
	first = load()
	firstEpoch := first.Status.Drain.Epoch
	require.NotEmpty(t, firstEpoch)
	require.Equal(t, drainPhasePreflighting, first.Status.Drain.Phase)

	result, err = r.reconcileAnnotatedDrain(t.Context(), first, preflight, barrier, release)
	require.NoError(t, err)
	require.True(t, result.Requeue)
	require.Zero(t, barrierCalls)
	require.Equal(t, drainPhaseDraining, load().Status.Drain.Phase)

	result, err = r.reconcileAnnotatedDrain(t.Context(), load(), preflight, barrier, release)
	require.NoError(t, err)
	require.False(t, result.Requeue)
	require.Equal(t, drainPhaseDrained, load().Status.Drain.Phase)
	require.Equal(t, 1, barrierCalls)
	require.Equal(t, drainPhaseDrained, load().Status.Drain.Phase)

	removed := load()
	delete(removed.Annotations, ClusterDrainAnnotation)
	removed.Spec.GrpcPort = 9999
	removed.Spec.Replicas = new(int32(0))
	require.NoError(t, r.Update(t.Context(), removed))
	removed = load()
	result, err = r.reconcileAnnotatedDrain(t.Context(), removed, preflight, barrier, release)
	require.NoError(t, err)
	require.True(t, result.Requeue)
	require.Zero(t, releaseCalls)
	require.Equal(t, drainPhaseReleasing, load().Status.Drain.Phase)

	result, err = r.reconcileAnnotatedDrain(t.Context(), load(), preflight, barrier, release)
	require.NoError(t, err)
	require.False(t, result.Requeue)
	require.Equal(t, 1, releaseCalls)
	require.Nil(t, load().Status.Drain)

	readded := load()
	readded.Annotations = map[string]string{ClusterDrainAnnotation: "true"}
	require.NoError(t, r.Update(t.Context(), readded))
	result, err = r.reconcileAnnotatedDrain(t.Context(), load(), preflight, barrier, release)
	require.NoError(t, err)
	require.True(t, result.Requeue)
	require.NotEqual(t, firstEpoch, load().Status.Drain.Epoch)
}

func TestAnnotatedDrainCancellationAfterFailedPreflight(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "unsupported", Namespace: "test", UID: "unsupported-uid", Annotations: map[string]string{ClusterDrainAnnotation: "true"}},
		Spec:       ledgerv1alpha1.ClusterSpec{Replicas: new(int32(1)), GrpcPort: 8888},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName(cluster.Name), Namespace: cluster.Namespace},
		Spec:       appsv1.StatefulSetSpec{Replicas: new(int32(1)), Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ledger", Ports: []corev1.ContainerPort{{Name: "grpc", ContainerPort: 8888, Protocol: corev1.ProtocolTCP}}}}}}},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}
	controlledByCluster(sts, cluster)
	r := &ClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster, sts).WithObjects(cluster, sts).Build()}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	load := func() *ledgerv1alpha1.Cluster {
		fresh := &ledgerv1alpha1.Cluster{}
		require.NoError(t, r.Get(t.Context(), key, fresh))

		return fresh
	}
	preflight := func(context.Context, *ledgerv1alpha1.Cluster, string) error {
		return errors.New("member lacks drain support")
	}
	barrier := func(context.Context, *ledgerv1alpha1.Cluster, string, string) (bool, error) {
		t.Fatal("BeginDrain must not run before preflight succeeds")

		return false, nil
	}
	release := func(context.Context, *ledgerv1alpha1.Cluster, string, string) (bool, error) {
		t.Fatal("EndDrain must not run when no marker was proposed")

		return false, nil
	}
	_, err := r.reconcileAnnotatedDrain(t.Context(), load(), preflight, barrier, release)
	require.NoError(t, err)
	require.Equal(t, drainPhasePreflighting, load().Status.Drain.Phase)
	_, err = r.reconcileAnnotatedDrain(t.Context(), load(), preflight, barrier, release)
	require.ErrorContains(t, err, "member lacks drain support")
	removed := load()
	delete(removed.Annotations, ClusterDrainAnnotation)
	require.NoError(t, r.Update(t.Context(), removed))
	result, err := r.reconcileAnnotatedDrain(t.Context(), load(), preflight, barrier, release)
	require.NoError(t, err)
	require.True(t, result.Requeue)
	require.Nil(t, load().Status.Drain)
}

func TestAnnotatedDrainReaddedDuringReleaseStartsNewEpoch(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "readded", Namespace: "test", UID: "readded-uid", Annotations: map[string]string{ClusterDrainAnnotation: "true"}},
		Spec:       ledgerv1alpha1.ClusterSpec{Replicas: new(int32(1)), GrpcPort: 8888},
		Status:     ledgerv1alpha1.ClusterStatus{Drain: &ledgerv1alpha1.DrainStatus{Epoch: "old-epoch", Phase: drainPhaseReleasing}},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName(cluster.Name), Namespace: cluster.Namespace},
		Spec:       appsv1.StatefulSetSpec{Replicas: new(int32(1)), Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ledger", Ports: []corev1.ContainerPort{{Name: "grpc", ContainerPort: 8888, Protocol: corev1.ProtocolTCP}}}}}}},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}
	controlledByCluster(sts, cluster)
	r := &ClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster, sts).WithObjects(cluster, sts).Build()}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	preflight := func(context.Context, *ledgerv1alpha1.Cluster, string) error { return nil }
	barrierCalls, releaseCalls := 0, 0
	barrier := func(context.Context, *ledgerv1alpha1.Cluster, string, string) (bool, error) {
		barrierCalls++

		return true, nil
	}
	release := func(_ context.Context, _ *ledgerv1alpha1.Cluster, epoch, _ string) (bool, error) {
		releaseCalls++
		require.Equal(t, "old-epoch", epoch)

		return true, nil
	}
	result, err := r.reconcileAnnotatedDrain(t.Context(), cluster, preflight, barrier, release)
	require.NoError(t, err)
	require.True(t, result.Requeue)
	require.Equal(t, 1, releaseCalls)
	require.Zero(t, barrierCalls)
	fresh := &ledgerv1alpha1.Cluster{}
	require.NoError(t, r.Get(t.Context(), key, fresh))
	require.Equal(t, drainPhasePreflighting, fresh.Status.Drain.Phase)
	require.NotEqual(t, "old-epoch", fresh.Status.Drain.Epoch)
	_, err = r.reconcileAnnotatedDrain(t.Context(), fresh, preflight, barrier, release)
	require.NoError(t, err)
	require.Zero(t, barrierCalls)
	fresh = &ledgerv1alpha1.Cluster{}
	require.NoError(t, r.Get(t.Context(), key, fresh))
	_, err = r.reconcileAnnotatedDrain(t.Context(), fresh, preflight, barrier, release)
	require.NoError(t, err)
	require.Equal(t, 1, barrierCalls)
}
