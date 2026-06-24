//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestReconcile_MinimalLedgerService(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newLedgerService("basic", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for StatefulSet to appear (proves reconciliation ran)
	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-basic", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	// ServiceAccount
	sa := &corev1.ServiceAccount{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-basic", Namespace: ns}, sa))
	assert.Equal(t, "ledger-operator", sa.Labels[labelManagedBy])
	requireOwnerRef(t, sa.OwnerReferences, "basic")

	// Headless Service
	hlsSvc := &corev1.Service{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-basic-headless", Namespace: ns}, hlsSvc))
	assert.Equal(t, corev1.ClusterIPNone, hlsSvc.Spec.ClusterIP)
	assert.True(t, hlsSvc.Spec.PublishNotReadyAddresses)
	requireOwnerRef(t, hlsSvc.OwnerReferences, "basic")

	// Check headless service ports
	requirePort(t, hlsSvc.Spec.Ports, "raft", 7777)
	requirePort(t, hlsSvc.Spec.Ports, "grpc", 8888)
	requirePort(t, hlsSvc.Spec.Ports, "http", 9000)

	// ClusterIP Service
	svc := &corev1.Service{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-basic", Namespace: ns}, svc))
	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
	requireOwnerRef(t, svc.OwnerReferences, "basic")
	requirePort(t, svc.Spec.Ports, "http", 9000)
	requirePort(t, svc.Spec.Ports, "grpc", 8888)

	// StatefulSet details
	assert.Equal(t, int32(3), *sts.Spec.Replicas)
	assert.Equal(t, appsv1.OrderedReadyPodManagement, sts.Spec.PodManagementPolicy)
	assert.Equal(t, "ledger-basic-headless", sts.Spec.ServiceName)
	requireOwnerRef(t, sts.OwnerReferences, "basic")

	// Container image defaults
	container := sts.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "ghcr.io/formancehq/ledger:latest", container.Image)
	assert.Equal(t, corev1.PullAlways, container.ImagePullPolicy) // latest → PullAlways

	// Selector labels
	assert.Equal(t, "ledger", sts.Spec.Selector.MatchLabels[labelName])
	assert.Equal(t, "basic", sts.Spec.Selector.MatchLabels[labelInstance])

	// VolumeClaimTemplates
	require.Len(t, sts.Spec.VolumeClaimTemplates, 3)
	assert.Equal(t, "wal", sts.Spec.VolumeClaimTemplates[0].Name)
	assert.Equal(t, "data", sts.Spec.VolumeClaimTemplates[1].Name)
	assert.Equal(t, "cold-cache", sts.Spec.VolumeClaimTemplates[2].Name)

	// Volume mounts
	requireVolumeMount(t, container.VolumeMounts, "wal", "/data/raft")
	requireVolumeMount(t, container.VolumeMounts, "data", "/data/app")
	requireVolumeMount(t, container.VolumeMounts, "cold-cache", "/data/cold-cache")

	// Container ports
	requireContainerPort(t, container.Ports, "http", 9000)
	requireContainerPort(t, container.Ports, "grpc", 8888)
	requireContainerPort(t, container.Ports, "raft", 7777)

	// Env vars
	requireEnvVar(t, container.Env, "BIND_ADDR", "0.0.0.0:7777")
	requireEnvVar(t, container.Env, "GRPC_PORT", "8888")
	requireEnvVar(t, container.Env, "HTTP_PORT", "9000")
	requireEnvVar(t, container.Env, "CLUSTER_ID", "default")
}

func TestReconcile_GrpcServiceNotCreatedByDefault(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newLedgerService("no-grpc", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for main service to appear
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-no-grpc", Namespace: ns}, &corev1.Service{}) == nil
	}, "Service should be created")

	// gRPC service should NOT exist
	grpcSvc := &corev1.Service{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-no-grpc-grpc", Namespace: ns}, grpcSvc)
	assert.Error(t, err, "gRPC service should not be created when ingressGrpc is nil")
}

func TestReconcile_StatefulSetSpec(t *testing.T) {
	ns := createTestNamespace(t)
	replicas := int32(5)
	ls := newLedgerService("sts-spec", ns)
	ls.Spec.Replicas = &replicas
	require.NoError(t, k8sClient.Create(ctx, ls))

	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-sts-spec", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	assert.Equal(t, int32(5), *sts.Spec.Replicas)
	assert.Equal(t, appsv1.OrderedReadyPodManagement, sts.Spec.PodManagementPolicy)
	assert.Equal(t, "ledger-sts-spec-headless", sts.Spec.ServiceName)

	// Retention policy defaults to Retain
	require.NotNil(t, sts.Spec.PersistentVolumeClaimRetentionPolicy)
	assert.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
	assert.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)

	// Rolling update is explicit so cluster-config rotation goes pod-by-pod.
	assert.Equal(t, appsv1.RollingUpdateStatefulSetStrategyType, sts.Spec.UpdateStrategy.Type)
	require.NotNil(t, sts.Spec.UpdateStrategy.RollingUpdate)
	require.NotNil(t, sts.Spec.UpdateStrategy.RollingUpdate.Partition)
	assert.Equal(t, int32(0), *sts.Spec.UpdateStrategy.RollingUpdate.Partition)
}

// --- Assertion helpers ---

func requireOwnerRef(t *testing.T, refs []metav1.OwnerReference, name string) {
	t.Helper()
	for _, ref := range refs {
		if ref.Name == name && ref.Kind == "LedgerService" {
			return
		}
	}
	t.Errorf("expected ownerReference to LedgerService %q, got %v", name, refs)
}

func requirePort(t *testing.T, ports []corev1.ServicePort, name string, port int32) {
	t.Helper()
	for _, p := range ports {
		if p.Name == name {
			assert.Equal(t, port, p.Port, "port %s", name)
			return
		}
	}
	t.Errorf("port %q not found in %v", name, ports)
}

func requireContainerPort(t *testing.T, ports []corev1.ContainerPort, name string, port int32) {
	t.Helper()
	for _, p := range ports {
		if p.Name == name {
			assert.Equal(t, port, p.ContainerPort, "container port %s", name)
			return
		}
	}
	t.Errorf("container port %q not found in %v", name, ports)
}

func requireVolumeMount(t *testing.T, mounts []corev1.VolumeMount, name, mountPath string) {
	t.Helper()
	for _, m := range mounts {
		if m.Name == name {
			assert.Equal(t, mountPath, m.MountPath, "volume mount %s", name)
			return
		}
	}
	t.Errorf("volume mount %q not found in %v", name, mounts)
}

func requireEnvVar(t *testing.T, envs []corev1.EnvVar, name, value string) {
	t.Helper()
	for _, e := range envs {
		if e.Name == name {
			assert.Equal(t, value, e.Value, "env var %s", name)
			return
		}
	}
	t.Errorf("env var %q not found", name)
}
