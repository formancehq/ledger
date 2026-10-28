package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestReconcileDebugBoxCreatesSuperuserCredentialAndPod(t *testing.T) {
	scheme := authKeysScheme(t)
	cluster := authEnabledCluster("ledger", "tenant-a", nil)
	cluster.Spec.DebugBox = &ledgerv1alpha1.DebugBoxSpec{Enabled: true}
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}

	ready, err := r.reconcileDebugBox(context.Background(), cluster)
	require.NoError(t, err)
	require.False(t, ready)

	credentials := &ledgerv1alpha1.Credentials{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: debugBoxCredentialsName(cluster)}, credentials))
	require.True(t, credentials.Spec.Superuser)
	require.Empty(t, credentials.Spec.Scopes)
	require.Equal(t, debugBoxCredentialsSelectorValue(cluster), cluster.Labels[labelDebugBoxCredential])

	credentials.Status.Phase = "Ready"
	credentials.Status.DistributedSecretRefs = []ledgerv1alpha1.SecretReference{{
		Namespace: cluster.Namespace,
		Name:      operatorCredentialsSecretName(debugBoxCredentialsName(cluster)),
	}}
	require.NoError(t, r.Update(context.Background(), credentials))
	ready, err = r.reconcileDebugBox(context.Background(), cluster)
	require.NoError(t, err)
	require.True(t, ready)

	pod := &corev1.Pod{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: debugBoxPodName(cluster), Namespace: cluster.Namespace}, pod))
	require.Equal(t, []string{"/bin/bash", "-c"}, pod.Spec.Containers[0].Command)
	require.Equal(t, "ledgerctl", pod.Spec.Containers[0].Name)
	require.Equal(t, operatorCredentialsSecretName(debugBoxCredentialsName(cluster)), pod.Spec.Volumes[0].Secret.SecretName)
	require.Contains(t, pod.Spec.Containers[0].Args[0], "/ledgerctl-wrapper/ledgerctl")
	require.Contains(t, envValue(pod.Spec.Containers[0].Env, "PATH"), "/ledgerctl-wrapper")
	require.NotNil(t, volumeByName(pod.Spec.Volumes, "ledgerctl-wrapper").EmptyDir)
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, item := range env {
		if item.Name == name {
			return item.Value
		}
	}

	return ""
}

func volumeByName(volumes []corev1.Volume, name string) corev1.Volume {
	for _, volume := range volumes {
		if volume.Name == name {
			return volume
		}
	}

	return corev1.Volume{}
}

func TestReconcileDebugBoxDeletesWhenDisabled(t *testing.T) {
	scheme := authKeysScheme(t)
	cluster := authEnabledCluster("ledger", "tenant-a", nil)
	credentials := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: debugBoxCredentialsName(cluster)}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: debugBoxPodName(cluster), Namespace: cluster.Namespace}}
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, credentials, pod).Build(),
		Scheme: scheme,
	}

	ready, err := r.reconcileDebugBox(context.Background(), cluster)
	require.NoError(t, err)
	require.True(t, ready)
	require.Error(t, r.Get(context.Background(), types.NamespacedName{Name: debugBoxCredentialsName(cluster)}, &ledgerv1alpha1.Credentials{}))
	require.Error(t, r.Get(context.Background(), types.NamespacedName{Name: debugBoxPodName(cluster), Namespace: cluster.Namespace}, &corev1.Pod{}))
}

func TestReconcileDebugBoxReplacesPodWhenConfigurationChanges(t *testing.T) {
	scheme := authKeysScheme(t)
	cluster := authEnabledCluster("ledger", "tenant-a", nil)
	cluster.Spec.DebugBox = &ledgerv1alpha1.DebugBoxSpec{Enabled: true}
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}

	ready, err := r.reconcileDebugBox(context.Background(), cluster)
	require.NoError(t, err)
	require.False(t, ready)
	credentials := &ledgerv1alpha1.Credentials{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: debugBoxCredentialsName(cluster)}, credentials))
	credentials.Status.Phase = "Ready"
	credentials.Status.DistributedSecretRefs = []ledgerv1alpha1.SecretReference{{
		Namespace: cluster.Namespace,
		Name:      operatorCredentialsSecretName(debugBoxCredentialsName(cluster)),
	}}
	require.NoError(t, r.Update(context.Background(), credentials))
	ready, err = r.reconcileDebugBox(context.Background(), cluster)
	require.NoError(t, err)
	require.True(t, ready)

	cluster.Spec.TLS = &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "new-tls", CASecretKey: "ca.crt"}
	ready, err = r.reconcileDebugBox(context.Background(), cluster)
	require.NoError(t, err)
	require.True(t, ready)
	require.Error(t, r.Get(context.Background(), types.NamespacedName{Name: debugBoxPodName(cluster), Namespace: cluster.Namespace}, &corev1.Pod{}),
		"the old pod must be deleted before recreating it with immutable configuration")

	ready, err = r.reconcileDebugBox(context.Background(), cluster)
	require.NoError(t, err)
	require.True(t, ready)
	pod := &corev1.Pod{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: debugBoxPodName(cluster), Namespace: cluster.Namespace}, pod))
	require.Equal(t, "new-tls", volumeByName(pod.Spec.Volumes, "tls-ca").Secret.SecretName)
}

func TestReconcileDebugBoxSupportsNoAuth(t *testing.T) {
	scheme := authKeysScheme(t)
	authEnabled := false
	cluster := authEnabledCluster("ledger", "tenant-a", nil)
	cluster.Spec.Auth.Enabled = &authEnabled
	cluster.Spec.DebugBox = &ledgerv1alpha1.DebugBoxSpec{Enabled: true}
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}

	ready, err := r.reconcileDebugBox(context.Background(), cluster)
	require.NoError(t, err)
	require.True(t, ready)
	require.Error(t, r.Get(context.Background(), types.NamespacedName{Name: debugBoxCredentialsName(cluster)}, &ledgerv1alpha1.Credentials{}))

	pod := &corev1.Pod{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: debugBoxPodName(cluster), Namespace: cluster.Namespace}, pod))
	assert.Empty(t, envValue(pod.Spec.Containers[0].Env, "OPERATOR_SIGNING_KEY"))
	assert.Empty(t, volumeByName(pod.Spec.Volumes, "operator-credentials").Name)
	assert.Contains(t, pod.Spec.Containers[0].Args[0], `if [[ -z "${OPERATOR_SIGNING_KEY:-}" ]]`)
}
