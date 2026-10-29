package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func tlsDisableFixture(t *testing.T) (client.WithWatch, *ledgerv1alpha1.Cluster) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "tls-disable", Namespace: "default", UID: "tls-disable-cluster"},
		Spec:       ledgerv1alpha1.ClusterSpec{Replicas: new(int32(1)), TLS: &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "existing-certs", CASecretKey: "custom-ca.pem"}},
	}
	applyDefaults(cluster)
	cluster.Spec.Persistence.DeletionProtection = new(false)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName(cluster.Name), Namespace: cluster.Namespace, UID: "tls-disable-sts", Generation: 1, CreationTimestamp: metav1.Now()},
		Spec:       buildStatefulSetSpec(cluster, "initial", nil, tlsModeRequired),
		Status:     appsv1.StatefulSetStatus{ObservedGeneration: 1, UpdatedReplicas: 1, ReadyReplicas: 1},
	}
	sts.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(cluster, ledgerv1alpha1.GroupVersion.WithKind("Cluster"))}

	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster, sts).WithObjects(cluster, sts, backupClusterService(cluster)).Build(), cluster
}

func tlsReconcile(t *testing.T, c client.Client, key client.ObjectKey) *appsv1.StatefulSet {
	t.Helper()
	cluster := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(context.Background(), key, cluster))
	r := &ClusterReconciler{Client: c, Scheme: c.Scheme()}
	_, err := r.reconcileStatefulSet(context.Background(), cluster, computeSpecHash(&cluster.Spec), nil)
	require.NoError(t, err)
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: resourceName(key.Name), Namespace: key.Namespace}, sts))

	return sts
}

func requireTLSCertificates(t *testing.T, sts *appsv1.StatefulSet, mode string) {
	t.Helper()
	require.Equal(t, mode, currentTLSModeFromStatefulSet(sts))
	var secretName string
	for _, v := range sts.Spec.Template.Spec.Volumes {
		if v.Name == "tls-certs" && v.Secret != nil {
			secretName = v.Secret.SecretName
		}
	}
	require.Equal(t, "existing-certs", secretName, "optional rollout must retain the certificate Secret")
	container := sts.Spec.Template.Spec.Containers[0]
	require.Contains(t, container.VolumeMounts, corev1.VolumeMount{Name: "tls-certs", MountPath: "/tls", ReadOnly: true})
	require.Contains(t, container.Env, corev1.EnvVar{Name: "TLS_CA_CERT_FILE", Value: "/tls/custom-ca.pem"})
	require.Contains(t, container.Env, corev1.EnvVar{Name: "LEDGERCTL_TLS_CA_CERT", Value: "/tls/custom-ca.pem"})
}

func TestTLSDisableRetainsCertificates(t *testing.T) {
	t.Parallel()
	for _, edit := range []string{"omit TLS", "clear reference", "retain reference"} {
		t.Run(edit, func(t *testing.T) {
			t.Parallel()
			c, cluster := tlsDisableFixture(t)
			switch edit {
			case "omit TLS":
				cluster.Spec.TLS = nil
			case "clear reference":
				cluster.Spec.TLS = &ledgerv1alpha1.TLSConfig{Enabled: false}
			case "retain reference":
				cluster.Spec.TLS.Enabled = false
			}
			require.NoError(t, c.Update(context.Background(), cluster))
			expectedSpec := cluster.Spec.DeepCopy()
			sts := tlsReconcile(t, c, client.ObjectKeyFromObject(cluster))
			requireTLSCertificates(t, sts, tlsModeOptional)
			latest := &ledgerv1alpha1.Cluster{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(cluster), latest))
			require.Equal(t, *expectedSpec, latest.Spec, "effective TLS must not rewrite desired spec")
		})
	}
}

// The fake API has no StatefulSet controller; rollout observations are supplied
// explicitly. These tests establish controller persistence, not Pod readiness.
func tlsSetConverged(t *testing.T, c client.Client, sts *appsv1.StatefulSet, converged bool) {
	t.Helper()
	sts.Status.ObservedGeneration = sts.Generation
	sts.Status.ReadyReplicas = 0
	sts.Status.UpdatedReplicas = 0
	if converged {
		sts.Status.ReadyReplicas = 1
		sts.Status.UpdatedReplicas = 1
	}
	require.NoError(t, c.Status().Update(context.Background(), sts))
}

func TestTLSDisableResumesUntilConverged(t *testing.T) {
	t.Parallel()
	c, cluster := tlsDisableFixture(t)
	cluster.Spec.TLS = nil
	require.NoError(t, c.Update(context.Background(), cluster))
	key := client.ObjectKeyFromObject(cluster)
	sts := tlsReconcile(t, c, key)
	tlsSetConverged(t, c, sts, false)
	// tlsReconcile constructs a fresh controller and reads only persisted inputs.
	sts = tlsReconcile(t, c, key)
	requireTLSCertificates(t, sts, tlsModeOptional)
	latest := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(context.Background(), key, latest))
	require.NotNil(t, latest.Status.TLSRollout)
	require.Equal(t, "existing-certs", latest.Status.TLSRollout.SecretName)
	require.Equal(t, "custom-ca.pem", latest.Status.TLSRollout.CASecretKey)
	require.Equal(t, tlsModeOptional, latest.Status.TLSRollout.Mode)
	tlsSetConverged(t, c, sts, true)
	sts = tlsReconcile(t, c, key)
	require.Equal(t, tlsModeDisabled, currentTLSModeFromStatefulSet(sts))
	for _, volume := range sts.Spec.Template.Spec.Volumes {
		require.NotEqual(t, "tls-certs", volume.Name)
	}
	for _, env := range sts.Spec.Template.Spec.Containers[0].Env {
		require.NotContains(t, []string{"TLS_CERT_FILE", "TLS_KEY_FILE", "TLS_CA_CERT_FILE", "LEDGERCTL_TLS_CA_CERT"}, env.Name)
	}
	tlsSetConverged(t, c, sts, false)
	tlsReconcile(t, c, key)
	require.NoError(t, c.Get(context.Background(), key, latest))
	require.NotNil(t, latest.Status.TLSRollout, "retain recovery state until disabled pods converge")
	tlsSetConverged(t, c, sts, true)
	tlsReconcile(t, c, key)
	require.NoError(t, c.Get(context.Background(), key, latest))
	require.Nil(t, latest.Status.TLSRollout)
}

func TestTLSDisableResumesAfterStatefulSetRecreation(t *testing.T) {
	t.Parallel()
	c, cluster := tlsDisableFixture(t)
	cluster.Spec.TLS = nil
	cluster.Spec.Persistence.WAL = ledgerv1alpha1.VolumeSpec{HostPath: &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/wal"}}
	require.NoError(t, c.Update(context.Background(), cluster))
	key := client.ObjectKeyFromObject(cluster)
	r := &ClusterReconciler{Client: c, Scheme: c.Scheme()}
	_, err := r.reconcileStatefulSet(context.Background(), cluster, "storage-change-and-disable", nil)
	require.NoError(t, err)
	missing := &appsv1.StatefulSet{}
	err = c.Get(context.Background(), client.ObjectKey{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}, missing)
	require.True(t, apierrors.IsNotFound(err), "the real immutable-template path must delete the StatefulSet")
	latest := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(context.Background(), key, latest))
	require.NotNil(t, latest.Status.TLSRollout)
	require.Equal(t, tlsModeOptional, latest.Status.TLSRollout.Mode)
	// Cluster-secret reconciliation runs before workload recreation. It must not
	// interpret the absent StatefulSet as a fresh plaintext bootstrap.
	token := controlledByCluster(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: clusterSecretName(cluster.Name), Namespace: cluster.Namespace}, Data: map[string][]byte{clusterSecretKey: []byte("original-token")}}, cluster)
	require.NoError(t, c.Create(context.Background(), token))
	r = &ClusterReconciler{Client: c, Scheme: c.Scheme()}
	require.NoError(t, r.reconcileClusterSecretForTLSState(context.Background(), latest, true))
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(token), token))
	require.Equal(t, []byte("original-token"), token.Data[clusterSecretKey])
	sts := tlsReconcile(t, c, key)
	requireTLSCertificates(t, sts, tlsModeOptional)
}

func TestTLSDisableStatusFailureLeavesWorkloadUntouched(t *testing.T) {
	t.Parallel()
	c, cluster := tlsDisableFixture(t)
	cluster.Spec.TLS = nil
	require.NoError(t, c.Update(context.Background(), cluster))
	original := &appsv1.StatefulSet{}
	stsKey := client.ObjectKey{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}
	require.NoError(t, c.Get(context.Background(), stsKey, original))
	attempts := 0
	wrapped := interceptor.NewClient(c, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, inner client.Client, resource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if resource == "status" {
				attempts++
				if attempts == 1 {
					return errors.New("injected TLS status conflict")
				}
			}

			return inner.SubResource(resource).Patch(ctx, obj, patch, opts...)
		},
	})
	r := &ClusterReconciler{Client: wrapped, Scheme: c.Scheme()}
	_, err := r.reconcileStatefulSet(context.Background(), cluster, "disable", nil)
	require.ErrorContains(t, err, "persisting TLS rollout recovery state: injected TLS status conflict")
	unchanged := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(context.Background(), stsKey, unchanged))
	require.Equal(t, original, unchanged)
	require.Nil(t, cluster.Status.TLSRollout)
	sts := tlsReconcile(t, wrapped, client.ObjectKeyFromObject(cluster))
	require.Equal(t, 2, attempts)
	requireTLSCertificates(t, sts, tlsModeOptional)
}

func TestTLSDisableCanReverseToRequired(t *testing.T) {
	t.Parallel()
	c, cluster := tlsDisableFixture(t)
	config := cluster.Spec.TLS.DeepCopy()
	cluster.Spec.TLS = nil
	require.NoError(t, c.Update(context.Background(), cluster))
	key := client.ObjectKeyFromObject(cluster)
	sts := tlsReconcile(t, c, key)
	tlsSetConverged(t, c, sts, false)
	require.NoError(t, c.Get(context.Background(), key, cluster))
	cluster.Spec.TLS = config
	require.NoError(t, c.Update(context.Background(), cluster))
	sts = tlsReconcile(t, c, key)
	requireTLSCertificates(t, sts, tlsModeOptional)
	tlsSetConverged(t, c, sts, true)
	sts = tlsReconcile(t, c, key)
	requireTLSCertificates(t, sts, tlsModeRequired)
}

func TestTLSDisableResumesAfterWorkloadWriteFailure(t *testing.T) {
	t.Parallel()
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%t", committed), func(t *testing.T) {
			t.Parallel()
			c, cluster := tlsDisableFixture(t)
			cluster.Spec.TLS = nil
			require.NoError(t, c.Update(context.Background(), cluster))
			attempts := 0
			wrapped := interceptor.NewClient(c, interceptor.Funcs{
				Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if _, ok := obj.(*appsv1.StatefulSet); ok {
						attempts++
						if attempts == 1 {
							if committed {
								require.NoError(t, inner.Update(ctx, obj, opts...))
							}

							return errors.New("injected workload response loss")
						}
					}

					return inner.Update(ctx, obj, opts...)
				},
			})
			r := &ClusterReconciler{Client: wrapped, Scheme: c.Scheme()}
			_, err := r.reconcileStatefulSet(context.Background(), cluster, "disable", nil)
			require.ErrorContains(t, err, "injected workload response loss")
			latest := &ledgerv1alpha1.Cluster{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(cluster), latest))
			require.NotNil(t, latest.Status.TLSRollout, "recovery references must already be durable")
			require.Equal(t, "existing-certs", latest.Status.TLSRollout.SecretName)
			sts := &appsv1.StatefulSet{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKey{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}, sts))
			tlsSetConverged(t, c, sts, false)
			sts = tlsReconcile(t, wrapped, client.ObjectKeyFromObject(cluster))
			requireTLSCertificates(t, sts, tlsModeOptional)
			require.Equal(t, 2, attempts)
		})
	}
}

func TestTLSDisableRejectsStaleClusterBeforeWorkloadMutation(t *testing.T) {
	t.Parallel()
	c, cluster := tlsDisableFixture(t)
	stale := cluster.DeepCopy()
	stale.Spec.TLS = nil
	// A newer API write makes the reconciler's resourceVersion stale.
	cluster.Annotations = map[string]string{"test": "concurrent edit"}
	require.NoError(t, c.Update(context.Background(), cluster))
	r := &ClusterReconciler{Client: c, Scheme: c.Scheme()}
	_, err := r.reconcileStatefulSet(context.Background(), stale, "disable", nil)
	require.True(t, apierrors.IsConflict(err), "retained-state persistence must use optimistic concurrency: %v", err)
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}, sts))
	requireTLSCertificates(t, sts, tlsModeRequired)
}

func TestTLSDisableBackupRetainsLiveCertificates(t *testing.T) {
	t.Parallel()
	c, cluster := tlsDisableFixture(t)
	require.NoError(t, batchv1.AddToScheme(c.Scheme()))
	cluster.Spec.TLS = nil
	require.NoError(t, c.Update(context.Background(), cluster))
	sts := tlsReconcile(t, c, client.ObjectKeyFromObject(cluster))
	requireTLSCertificates(t, sts, tlsModeOptional)
	run := &ledgerv1alpha1.BackupRun{ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: cluster.Namespace, UID: "run-uid"}, Spec: ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull}}
	backup := &ledgerv1alpha1.Backup{Spec: ledgerv1alpha1.BackupSpec{Destination: ledgerv1alpha1.BackupDestination{Driver: "s3", S3: &ledgerv1alpha1.S3Config{Bucket: "backups", Region: "eu-west-1"}}}}
	r := &BackupRunReconciler{Client: c, Scheme: c.Scheme()}
	job, err := r.ensureBackupJob(context.Background(), run, backup, cluster)
	require.NoError(t, err)
	require.Contains(t, job.Spec.Template.Spec.Volumes, corev1.Volume{Name: "tls-certs", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "existing-certs"}}})
	require.Contains(t, job.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "TLS_CA_CERT_FILE", Value: "/tls/custom-ca.pem"})
	require.NotContains(t, job.Spec.Template.Spec.Containers[0].Command[2], "--insecure")
}
