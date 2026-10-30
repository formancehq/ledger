package controller

import (
	"context"
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

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// TestEnsureBackupJob_ResolvesTLSModeFromPrefixedStatefulSet drives the backup
// Job creation boundary against a fake client to verify TLS workload lookup:
// ensureBackupJob must look up the StatefulSet by the operator-created (prefixed)
// name, not the bare CR name. The reconciler creates the StatefulSet as
// resourceName(cr). Previously, a missing workload could select plaintext
// against a TLS-required server; Job creation must now retry instead.
func TestEnsureBackupJob_ResolvesTLSModeFromPrefixedStatefulSet(t *testing.T) {
	t.Parallel()

	const (
		crName    = "my-ledger"
		namespace = "ledger-v3"
	)

	ls := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: namespace, UID: "cluster-uid"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Image: ledgerv1alpha1.ImageSpec{Repository: "ghcr.io/formancehq/ledger", Tag: "v0.0.8"},
			TLS:   &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "ledger-tls", CASecretKey: "ca.crt"},
		},
	}
	backup := &ledgerv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "bk", Namespace: namespace},
		Spec: ledgerv1alpha1.BackupSpec{
			ClusterRef: crName,
			Destination: ledgerv1alpha1.BackupDestination{
				Driver: "s3",
				S3:     &ledgerv1alpha1.S3Config{Bucket: "my-bucket", Region: "eu-west-1"},
			},
		},
	}
	run := &ledgerv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "bk-manual-abc", Namespace: namespace},
		Spec:       ledgerv1alpha1.BackupRunSpec{BackupRef: "bk", Type: ledgerv1alpha1.BackupRunTypeFull},
	}

	makeSTS := func(name string) *appsv1.StatefulSet {
		sts := backupTLSWorkload(ls, tlsModeRequired)
		sts.Name = name

		return sts
	}

	tests := []struct {
		name        string
		stsName     string
		wantTLSCA   bool // command must carry --tls-ca-cert
		wantMissing bool // a bare-name resource cannot satisfy the prefixed workload lookup
	}{
		{
			name:      "prefixed StatefulSet name resolves TLS-required",
			stsName:   resourceName(crName), // what the operator actually creates
			wantTLSCA: true,
		},
		{
			name:        "bare StatefulSet name is NotFound and retries",
			stsName:     crName, // the pre-fix bug: bare CR name
			wantMissing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
			require.NoError(t, appsv1.AddToScheme(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))
			require.NoError(t, batchv1.AddToScheme(scheme))

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(makeSTS(tt.stsName), backupClusterService(ls)).
				Build()

			r := &BackupRunReconciler{Client: c, APIReader: c, Scheme: scheme}

			job, err := r.ensureBackupJob(context.Background(), run, backup, ls)
			if tt.wantMissing {
				require.True(t, apierrors.IsNotFound(err), "missing prefixed StatefulSet must be retryable: %v", err)
				require.Nil(t, job)
				var jobs batchv1.JobList
				require.NoError(t, c.List(t.Context(), &jobs))
				require.Empty(t, jobs.Items)

				return
			}
			require.NoError(t, err)
			require.Len(t, job.Spec.Template.Spec.Containers, 1)

			shell := job.Spec.Template.Spec.Containers[0].Command[2]
			if tt.wantTLSCA {
				require.Contains(t, shell, `--tls-ca-cert "$TLS_CA_CERT_FILE"`,
					"TLS-required StatefulSet found under prefixed name must yield a TLS ledgerctl command")
				require.NotContains(t, shell, "--insecure")
			}
		})
	}
}

// backupTLSWorkload includes the owner and rendered transport configuration that
// a real Cluster reconciliation supplies before backup Jobs can be provisioned.
func backupTLSWorkload(cluster *ledgerv1alpha1.Cluster, mode string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceName(cluster.Name), Namespace: cluster.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cluster, ledgerv1alpha1.GroupVersion.WithKind("Cluster"))},
		},
		Spec: buildStatefulSetSpec(cluster, "backup-fixture", nil, mode),
	}
}

// backupClusterService is the Cluster's own ClusterIP Service, which the
// backup Job dials and ensureBackupJob requires to be controlled by the Cluster.
func backupClusterService(cluster *ledgerv1alpha1.Cluster) *corev1.Service {
	return controlledByCluster(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName(cluster.Name), Namespace: cluster.Namespace},
	}, cluster)
}

func TestEnsureBackupJobWorkloadRecovery(t *testing.T) {
	t.Parallel()
	for _, owner := range []string{"missing", "foreign", "unowned"} {
		t.Run(owner, func(t *testing.T) {
			t.Parallel()
			c, run := backupRunRecoveryFixture(t)
			ctx := t.Context()
			cluster := &ledgerv1alpha1.Cluster{}
			backup := &ledgerv1alpha1.Backup{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "cluster", Namespace: run.Namespace}, cluster))
			require.NoError(t, c.Get(ctx, types.NamespacedName{Name: run.Spec.BackupRef, Namespace: run.Namespace}, backup))
			sts := &appsv1.StatefulSet{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Name: resourceName(cluster.Name), Namespace: run.Namespace}, sts))
			require.NoError(t, c.Delete(ctx, sts))
			if owner != "missing" {
				sts.ResourceVersion = ""
				if owner == "foreign" {
					sts.OwnerReferences[0].UID = "foreign-cluster-uid"
				} else {
					sts.OwnerReferences = nil
				}
				require.NoError(t, c.Create(ctx, sts))
			}
			r := &BackupRunReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
			job, err := r.ensureBackupJob(ctx, run, backup, cluster)
			if owner == "missing" {
				require.True(t, apierrors.IsNotFound(err), "missing workload must retry: %v", err)
			} else {
				require.ErrorContains(t, err, "is not controlled by Cluster")
			}
			require.Nil(t, job)
			var jobs batchv1.JobList
			require.NoError(t, c.List(ctx, &jobs))
			require.Empty(t, jobs.Items)
			if owner != "missing" {
				require.NoError(t, c.Delete(ctx, sts))
			}
			require.NoError(t, c.Create(ctx, backupTLSWorkload(cluster, tlsModeDisabled)))
			// A replacement controller observes the recovered workload and creates
			// exactly one owned Job rather than retaining the transient failure.
			r = &BackupRunReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
			job, err = r.ensureBackupJob(ctx, run, backup, cluster)
			require.NoError(t, err)
			require.True(t, metav1.IsControlledBy(job, run))
			require.NoError(t, c.List(ctx, &jobs))
			require.Len(t, jobs.Items, 1)
		})
	}
}

func TestEnsureBackupJobUsesCurrentTLSReferences(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{tlsModeOptional, tlsModeRequired} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			c, run := backupRunRecoveryFixture(t)
			ctx := t.Context()
			cluster := &ledgerv1alpha1.Cluster{}
			backup := &ledgerv1alpha1.Backup{}
			require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: "cluster"}, cluster))
			require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.BackupRef}, backup))
			sts := &appsv1.StatefulSet{}
			require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: resourceName(cluster.Name)}, sts))
			rendered := cluster.DeepCopy()
			rendered.Spec.TLS = &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "retained-certs", CASecretKey: "retained-ca.pem"}
			sts.Spec = backupTLSWorkload(rendered, mode).Spec
			require.NoError(t, c.Update(ctx, sts))
			require.Nil(t, cluster.Spec.TLS, "desired spec has already removed TLS")
			r := &BackupRunReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
			job, err := r.ensureBackupJob(ctx, run, backup, cluster)
			require.NoError(t, err)
			shell := job.Spec.Template.Spec.Containers[0].Command[2]
			require.NotContains(t, shell, "--insecure")
			require.Contains(t, shell, `--tls-ca-cert "$TLS_CA_CERT_FILE"`)
			require.Contains(t, job.Spec.Template.Spec.Volumes, corev1.Volume{
				Name: "tls-certs", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "retained-certs"}},
			})
			require.Contains(t, job.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "TLS_CA_CERT_FILE", Value: "/tls/retained-ca.pem"})
		})
	}
}
