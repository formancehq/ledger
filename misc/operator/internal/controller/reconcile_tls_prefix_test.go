package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// TestEnsureBackupJob_ResolvesTLSModeFromPrefixedStatefulSet drives the backup
// exec path end-to-end against a fake client to lock in the EN-1319 invariant:
// fetchTLSMode must look up the StatefulSet by the operator-created (prefixed)
// name, not the bare CR name. The reconciler creates the StatefulSet as
// resourceName(cr); if the exec/backup call sites look it up by the bare CR
// name the Get returns NotFound, fetchTLSMode silently returns "", and the Job
// runs ledgerctl with --insecure against a TLS-required server.
func TestEnsureBackupJob_ResolvesTLSModeFromPrefixedStatefulSet(t *testing.T) {
	t.Parallel()

	const (
		crName    = "my-ledger"
		namespace = "ledger-v3"
	)

	ls := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: namespace},
		Spec: ledgerv1alpha1.ClusterSpec{
			Image: ledgerv1alpha1.ImageSpec{Repository: "ghcr.io/formancehq/ledger", Tag: "v0.0.8"},
			TLS:   &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "ledger-tls", CASecretKey: "ca.crt"},
		},
	}
	backup := &ledgerv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "bk", Namespace: namespace},
		Spec: ledgerv1alpha1.BackupSpec{
			ServiceRef: crName,
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

	// makeSTS builds a running StatefulSet carrying TLS_MODE=required, mirroring
	// what the reconciler renders for a TLS-required Cluster.
	makeSTS := func(name string) *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: appsv1.StatefulSetSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name: ledgerContainer,
							Env:  []corev1.EnvVar{{Name: "TLS_MODE", Value: tlsModeRequired}},
						}},
					},
				},
			},
		}
	}

	tests := []struct {
		name         string
		stsName      string
		wantTLSCA    bool // command must carry --tls-ca-cert
		wantInsecure bool // command must carry --insecure
	}{
		{
			name:      "prefixed StatefulSet name resolves TLS-required",
			stsName:   resourceName(crName), // what the operator actually creates
			wantTLSCA: true,
		},
		{
			name:         "bare StatefulSet name is NotFound and degrades to insecure",
			stsName:      crName, // the pre-fix bug: bare CR name
			wantInsecure: true,
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
				WithObjects(makeSTS(tt.stsName)).
				Build()

			r := &BackupRunReconciler{Client: c, Scheme: scheme}

			job, err := r.ensureBackupJob(context.Background(), run, backup, ls)
			require.NoError(t, err)
			require.Len(t, job.Spec.Template.Spec.Containers, 1)

			shell := job.Spec.Template.Spec.Containers[0].Command[2]
			if tt.wantTLSCA {
				require.Contains(t, shell, `--tls-ca-cert "$TLS_CA_CERT_FILE"`,
					"TLS-required StatefulSet found under prefixed name must yield a TLS ledgerctl command")
				require.NotContains(t, shell, "--insecure")
			}
			if tt.wantInsecure {
				require.Contains(t, shell, "--insecure",
					"bare-name lookup misses the StatefulSet and silently degrades to plaintext")
				require.NotContains(t, shell, "--tls-ca-cert")
			}
		})
	}
}
