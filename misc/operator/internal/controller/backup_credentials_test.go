package controller

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// Exercise the persisted resources after the real BackupRun reconciliation.
// Fake clients do not run the Job controller: the Pod template is the assertion
// surface for Pod propagation, not evidence of a kubelet resolving a Secret.
func TestBackupRunCredentialsStayInSecrets(t *testing.T) {
	t.Parallel()

	for _, runType := range []ledgerv1alpha1.BackupRunType{ledgerv1alpha1.BackupRunTypeFull, ledgerv1alpha1.BackupRunTypeIncremental} {
		for _, static := range []bool{false, true} {
			name := string(runType) + "/ambient"
			if static {
				name = string(runType) + "/static"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				const accessKey = "AUDIT_S3_ACCESS_69da"
				const secretKey = "AUDIT_S3_SECRET_71ca+/='\"$(false)"
				scheme := runtime.NewScheme()
				require.NoError(t, corev1.AddToScheme(scheme))
				require.NoError(t, appsv1.AddToScheme(scheme))
				require.NoError(t, batchv1.AddToScheme(scheme))
				require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
				cluster := &ledgerv1alpha1.Cluster{
					ObjectMeta: metav1.ObjectMeta{Name: "ledger", Namespace: "test"},
					Spec: ledgerv1alpha1.ClusterSpec{
						Image: ledgerv1alpha1.ImageSpec{Repository: "ledger", Tag: "test"},
						TLS:   &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "ledger-tls", CASecretKey: "ca.crt"},
					},
				}
				backup := &ledgerv1alpha1.Backup{
					ObjectMeta: metav1.ObjectMeta{Name: "backup", Namespace: "test"},
					Spec: ledgerv1alpha1.BackupSpec{ClusterRef: cluster.Name, Destination: ledgerv1alpha1.BackupDestination{
						Driver: "s3", S3: &ledgerv1alpha1.S3Config{Bucket: "audit-control", Region: "eu-west-1"},
					}},
				}
				if static {
					backup.Spec.Destination.S3AccessKeyIDFrom = &ledgerv1alpha1.SecretKeyRef{Name: "s3-auth", Key: "access"}
					backup.Spec.Destination.S3SecretAccessKeyFrom = &ledgerv1alpha1.SecretKeyRef{Name: "s3-auth", Key: "secret"}
				}
				run := &ledgerv1alpha1.BackupRun{
					ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "test", UID: "run-uid"},
					Spec:       ledgerv1alpha1.BackupRunSpec{BackupRef: backup.Name, Type: runType},
				}
				sts := &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{Name: resourceName(cluster.Name), Namespace: "test"},
					Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "ledger", Env: []corev1.EnvVar{{Name: "TLS_MODE", Value: tlsModeRequired}}}},
					}}},
				}
				secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-auth", Namespace: "test"},
					Data: map[string][]byte{"access": []byte(accessKey), "secret": []byte(secretKey)}}
				c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(run).
					WithObjects(cluster, backup, run, sts, secret).Build()
				r := &BackupRunReconciler{Client: c, Scheme: scheme}
				_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
				require.NoError(t, err)
				job := &batchv1.Job{}
				require.NoError(t, c.Get(t.Context(), types.NamespacedName{Name: backupJobName(run), Namespace: "test"}, job))
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(backup), backup))
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(run), run))
				require.Equal(t, ledgerv1alpha1.BackupRunPhaseRunning, run.Status.Phase)
				require.Equal(t, run.UID, job.OwnerReferences[0].UID)
				container := job.Spec.Template.Spec.Containers[0]
				if static {
					require.Contains(t, container.Env, secretKeyEnv("S3_ACCESS_KEY_ID", "s3-auth", "access"))
					require.Contains(t, container.Env, secretKeyEnv("S3_SECRET_ACCESS_KEY", "s3-auth", "secret"))
				} else {
					for _, env := range container.Env {
						require.NotContains(t, []string{"S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"}, env.Name)
					}
				}
				require.Contains(t, container.Command[2], "'--s3-bucket' 'audit-control'")
				subcommand, err := backupSubcommand(runType)
				require.NoError(t, err)
				require.Contains(t, container.Command[2], "'store' '"+subcommand+"'")
				for _, surface := range []any{backup, run, job, job.Spec.Template} {
					for _, marshal := range []func(any) ([]byte, error){json.Marshal, yaml.Marshal} {
						data, err := marshal(surface)
						require.NoError(t, err)
						for _, value := range []string{accessKey, secretKey} {
							require.NotContains(t, string(data), value)
							encoded, err := json.Marshal(value)
							require.NoError(t, err)
							require.NotContains(t, string(data), string(encoded[1:len(encoded)-1]))
							require.NotContains(t, string(data), "AUDIT_S3_SECRET_71ca")
							require.NotContains(t, string(data), base64.StdEncoding.EncodeToString([]byte(value)))
						}
						require.NotContains(t, string(data), "--s3-access-key-id")
						require.NotContains(t, string(data), "--s3-secret-access-key")
					}
				}
				// Generated deep copies must not alias either newly added reference.
				if static {
					cloned := backup.DeepCopy()
					cloned.Spec.Destination.S3AccessKeyIDFrom.Key = "changed"
					cloned.Spec.Destination.S3SecretAccessKeyFrom.Key = "changed"
					require.Equal(t, "access", backup.Spec.Destination.S3AccessKeyIDFrom.Key)
					require.Equal(t, "secret", backup.Spec.Destination.S3SecretAccessKeyFrom.Key)
				}
			})
		}
	}
}

// Both install paths must expose references and remove the literal fields.
func TestBackupCredentialCRDSchema(t *testing.T) {
	t.Parallel()
	for _, path := range []string{
		"../../config/crd/bases/ledger.formance.com_backups.yaml",
		"../../helm/crds/templates/ledger.formance.com_backups.yaml",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			var crd apiextensionsv1.CustomResourceDefinition
			require.NoError(t, yaml.Unmarshal(data, &crd))
			for _, version := range crd.Spec.Versions {
				dest := version.Schema.OpenAPIV3Schema.Properties["spec"].Properties["destination"]
				require.NotContains(t, dest.Properties, "s3AccessKeyId")
				require.NotContains(t, dest.Properties, "s3SecretAccessKey")
				for _, field := range []string{"s3AccessKeyIdFrom", "s3SecretAccessKeyFrom"} {
					ref := dest.Properties[field]
					require.Equal(t, "object", ref.Type)
					require.ElementsMatch(t, []string{"name", "key"}, ref.Required)
					require.Len(t, ref.Properties, 2)
				}
			}
		})
	}
}
