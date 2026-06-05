package controller

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestBackupServerAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ls   *ledgerv1alpha1.LedgerService
		want string
	}{
		{
			name: "default port",
			ls: &ledgerv1alpha1.LedgerService{
				ObjectMeta: metav1.ObjectMeta{Name: "ledger", Namespace: "ledger-v3"},
			},
			want: "ledger.ledger-v3.svc.cluster.local:8888",
		},
		{
			name: "custom port",
			ls: &ledgerv1alpha1.LedgerService{
				ObjectMeta: metav1.ObjectMeta{Name: "lgr", Namespace: "ns"},
				Spec:       ledgerv1alpha1.LedgerServiceSpec{GrpcPort: 9999},
			},
			want: "lgr.ns.svc.cluster.local:9999",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, backupServerAddr(tt.ls))
		})
	}
}

func TestBuildBackupJob_FullWithTLS(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.LedgerService{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger", Namespace: "ledger-v3"},
		Spec: ledgerv1alpha1.LedgerServiceSpec{
			Image: ledgerv1alpha1.ImageSpec{Repository: "ghcr.io/formancehq/ledger-v3-poc", Tag: "v0.0.8"},
			TLS:   &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "ledger-tls", CASecretKey: "ca.crt"},
		},
	}
	backup := &ledgerv1alpha1.LedgerBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "blockchains", Namespace: "ledger-v3"},
		Spec: ledgerv1alpha1.LedgerBackupSpec{
			ServiceRef: "ledger",
			Destination: ledgerv1alpha1.BackupDestination{
				Driver: "s3",
				S3:     &ledgerv1alpha1.S3Config{Bucket: "my-bucket", Region: "eu-west-1"},
			},
		},
	}
	run := &ledgerv1alpha1.LedgerBackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "blockchains-manual-abc", Namespace: "ledger-v3"},
		Spec:       ledgerv1alpha1.LedgerBackupRunSpec{BackupRef: "blockchains", Type: ledgerv1alpha1.BackupRunTypeFull},
	}

	job, err := buildBackupJob(run, backup, ls, tlsModeRequired)
	require.NoError(t, err)

	require.Equal(t, "blockchains-manual-abc", job.Name)
	require.Equal(t, "ledger-v3", job.Namespace)
	require.Equal(t, "blockchains", job.Labels[ledgerv1alpha1.LabelLedgerBackup])
	require.Equal(t, "blockchains-manual-abc", job.Labels[backupJobLabelRun])

	require.NotNil(t, job.Spec.BackoffLimit)
	require.Equal(t, int32(0), *job.Spec.BackoffLimit)
	require.Equal(t, corev1.RestartPolicyNever, job.Spec.Template.Spec.RestartPolicy)

	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	c := job.Spec.Template.Spec.Containers[0]
	require.Equal(t, backupJobContainerName, c.Name)
	require.Equal(t, "ghcr.io/formancehq/ledger-v3-poc:v0.0.8", c.Image)

	require.Equal(t, "/bin/sh", c.Command[0])
	require.Equal(t, "-c", c.Command[1])
	shell := c.Command[2]
	require.Contains(t, shell, "./ledgerctl store backup")
	require.Contains(t, shell, `--server "ledger.ledger-v3.svc.cluster.local:8888"`)
	require.Contains(t, shell, `--tls-ca-cert "$TLS_CA_CERT_FILE"`)
	require.Contains(t, shell, `--auth-token "$CLUSTER_SECRET"`)
	require.Contains(t, shell, "--driver s3")
	require.Contains(t, shell, "--s3-bucket my-bucket")
	require.Contains(t, shell, "--s3-region eu-west-1")
	require.Contains(t, shell, "--json")

	// TLS volume must be present with the ledger-tls secret.
	require.Len(t, job.Spec.Template.Spec.Volumes, 1)
	require.Equal(t, "tls-certs", job.Spec.Template.Spec.Volumes[0].Name)
	require.NotNil(t, job.Spec.Template.Spec.Volumes[0].Secret)
	require.Equal(t, "ledger-tls", job.Spec.Template.Spec.Volumes[0].Secret.SecretName)
	require.Len(t, c.VolumeMounts, 1)
	require.Equal(t, "/tls", c.VolumeMounts[0].MountPath)

	// TLS_CA_CERT_FILE env must point inside the mounted volume.
	envByName := func(name string) (corev1.EnvVar, bool) {
		for _, e := range c.Env {
			if e.Name == name {
				return e, true
			}
		}

		return corev1.EnvVar{}, false
	}
	tlsCA, ok := envByName("TLS_CA_CERT_FILE")
	require.True(t, ok)
	require.Equal(t, "/tls/ca.crt", tlsCA.Value)

	podName, ok := envByName("POD_NAME")
	require.True(t, ok)
	require.NotNil(t, podName.ValueFrom)

	podNs, ok := envByName("POD_NAMESPACE")
	require.True(t, ok)
	require.NotNil(t, podNs.ValueFrom)

	clusterSecret, ok := envByName("CLUSTER_SECRET")
	require.True(t, ok, "CLUSTER_SECRET must be injected when TLS is active")
	require.NotNil(t, clusterSecret.ValueFrom)
	require.NotNil(t, clusterSecret.ValueFrom.SecretKeyRef)
	require.Equal(t, "ledger-cluster-secret", clusterSecret.ValueFrom.SecretKeyRef.Name)
}

func TestBuildBackupJob_DisabledTLS_NoClusterSecret(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.LedgerService{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger", Namespace: "ns"},
		Spec:       ledgerv1alpha1.LedgerServiceSpec{Image: ledgerv1alpha1.ImageSpec{Repository: "r", Tag: "t"}},
	}
	backup := &ledgerv1alpha1.LedgerBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"},
		Spec:       ledgerv1alpha1.LedgerBackupSpec{Destination: ledgerv1alpha1.BackupDestination{Driver: "s3"}},
	}
	run := &ledgerv1alpha1.LedgerBackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns"},
		Spec:       ledgerv1alpha1.LedgerBackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
	}

	job, err := buildBackupJob(run, backup, ls, tlsModeDisabled)
	require.NoError(t, err)
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		require.NotEqual(t, "CLUSTER_SECRET", e.Name,
			"CLUSTER_SECRET must not be injected when TLS is disabled — the operator deletes the Secret in that mode")
	}
}

func TestBuildBackupJob_Incremental(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.LedgerService{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger", Namespace: "ns"},
		Spec:       ledgerv1alpha1.LedgerServiceSpec{Image: ledgerv1alpha1.ImageSpec{Repository: "repo", Tag: "tag"}},
	}
	backup := &ledgerv1alpha1.LedgerBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"},
		Spec:       ledgerv1alpha1.LedgerBackupSpec{Destination: ledgerv1alpha1.BackupDestination{Driver: "s3"}},
	}
	run := &ledgerv1alpha1.LedgerBackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "b-incr", Namespace: "ns"},
		Spec:       ledgerv1alpha1.LedgerBackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeIncremental},
	}

	job, err := buildBackupJob(run, backup, ls, tlsModeDisabled)
	require.NoError(t, err)

	shell := job.Spec.Template.Spec.Containers[0].Command[2]
	require.Contains(t, shell, "./ledgerctl store incremental-backup")
	require.Contains(t, shell, "--insecure")
	require.NotContains(t, shell, "--tls-ca-cert")
	require.Empty(t, job.Spec.Template.Spec.Volumes, "disabled TLS must not mount the tls-certs volume")
}

func TestBuildBackupJob_UnknownType(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.LedgerService{ObjectMeta: metav1.ObjectMeta{Name: "ledger", Namespace: "ns"}}
	backup := &ledgerv1alpha1.LedgerBackup{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"}}
	run := &ledgerv1alpha1.LedgerBackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns"},
		Spec:       ledgerv1alpha1.LedgerBackupRunSpec{Type: "Garbage"},
	}

	_, err := buildBackupJob(run, backup, ls, tlsModeDisabled)
	require.Error(t, err)
}

func TestBuildBackupJob_Timeout(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.LedgerService{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger", Namespace: "ns"},
		Spec:       ledgerv1alpha1.LedgerServiceSpec{Image: ledgerv1alpha1.ImageSpec{Repository: "r", Tag: "t"}},
	}
	run := &ledgerv1alpha1.LedgerBackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns"},
		Spec:       ledgerv1alpha1.LedgerBackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
	}

	t.Run("explicit timeout is forwarded as --timeout", func(t *testing.T) {
		t.Parallel()
		backup := &ledgerv1alpha1.LedgerBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"},
			Spec: ledgerv1alpha1.LedgerBackupSpec{
				Destination: ledgerv1alpha1.BackupDestination{Driver: "s3"},
				Timeout:     &metav1.Duration{Duration: 30 * time.Minute},
			},
		}
		job, err := buildBackupJob(run, backup, ls, tlsModeDisabled)
		require.NoError(t, err)
		require.Contains(t, job.Spec.Template.Spec.Containers[0].Command[2], "--timeout 30m0s")
	})

	t.Run("missing timeout means no --timeout flag", func(t *testing.T) {
		t.Parallel()
		backup := &ledgerv1alpha1.LedgerBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"},
			Spec:       ledgerv1alpha1.LedgerBackupSpec{Destination: ledgerv1alpha1.BackupDestination{Driver: "s3"}},
		}
		job, err := buildBackupJob(run, backup, ls, tlsModeDisabled)
		require.NoError(t, err)
		require.NotContains(t, job.Spec.Template.Spec.Containers[0].Command[2], "--timeout")
	})

	t.Run("zero timeout is omitted", func(t *testing.T) {
		t.Parallel()
		backup := &ledgerv1alpha1.LedgerBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"},
			Spec: ledgerv1alpha1.LedgerBackupSpec{
				Destination: ledgerv1alpha1.BackupDestination{Driver: "s3"},
				Timeout:     &metav1.Duration{Duration: 0},
			},
		}
		job, err := buildBackupJob(run, backup, ls, tlsModeDisabled)
		require.NoError(t, err)
		require.NotContains(t, job.Spec.Template.Spec.Containers[0].Command[2], "--timeout")
	})
}

func TestJobTerminalCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		job       *batchv1.Job
		succeeded bool
		terminal  bool
	}{
		{
			name:     "no conditions",
			job:      &batchv1.Job{},
			terminal: false,
		},
		{
			name: "complete",
			job: &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, Message: "done"},
			}}},
			succeeded: true,
			terminal:  true,
		},
		{
			name: "failed",
			job: &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
			}}},
			succeeded: false,
			terminal:  true,
		},
		{
			name: "false complete is not terminal",
			job: &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionFalse},
			}}},
			terminal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ok, term, _ := jobTerminalCondition(tt.job)
			require.Equal(t, tt.terminal, term)
			require.Equal(t, tt.succeeded, ok)
		})
	}
}

func TestParseBackupResult_Full(t *testing.T) {
	t.Parallel()

	logs := strings.Join([]string{
		"2026-06-05T10:00:00 INFO starting backup",
		"2026-06-05T10:00:01 INFO uploading files",
		`{"filesUploaded":42,"filesDeleted":3,"totalFiles":1500,"durationMs":12345,"lastLogSequence":987,"lastAuditSequence":654,"lastAppliedIndex":1000}`,
	}, "\n")

	var result fullBackupResult
	require.NoError(t, parseBackupResult(logs, ledgerv1alpha1.BackupRunTypeFull, &result))
	require.Equal(t, uint32(42), result.FilesUploaded)
	require.Equal(t, uint32(3), result.FilesDeleted)
	require.Equal(t, uint32(1500), result.TotalFiles)
	require.Equal(t, int64(12345), result.DurationMs)
	require.Equal(t, uint64(987), result.LastLogSequence)
	require.Equal(t, uint64(1000), result.LastAppliedIndex)
}

func TestParseBackupResult_Incremental(t *testing.T) {
	t.Parallel()

	logs := strings.Join([]string{
		"INFO running",
		`{"logEntriesExported":100,"auditEntriesExported":50,"segmentsUploaded":2,"durationMs":777,"lastLogSequence":42,"lastAuditSequence":21}`,
	}, "\n")

	var result incrementalBackupResult
	require.NoError(t, parseBackupResult(logs, ledgerv1alpha1.BackupRunTypeIncremental, &result))
	require.Equal(t, uint64(100), result.LogEntriesExported)
	require.Equal(t, uint32(2), result.SegmentsUploaded)
}

func TestParseBackupResult_NoJSON(t *testing.T) {
	t.Parallel()

	logs := "INFO no json line here\nfail"
	var result fullBackupResult
	require.Error(t, parseBackupResult(logs, ledgerv1alpha1.BackupRunTypeFull, &result))
}
