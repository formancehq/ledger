package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

const (
	// backupJobContainerName is the single container running ledgerctl in the backup Job.
	backupJobContainerName = "ledgerctl"

	// backupJobLabelRun carries the owning LedgerBackupRun name so we can find
	// the Job and its pod from the reconciler.
	backupJobLabelRun = "ledger.formance.com/backup-run"
)

// fullBackupResult holds the parsed JSON output from `ledgerctl store backup`.
type fullBackupResult struct {
	FilesUploaded     uint32 `json:"filesUploaded"`
	FilesDeleted      uint32 `json:"filesDeleted"`
	TotalFiles        uint32 `json:"totalFiles"`
	DurationMs        int64  `json:"durationMs"`
	LastLogSequence   uint64 `json:"lastLogSequence"`
	LastAuditSequence uint64 `json:"lastAuditSequence"`
	LastAppliedIndex  uint64 `json:"lastAppliedIndex"`
}

// incrementalBackupResult holds the parsed JSON output from `ledgerctl store incremental-backup`.
type incrementalBackupResult struct {
	LogEntriesExported   uint64 `json:"logEntriesExported"`
	AuditEntriesExported uint64 `json:"auditEntriesExported"`
	SegmentsUploaded     uint32 `json:"segmentsUploaded"`
	DurationMs           int64  `json:"durationMs"`
	LastLogSequence      uint64 `json:"lastLogSequence"`
	LastAuditSequence    uint64 `json:"lastAuditSequence"`
}

// backupFlags builds the common ledgerctl flags for backup commands.
func backupFlags(dest *ledgerv1alpha1.BackupDestination) []string {
	args := []string{"--driver", dest.Driver}
	if dest.BucketID != "" {
		args = append(args, "--bucket-id", dest.BucketID)
	}

	if dest.S3 != nil {
		if dest.S3.Bucket != "" {
			args = append(args, "--s3-bucket", dest.S3.Bucket)
		}

		if dest.S3.Region != "" {
			args = append(args, "--s3-region", dest.S3.Region)
		}

		if dest.S3.Endpoint != "" {
			args = append(args, "--s3-endpoint", dest.S3.Endpoint)
		}
	}

	if dest.S3AccessKeyID != "" {
		args = append(args, "--s3-access-key-id", dest.S3AccessKeyID)
	}

	if dest.S3SecretAccessKey != "" {
		args = append(args, "--s3-secret-access-key", dest.S3SecretAccessKey)
	}

	return args
}

// backupServerAddr is the host:port a backup Job dials. The LedgerService
// Service routes to any pod; the leader proxies the backup internally so we
// don't need to target a specific pod. Using the cluster-internal service
// DNS keeps the TLS SNI inside the server certificate's SAN coverage.
func backupServerAddr(ls *ledgerv1alpha1.LedgerService) string {
	port := ls.Spec.GrpcPort
	if port == 0 {
		port = 8888
	}

	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", ls.Name, ls.Namespace, port)
}

// backupJobName returns the Job name for a given LedgerBackupRun.
func backupJobName(run *ledgerv1alpha1.LedgerBackupRun) string {
	return run.Name
}

// buildBackupJob renders the desired batchv1.Job for a LedgerBackupRun. The
// Job runs `ledgerctl store backup` (or incremental-backup) in a one-shot
// pod that connects to the LedgerService over its cluster DNS.
//
// tlsMode mirrors the running pod's TLS_MODE so the Job's ledgerctl negotiates
// the same transport. Pre-existing volumes/secrets from the ledger StatefulSet
// are reused (TLS secret, cluster-secret); the Job uses the same
// ServiceAccount so IRSA / IAM bindings transfer transparently.
//
// The caller is expected to set the owner reference on the returned Job
// (LedgerBackupRun → Job) for cascade deletion.
func buildBackupJob(
	run *ledgerv1alpha1.LedgerBackupRun,
	backup *ledgerv1alpha1.LedgerBackup,
	ls *ledgerv1alpha1.LedgerService,
	tlsMode string,
) (*batchv1.Job, error) {
	subcmd, err := backupSubcommand(run.Spec.Type)
	if err != nil {
		return nil, err
	}

	args := []string{"store", subcmd}
	args = append(args, backupFlags(&backup.Spec.Destination)...)
	args = append(args, "--json")
	if backup.Spec.Timeout != nil && backup.Spec.Timeout.Duration > 0 {
		args = append(args, "--timeout", backup.Spec.Timeout.Duration.String())
	}
	cmd := ledgerctlCommand(backupServerAddr(ls), tlsMode, args...)

	env := []corev1.EnvVar{
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
		}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
		}},
	}

	var (
		volumes []corev1.Volume
		mounts  []corev1.VolumeMount
	)

	if tlsMode == tlsModeRequired || tlsMode == tlsModeOptional {
		// The cluster-secret only exists when TLS is at least partially
		// active (the ledger operator deletes it when TLS is off), and it
		// must never travel in plaintext — so the bearer is gated together
		// with TLS.
		env = append(env, corev1.EnvVar{
			Name: "CLUSTER_SECRET",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: clusterSecretName(ls)},
					Key:                  clusterSecretKey,
				},
			},
		})

		if ls.Spec.TLS != nil && ls.Spec.TLS.SecretName != "" {
			volumes = append(volumes, corev1.Volume{
				Name: "tls-certs",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: ls.Spec.TLS.SecretName},
				},
			})
			mounts = append(mounts, corev1.VolumeMount{
				Name:      "tls-certs",
				MountPath: "/tls",
				ReadOnly:  true,
			})

			if ls.Spec.TLS.CASecretKey != "" {
				env = append(env, corev1.EnvVar{
					Name:  "TLS_CA_CERT_FILE",
					Value: "/tls/" + ls.Spec.TLS.CASecretKey,
				})
			}
		}
	}

	labels := map[string]string{
		ledgerv1alpha1.LabelLedgerBackup: backup.Name,
		backupJobLabelRun:                run.Name,
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupJobName(run),
			Namespace: run.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: new(int32),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: serviceAccountName(ls),
					RestartPolicy:      corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:         backupJobContainerName,
						Image:        fmt.Sprintf("%s:%s", ls.Spec.Image.Repository, ls.Spec.Image.Tag),
						Command:      cmd,
						Env:          env,
						VolumeMounts: mounts,
					}},
					Volumes:          volumes,
					ImagePullSecrets: ls.Spec.ImagePullSecrets,
					NodeSelector:     ls.Spec.NodeSelector,
					Tolerations:      ls.Spec.Tolerations,
				},
			},
		},
	}

	return job, nil
}

func backupSubcommand(t ledgerv1alpha1.BackupRunType) (string, error) {
	switch t {
	case ledgerv1alpha1.BackupRunTypeFull:
		return "backup", nil
	case ledgerv1alpha1.BackupRunTypeIncremental:
		return "incremental-backup", nil
	}

	return "", fmt.Errorf("unsupported backup type %q", t)
}

// jobTerminalCondition reports whether the Job has reached a terminal state
// (Complete or Failed) and which one. The boolean return is true iff the Job
// is terminal.
func jobTerminalCondition(job *batchv1.Job) (succeeded bool, terminal bool, message string) {
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}

		switch c.Type {
		case batchv1.JobComplete, batchv1.JobSuccessCriteriaMet:
			return true, true, c.Message
		case batchv1.JobFailed:
			msg := c.Message
			if c.Reason != "" {
				msg = c.Reason + ": " + msg
			}

			return false, true, msg
		}
	}

	return false, false, ""
}

// fetchJobPodLogs reads stdout from the Job's most recent pod. It returns
// the raw log content for callers to parse.
func fetchJobPodLogs(ctx context.Context, clientset kubernetes.Interface, namespace, jobName string) (string, error) {
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "batch.kubernetes.io/job-name=" + jobName,
	})
	if err != nil {
		return "", fmt.Errorf("listing pods for Job %q: %w", jobName, err)
	}

	if len(pods.Items) == 0 {
		// Fallback to the legacy label selector for older Kubernetes versions.
		pods, err = clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "job-name=" + jobName,
		})
		if err != nil {
			return "", fmt.Errorf("listing pods (legacy label) for Job %q: %w", jobName, err)
		}
	}

	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no pod found for Job %q", jobName)
	}

	// Pick the most recent pod by CreationTimestamp.
	target := &pods.Items[0]
	for i := range pods.Items[1:] {
		if pods.Items[i+1].CreationTimestamp.After(target.CreationTimestamp.Time) {
			target = &pods.Items[i+1]
		}
	}

	req := clientset.CoreV1().Pods(namespace).GetLogs(target.Name, &corev1.PodLogOptions{
		Container: backupJobContainerName,
	})

	stream, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("opening logs for pod %q: %w", target.Name, err)
	}
	defer func() {
		_ = stream.Close()
	}()

	data, err := io.ReadAll(stream)
	if err != nil {
		return "", fmt.Errorf("reading logs for pod %q: %w", target.Name, err)
	}

	return string(data), nil
}

// parseBackupResult extracts the last JSON object from the Job pod's stdout
// and unmarshals it into the result type matching the BackupRunType.
// ledgerctl emits the JSON summary on its last line; everything before is
// logging noise that we discard.
func parseBackupResult(logs string, runType ledgerv1alpha1.BackupRunType, out any) error {
	var jsonLine string

	scanner := bufio.NewScanner(strings.NewReader(logs))
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "{") && strings.HasSuffix(line, "}") {
			jsonLine = line
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning logs: %w", err)
	}

	if jsonLine == "" {
		return fmt.Errorf("no JSON summary found in %s output", runType)
	}

	if err := json.Unmarshal([]byte(jsonLine), out); err != nil {
		return fmt.Errorf("parsing %s summary: %w (line: %s)", runType, err, jsonLine)
	}

	return nil
}
