package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	// backupJobContainerName is the single container running ledgerctl in the backup Job.
	backupJobContainerName = "ledgerctl"

	// backupJobLabelRun carries the owning BackupRun name so we can find
	// the Job and its pod from the reconciler.
	backupJobLabelRun = "ledger.formance.com/backup-run"
)

// fullBackupResult holds the parsed JSON output from `ledgerctl store backup`.
type fullBackupResult struct {
	FilesUploaded     uint32 `json:"filesUploaded"`
	FilesDeleted      uint32 `json:"filesDeleted"`
	OrphansDeleted    uint32 `json:"orphansDeleted"`
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
	OrphansDeleted       uint32 `json:"orphansDeleted"`
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

// backupServerAddr is the host:port a backup Job dials. The Cluster
// Service routes to any pod; the leader proxies the backup internally so we
// don't need to target a specific pod. Using the cluster-internal service
// DNS keeps the TLS SNI inside the server certificate's SAN coverage.
func backupServerAddr(ls *ledgerv1alpha1.Cluster) string {
	port := ls.Spec.GrpcPort
	if port == 0 {
		port = 8888
	}

	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", resourceName(ls.Name), ls.Namespace, port)
}

// backupJobName returns the Job name for a given BackupRun.
func backupJobName(run *ledgerv1alpha1.BackupRun) string {
	return prefixedName(run.Name)
}

// buildBackupJob renders the desired batchv1.Job for a BackupRun. The
// Job runs `ledgerctl store backup` (or incremental-backup) in a one-shot
// pod that connects to the Cluster over its cluster DNS.
//
// tlsMode mirrors the running pod's TLS_MODE so the Job's ledgerctl negotiates
// the same transport. Pre-existing volumes/secrets from the ledger StatefulSet
// are reused (TLS secret, cluster-secret); the Job uses the same
// ServiceAccount so IRSA / IAM bindings transfer transparently.
//
// The caller is expected to set the owner reference on the returned Job
// (BackupRun → Job) for cascade deletion.
func buildBackupJob(
	run *ledgerv1alpha1.BackupRun,
	backup *ledgerv1alpha1.Backup,
	ls *ledgerv1alpha1.Cluster,
	tlsMode string,
) (*batchv1.Job, error) {
	subcmd, err := backupSubcommand(run.Spec.Type)
	if err != nil {
		return nil, err
	}

	args := []string{"store", subcmd}
	args = append(args, backupFlags(&backup.Spec.Destination)...)
	args = append(args, "--json")
	// Mirror the JSON result to /dev/termination-log so the kubelet surfaces
	// it on ContainerStatus.State.Terminated.Message (≤4 KiB, structured) —
	// the operator reads it back from pod.status rather than scraping
	// merged stdout+stderr. --result-file is a generic ledgerctl flag; the
	// Kubernetes-specific path lives only here.
	args = append(args, "--result-file", corev1.TerminationMessagePathDefault)
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
					LocalObjectReference: corev1.LocalObjectReference{Name: clusterSecretName(ls.Name)},
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
		ledgerv1alpha1.LabelBackup: backup.Name,
		backupJobLabelRun:          run.Name,
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
						// ledgerctl mirrors its --json summary into
						// /dev/termination-log; the kubelet then surfaces it
						// on ContainerStatus.State.Terminated.Message. We
						// read that field first (see applyJobResult) to skip
						// the merged stdout+stderr scrape entirely.
						// FallbackToLogsOnError keeps the existing log-based
						// path usable when the container exits before
						// writing the message (e.g. early crash).
						TerminationMessagePath:   corev1.TerminationMessagePathDefault,
						TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
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

// fetchJobResultPayload returns the bytes the operator should hand to
// parseBackupResult to extract the ledgerctl --json summary.
//
// Preferred source is ContainerStatus.State.Terminated.Message, populated by
// the kubelet from the container's terminationMessagePath when ledgerctl
// mirrors its summary into /dev/termination-log. That gives us a bounded
// (≤4 KiB), purely structured payload — no log scraping needed.
//
// If the field is empty (older ledgerctl, container died early, ...) we fall
// back to streaming the pod logs and let parseBackupResult find the JSON
// suffix in the merged stdout+stderr.
func fetchJobResultPayload(ctx context.Context, clientset kubernetes.Interface, namespace, jobName string) (string, error) {
	pod, err := mostRecentJobPod(ctx, clientset, namespace, jobName)
	if err != nil {
		return "", err
	}

	if msg := terminatedMessage(pod); msg != "" {
		return msg, nil
	}

	return fetchPodLogs(ctx, clientset, namespace, pod.Name)
}

// terminatedMessage returns the kubelet-populated terminated message of the
// backup container if it exited cleanly (in either State.Terminated or
// LastTerminationState — the latter covers retried pods). Empty string when
// no terminated container has produced a message yet.
func terminatedMessage(pod *corev1.Pod) string {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != backupJobContainerName {
			continue
		}

		if cs.State.Terminated != nil && cs.State.Terminated.Message != "" {
			return cs.State.Terminated.Message
		}

		if cs.LastTerminationState.Terminated != nil && cs.LastTerminationState.Terminated.Message != "" {
			return cs.LastTerminationState.Terminated.Message
		}
	}

	return ""
}

// mostRecentJobPod returns the most recently created pod owned by the named
// Job. Job pods are labeled by the Job controller; the canonical label changed
// names between Kubernetes versions so the lookup tries the modern label
// first and falls back to the legacy one.
func mostRecentJobPod(ctx context.Context, clientset kubernetes.Interface, namespace, jobName string) (*corev1.Pod, error) {
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "batch.kubernetes.io/job-name=" + jobName,
	})
	if err != nil {
		return nil, fmt.Errorf("listing pods for Job %q: %w", jobName, err)
	}

	if len(pods.Items) == 0 {
		// Fallback to the legacy label selector for older Kubernetes versions.
		pods, err = clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "job-name=" + jobName,
		})
		if err != nil {
			return nil, fmt.Errorf("listing pods (legacy label) for Job %q: %w", jobName, err)
		}
	}

	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no pod found for Job %q", jobName)
	}

	// Pick the most recent pod by CreationTimestamp.
	target := &pods.Items[0]
	for i := range pods.Items[1:] {
		if pods.Items[i+1].CreationTimestamp.After(target.CreationTimestamp.Time) {
			target = &pods.Items[i+1]
		}
	}

	return target, nil
}

// fetchPodLogs streams the container's merged stdout+stderr log for the
// named pod and returns it as a string. Used as a fallback when
// terminationMessage is not available.
func fetchPodLogs(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) (string, error) {
	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: backupJobContainerName,
	})

	stream, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("opening logs for pod %q: %w", podName, err)
	}
	defer func() {
		_ = stream.Close()
	}()

	data, err := io.ReadAll(stream)
	if err != nil {
		return "", fmt.Errorf("reading logs for pod %q: %w", podName, err)
	}

	return string(data), nil
}

// parseBackupResult extracts the JSON summary from a payload produced by
// `ledgerctl --json` and unmarshals it into the result type matching the
// BackupRunType.
//
// The payload is typically the kubelet-captured terminationMessage (a
// standalone JSON object), but may also be the merged stdout+stderr log
// stream when we fall back from the structured field. In both cases the
// summary is the last JSON object in the payload — scan candidate "{"
// anchors from right to left and unmarshal from the first one whose suffix
// parses cleanly. This handles both compact and pretty-printed JSON
// (ledgerctl uses json.MarshalIndent) and tolerates any non-JSON preamble.
func parseBackupResult(payload string, runType ledgerv1alpha1.BackupRunType, out any) error {
	trimmed := strings.TrimRightFunc(payload, unicode.IsSpace)

	for end := len(trimmed); end > 0; {
		idx := strings.LastIndex(trimmed[:end], "{")
		if idx < 0 {
			break
		}

		candidate := trimmed[idx:]
		if err := json.Unmarshal([]byte(candidate), out); err == nil {
			return nil
		}

		end = idx
	}

	return fmt.Errorf("no JSON summary found in %s output", runType)
}
