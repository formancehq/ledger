//go:build integration

package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestBackupRunCommittedCreateLostResponseIntegration(t *testing.T) {
	t.Parallel()
	testCtx, cancelTest := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelTest()
	namespace := createTestNamespace(t)
	c, err := client.NewWithWatch(testEnv.Config, client.Options{Scheme: k8sClient.Scheme()})
	require.NoError(t, err)
	clientset := newTestClientset(t)

	cluster := newCluster("recovery-cluster", namespace)
	cluster.Spec.Image = ledgerv1alpha1.ImageSpec{Repository: "ghcr.io/formancehq/ledger", Tag: "test"}
	require.NoError(t, c.Create(testCtx, cluster))
	backup := &ledgerv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "recovery-backup", Namespace: namespace},
		Spec: ledgerv1alpha1.BackupSpec{
			ClusterRef: cluster.Name,
			Destination: ledgerv1alpha1.BackupDestination{
				Driver: "s3",
				S3:     &ledgerv1alpha1.S3Config{Bucket: "recovery-bucket"},
			},
			// Empty schedules keep the wired Backup controller from creating runs.
		},
	}
	require.NoError(t, c.Create(testCtx, backup))
	run := &ledgerv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "recovery-run", Namespace: namespace,
			Labels: map[string]string{ledgerv1alpha1.LabelBackup: backup.Name},
		},
		Spec: ledgerv1alpha1.BackupRunSpec{BackupRef: backup.Name, Type: ledgerv1alpha1.BackupRunTypeFull},
	}
	require.NoError(t, c.Create(testCtx, run))
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
	creates := 0
	wrapped := interceptor.NewClient(c, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*batchv1.Job); !ok {
				return c.Create(ctx, obj, opts...)
			}
			creates++
			// The reservation must really be durable before a Job can start.
			var reserved ledgerv1alpha1.BackupRun
			require.NoError(t, c.Get(ctx, req.NamespacedName, &reserved))
			require.Equal(t, ledgerv1alpha1.BackupRunPhaseRunning, reserved.Status.Phase)
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			if creates == 1 {
				// The real API server committed the Job; only its response is lost.
				return context.DeadlineExceeded
			}
			return nil
		},
	})
	reconciler := &BackupRunReconciler{Client: wrapped, Scheme: c.Scheme(), Clientset: clientset}
	_, err = reconciler.Reconcile(testCtx, req)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, c.Get(testCtx, req.NamespacedName, run))
	require.Equal(t, ledgerv1alpha1.BackupRunPhaseRunning, run.Status.Phase)
	require.Nil(t, run.Status.CompletionTime)
	require.NotNil(t, run.Status.StartTime)
	reservedStart := run.Status.StartTime.DeepCopy()

	var job batchv1.Job
	jobKey := client.ObjectKey{Namespace: namespace, Name: backupJobName(run)}
	require.NoError(t, c.Get(testCtx, jobKey, &job))
	require.True(t, metav1.IsControlledBy(&job, run))
	committedUID := job.UID
	require.NotEmpty(t, committedUID)

	sibling := &ledgerv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "waiting-run", Namespace: namespace,
			Labels: map[string]string{ledgerv1alpha1.LabelBackup: backup.Name},
		},
		Spec: run.Spec,
	}
	require.NoError(t, c.Create(testCtx, sibling))
	// A fresh controller has no memory of the timed-out request.
	reconciler = &BackupRunReconciler{Client: wrapped, Scheme: c.Scheme(), Clientset: clientset}
	result, err := reconciler.Reconcile(testCtx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(sibling)})
	require.NoError(t, err)
	require.Equal(t, concurrencyRequeue, result.RequeueAfter)
	require.NoError(t, c.Get(testCtx, client.ObjectKeyFromObject(sibling), sibling))
	require.Equal(t, ledgerv1alpha1.BackupRunPhasePending, sibling.Status.Phase)
	require.Contains(t, sibling.Status.Message, run.Name)
	require.True(t, apierrors.IsNotFound(c.Get(testCtx, client.ObjectKey{Namespace: namespace, Name: backupJobName(sibling)}, &batchv1.Job{})))
	require.Equal(t, 1, creates)

	// envtest has no Job controller or kubelet: publish their completed result
	// through the real Pod and Job status subresources.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "recovery-result", Namespace: namespace,
			Labels:          map[string]string{"batch.kubernetes.io/job-name": job.Name},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(&job, batchv1.SchemeGroupVersion.WithKind("Job"))},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: backupJobContainerName, Image: "ghcr.io/formancehq/ledger:test"}},
		},
	}
	pod, err = clientset.CoreV1().Pods(namespace).Create(testCtx, pod, metav1.CreateOptions{})
	require.NoError(t, err)
	now := metav1.Now()
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodSucceeded,
		ContainerStatuses: []corev1.ContainerStatus{{
			Name: backupJobContainerName,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 0, StartedAt: now, FinishedAt: now,
				Message: `{"filesUploaded":7,"totalFiles":7,"lastAppliedIndex":42,"lastLogSequence":40,"lastAuditSequence":41}`,
			}},
		}},
	}
	_, err = clientset.CoreV1().Pods(namespace).UpdateStatus(testCtx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)
	job.Status = batchv1.JobStatus{
		StartTime: &now, CompletionTime: &now, Succeeded: 1,
		Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
		},
	}
	require.NoError(t, c.Status().Update(testCtx, &job))
	_, err = reconciler.Reconcile(testCtx, req)
	require.NoError(t, err)
	require.NoError(t, c.Get(testCtx, req.NamespacedName, run))
	require.Equal(t, ledgerv1alpha1.BackupRunPhaseSucceeded, run.Status.Phase)
	require.NotNil(t, run.Status.CompletionTime)
	require.Equal(t, reservedStart, run.Status.StartTime)
	require.NotNil(t, run.Status.Full)
	require.Equal(t, uint32(7), run.Status.Full.FilesUploaded)
	require.Equal(t, uint64(42), run.Status.Full.LastAppliedIndex)
	require.Equal(t, uint64(40), run.Status.Full.LastLogSequence)
	require.Equal(t, uint64(41), run.Status.Full.LastAuditSequence)
	var jobs batchv1.JobList
	require.NoError(t, c.List(testCtx, &jobs, client.InNamespace(namespace)))
	require.Len(t, jobs.Items, 1)
	require.Equal(t, committedUID, jobs.Items[0].UID)
	require.Equal(t, 1, creates, "recovery must reuse the committed Job without another Create")
}
