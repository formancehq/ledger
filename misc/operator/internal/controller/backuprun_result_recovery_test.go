package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestBackupRunResultReadRecovery(t *testing.T) {
	t.Parallel()
	for _, runType := range []ledgerv1alpha1.BackupRunType{ledgerv1alpha1.BackupRunTypeFull, ledgerv1alpha1.BackupRunTypeIncremental} {
		for _, missing := range []bool{false, true} {
			name := string(runType) + "/list-error"
			if missing {
				name = string(runType) + "/missing-pod"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				testBackupRunResultReadRecovery(t, runType, missing)
			})
		}
	}
}

func testBackupRunResultReadRecovery(t *testing.T, runType ledgerv1alpha1.BackupRunType, missing bool) {
	t.Helper()
	ctx := t.Context()
	c, run := backupRunRecoveryFixture(t)
	run.Spec.Type = runType
	require.NoError(t, c.Update(ctx, run))
	r := &BackupRunReconciler{Client: c, Scheme: c.Scheme()}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, req.NamespacedName, run))
	start := run.Status.StartTime.DeepCopy()
	job := &batchv1.Job{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: backupJobName(run)}, job))
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	require.NoError(t, c.Status().Update(ctx, job))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "result", Namespace: run.Namespace, Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name}},
		Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: backupJobContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: `{"filesUploaded":7,"totalFiles":7,"lastAppliedIndex":42,"logEntriesExported":13,"segmentsUploaded":2}`}}}}},
	}
	pods := k8sfake.NewClientset()
	if !missing {
		require.NoError(t, pods.Tracker().Add(pod))
	}
	attempts := 0
	pods.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		attempts++
		if !missing && attempts == 1 {
			return true, nil, context.DeadlineExceeded
		}

		return false, nil, nil
	})
	r.Clientset = pods
	_, err = r.Reconcile(ctx, req)
	if missing {
		require.ErrorContains(t, err, "no pod found")
	} else {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	require.NoError(t, c.Get(ctx, req.NamespacedName, run))
	require.Equal(t, ledgerv1alpha1.BackupRunPhaseRunning, run.Status.Phase)
	require.Nil(t, run.Status.CompletionTime)
	require.Nil(t, run.Status.Full)
	require.Nil(t, run.Status.Incremental)
	require.Nil(t, meta.FindStatusCondition(run.Status.Conditions, "BackupCompleted"))
	require.Contains(t, run.Status.Message, "reading backup result")
	sibling := &ledgerv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "sibling", Namespace: run.Namespace, Labels: run.Labels},
		Spec:       run.Spec,
	}
	require.NoError(t, c.Create(ctx, sibling))
	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(sibling)})
	require.NoError(t, err)
	require.Equal(t, concurrencyRequeue, result.RequeueAfter)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sibling), sibling))
	require.Equal(t, ledgerv1alpha1.BackupRunPhasePending, sibling.Status.Phase)
	require.True(t, apierrors.IsNotFound(c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: backupJobName(sibling)}, &batchv1.Job{})))
	if missing {
		require.NoError(t, pods.Tracker().Add(pod))
	}

	r = &BackupRunReconciler{Client: c, Scheme: c.Scheme(), Clientset: pods}
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, req.NamespacedName, run))
	require.Equal(t, ledgerv1alpha1.BackupRunPhaseSucceeded, run.Status.Phase)
	require.Equal(t, start, run.Status.StartTime)
	require.NotNil(t, run.Status.CompletionTime)
	require.Empty(t, run.Status.Message)
	if runType == ledgerv1alpha1.BackupRunTypeFull {
		require.NotNil(t, run.Status.Full)
		require.EqualValues(t, 7, run.Status.Full.FilesUploaded)
		require.EqualValues(t, 42, run.Status.Full.LastAppliedIndex)
	} else {
		require.NotNil(t, run.Status.Incremental)
		require.EqualValues(t, 13, run.Status.Incremental.LogEntriesExported)
		require.EqualValues(t, 2, run.Status.Incremental.SegmentsUploaded)
	}
	expectedAttempts := 2
	if missing {
		expectedAttempts = 3
	} // Empty modern and legacy selectors, then recovery.
	require.Equal(t, expectedAttempts, attempts)
	var jobs batchv1.JobList
	require.NoError(t, c.List(ctx, &jobs))
	require.Len(t, jobs.Items, 1)
	require.True(t, metav1.IsControlledBy(&jobs.Items[0], run))
}

func TestBackupRunResultRecoveryPreservesParentAccounting(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	c, run := backupRunRecoveryFixture(t)
	var backup ledgerv1alpha1.Backup
	key := client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.BackupRef}
	require.NoError(t, c.Get(ctx, key, &backup))
	backup.Spec.Schedule.Incremental = "* * * * *"
	backup.Spec.FailedRunsHistoryLimit = new(int32)
	backup.Spec.SuccessfulRunsHistoryLimit = new(int32)
	require.NoError(t, c.Update(ctx, &backup))
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
	r := &BackupRunReconciler{Client: c, Scheme: c.Scheme()}
	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	var job batchv1.Job
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: backupJobName(run)}, &job))
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	require.NoError(t, c.Status().Update(ctx, &job))
	pods := k8sfake.NewClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "result", Namespace: run.Namespace, Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name}},
		Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: backupJobContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: `{"filesUploaded":7,"totalFiles":7,"lastAppliedIndex":42}`}}}}},
	})
	attempts := 0
	pods.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		attempts++
		if attempts == 1 {
			return true, nil, context.DeadlineExceeded
		}

		return false, nil, nil
	})
	r.Clientset = pods
	_, err = r.Reconcile(ctx, req)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	parent := &BackupReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
	parentReq := ctrl.Request{NamespacedName: key}
	_, err = parent.Reconcile(ctx, parentReq)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, req.NamespacedName, run), "zero failed retention must preserve the recovering run")
	require.NoError(t, c.Get(ctx, key, &backup))
	require.Nil(t, backup.Status.LastFullBackup)
	require.Nil(t, backup.Status.LastFullRunCompletionTime)
	require.Nil(t, backup.Status.NextIncrementalBackupTime)
	var runs ledgerv1alpha1.BackupRunList
	require.NoError(t, c.List(ctx, &runs))
	require.Len(t, runs.Items, 1, "no incremental run before the full result is captured")

	r = &BackupRunReconciler{Client: c, Scheme: c.Scheme(), Clientset: pods}
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, req.NamespacedName, run))
	require.Equal(t, ledgerv1alpha1.BackupRunPhaseSucceeded, run.Status.Phase)
	parent = &BackupReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
	_, err = parent.Reconcile(ctx, parentReq)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, &backup))
	require.NotNil(t, backup.Status.LastFullBackup)
	require.EqualValues(t, 7, backup.Status.LastFullBackup.FilesUploaded)
	require.EqualValues(t, 42, backup.Status.LastFullBackup.LastAppliedIndex)
	require.Equal(t, run.Status.CompletionTime, backup.Status.LastFullRunCompletionTime)
	require.NotNil(t, backup.Status.NextIncrementalBackupTime)
	require.True(t, apierrors.IsNotFound(c.Get(ctx, req.NamespacedName, run)), "successful retention zero prunes only after parent status captures the result")
	require.NoError(t, c.List(ctx, &runs))
	require.Len(t, runs.Items, 1)
	require.Equal(t, ledgerv1alpha1.BackupRunTypeIncremental, runs.Items[0].Spec.Type)
	require.Equal(t, 2, attempts)
}

func TestBackupRunDefinitiveResultFailure(t *testing.T) {
	t.Parallel()
	for _, failedJob := range []bool{false, true} {
		name := "malformed-result"
		if failedJob {
			name = "failed-job"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			c, run := backupRunRecoveryFixture(t)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
			r := &BackupRunReconciler{Client: c, Scheme: c.Scheme()}
			_, err := r.Reconcile(ctx, req)
			require.NoError(t, err)
			var job batchv1.Job
			require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: backupJobName(run)}, &job))
			condition := batchv1.JobComplete
			message := "no JSON summary found"
			if failedJob {
				condition = batchv1.JobFailed
				message = "backup execution failed"
			}
			job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue, Message: message}}
			require.NoError(t, c.Status().Update(ctx, &job))
			r.Clientset = k8sfake.NewClientset(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "result", Namespace: run.Namespace, Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name}},
				Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: backupJobContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: "invalid summary"}}}}},
			})
			_, err = r.Reconcile(ctx, req)
			require.NoError(t, err)
			require.NoError(t, c.Get(ctx, req.NamespacedName, run))
			require.Equal(t, ledgerv1alpha1.BackupRunPhaseFailed, run.Status.Phase)
			require.NotNil(t, run.Status.CompletionTime)
			require.Contains(t, run.Status.Message, message)
			require.Nil(t, run.Status.Full)
			require.Nil(t, run.Status.Incremental)
			conditionStatus := meta.FindStatusCondition(run.Status.Conditions, "BackupCompleted")
			require.NotNil(t, conditionStatus)
			require.Equal(t, metav1.ConditionFalse, conditionStatus.Status)
		})
	}
}

func TestBackupRunResultDiagnosticWriteRecovery(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	c, run := backupRunRecoveryFixture(t)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
	r := &BackupRunReconciler{Client: c, Scheme: c.Scheme()}
	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	var job batchv1.Job
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: backupJobName(run)}, &job))
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	require.NoError(t, c.Status().Update(ctx, &job))
	pods := k8sfake.NewClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "result", Namespace: run.Namespace, Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name}},
		Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: backupJobContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: `{"filesUploaded":7,"lastAppliedIndex":42}`}}}}},
	})
	attempts := 0
	pods.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		attempts++
		if attempts <= 3 {
			return true, nil, context.DeadlineExceeded
		}

		return false, nil, nil
	})
	writes := 0
	writeErr := errors.New("diagnostic status write failed")
	wrapped := interceptor.NewClient(c, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, name string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			writes++
			if writes == 1 {
				return writeErr
			}

			return c.SubResource(name).Update(ctx, obj, opts...)
		},
	})
	r = &BackupRunReconciler{Client: wrapped, Scheme: c.Scheme(), Clientset: pods}
	_, err = r.Reconcile(ctx, req)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, writeErr)
	require.NoError(t, c.Get(ctx, req.NamespacedName, run))
	require.Equal(t, ledgerv1alpha1.BackupRunPhaseRunning, run.Status.Phase)
	require.Empty(t, run.Status.Message)
	require.Nil(t, run.Status.CompletionTime)

	r = &BackupRunReconciler{Client: wrapped, Scheme: c.Scheme(), Clientset: pods}
	_, err = r.Reconcile(ctx, req)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, c.Get(ctx, req.NamespacedName, run))
	require.Contains(t, run.Status.Message, "listing pods")
	version := run.ResourceVersion
	_, err = r.Reconcile(ctx, req)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, c.Get(ctx, req.NamespacedName, run))
	require.Equal(t, version, run.ResourceVersion, "unchanged diagnostic must not rewrite status")
	require.Equal(t, 2, writes)

	r = &BackupRunReconciler{Client: wrapped, Scheme: c.Scheme(), Clientset: pods}
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, req.NamespacedName, run))
	require.Equal(t, ledgerv1alpha1.BackupRunPhaseSucceeded, run.Status.Phase)
	require.Empty(t, run.Status.Message)
	require.EqualValues(t, 7, run.Status.Full.FilesUploaded)
	require.EqualValues(t, 42, run.Status.Full.LastAppliedIndex)
	require.Equal(t, 4, attempts)
	require.Equal(t, 3, writes)
}
