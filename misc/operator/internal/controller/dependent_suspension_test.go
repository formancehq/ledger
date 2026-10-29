package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// No REST config or Clientset is supplied: an exec or result read would fail.
func TestDependentControllersPreserveStateWhileClusterDisabled(t *testing.T) {
	t.Parallel()
	for _, deleting := range []bool{false, true} {
		name := "active"
		if deleting {
			name = "deleting"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			scheme := runtime.NewScheme()
			require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
			require.NoError(t, appsv1.AddToScheme(scheme))
			require.NoError(t, batchv1.AddToScheme(scheme))
			cluster := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "test"}}
			cluster.Spec.Disabled = true
			ledger := newLedger("ledger", "test", "cluster", "orders")
			ledger.Finalizers = []string{ledgerFinalizer}
			ledger.Status.Phase = ledgerv1alpha1.LedgerPhaseReady
			ledger.Status.AppliedSpecHash = "retain-attribution"
			sink := testEventSink()
			sink.Namespace = "test"
			sink.Finalizers = []string{eventSinkFinalizer}
			sink.Status.Cursor = 42
			if deleting {
				now := metav1.Now()
				ledger.DeletionTimestamp = &now
				sink.DeletionTimestamp = &now
			}
			backup := &ledgerv1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "backup", Namespace: "test"}, Spec: ledgerv1alpha1.BackupSpec{ClusterRef: "cluster", Schedule: ledgerv1alpha1.BackupSchedule{Full: "* * * * *"}}}
			cursor := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			backup.Status.LastFullRunCompletionTime = &cursor
			run := &ledgerv1alpha1.BackupRun{ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "test"}, Spec: ledgerv1alpha1.BackupRunSpec{BackupRef: "backup", Type: ledgerv1alpha1.BackupRunTypeFull}, Status: ledgerv1alpha1.BackupRunStatus{Phase: ledgerv1alpha1.BackupRunPhaseRunning, StartTime: &cursor}}
			// Completed execution evidence stays uninterpreted until recovery.
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: backupJobName(run), Namespace: "test"}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}}}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster, ledger, sink, backup, run).WithObjects(cluster, ledger, sink, backup, run, job).Build()
			// Compare persisted state after Kubernetes timestamp serialization.
			require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(backup), backup))
			require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(run), run))
			request := func(obj client.Object) ctrl.Request {
				return ctrl.Request{NamespacedName: client.ObjectKeyFromObject(obj)}
			}
			result, err := (&LedgerReconciler{Client: kube, Scheme: scheme}).Reconcile(t.Context(), request(ledger))
			require.NoError(t, err)
			require.Positive(t, result.RequeueAfter)
			result, err = (&EventSinkReconciler{Client: kube, Scheme: scheme}).Reconcile(t.Context(), request(sink))
			require.NoError(t, err)
			require.Positive(t, result.RequeueAfter)
			result, err = (&BackupReconciler{Client: kube, APIReader: kube, Scheme: scheme}).Reconcile(t.Context(), request(backup))
			require.NoError(t, err)
			require.Positive(t, result.RequeueAfter)
			result, err = (&BackupRunReconciler{Client: kube, Scheme: scheme}).Reconcile(t.Context(), request(run))
			require.NoError(t, err)
			require.Positive(t, result.RequeueAfter)
			result, err = (&VolumeExpansionReconciler{Client: kube}).Reconcile(t.Context(), request(cluster))
			require.NoError(t, err)
			require.Positive(t, result.RequeueAfter)
			var storedLedger ledgerv1alpha1.Ledger
			require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(ledger), &storedLedger))
			require.Equal(t, ledger.Status, storedLedger.Status)
			require.Equal(t, ledger.Finalizers, storedLedger.Finalizers)
			var storedSink ledgerv1alpha1.EventSink
			require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(sink), &storedSink))
			require.Equal(t, sink.Status, storedSink.Status)
			require.Equal(t, sink.Finalizers, storedSink.Finalizers)
			var storedBackup ledgerv1alpha1.Backup
			require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(backup), &storedBackup))
			require.Equal(t, backup.Status, storedBackup.Status)
			var storedRun ledgerv1alpha1.BackupRun
			require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(run), &storedRun))
			require.Equal(t, run.Status, storedRun.Status)
			var jobs batchv1.JobList
			require.NoError(t, kube.List(t.Context(), &jobs))
			require.Len(t, jobs.Items, 1)
		})
	}
}

func TestBackupSchedulingResumesAfterRepeatedClusterPause(t *testing.T) {
	t.Parallel()
	fixture := newBackupSchedulingFixture(t)
	require.NoError(t, appsv1.AddToScheme(fixture.scheme))
	now := metav1.Now()
	for i := range 2 {
		var cluster ledgerv1alpha1.Cluster
		key := client.ObjectKey{Namespace: "test", Name: "ledger"}
		require.NoError(t, fixture.client.Get(t.Context(), key, &cluster))
		cluster.Spec.Disabled = true
		require.NoError(t, fixture.client.Update(t.Context(), &cluster))
		fixture.reconcile(t, now.Time)
		require.Empty(t, fixture.runs(t), "pause must not queue a new scheduled run")
		require.NoError(t, fixture.client.Get(t.Context(), key, &cluster))
		cluster.Spec.Disabled = false
		require.NoError(t, fixture.client.Update(t.Context(), &cluster))
		fixture.reconcile(t, now.Time)
		runs := fixture.runs(t)
		require.Len(t, runs, 1, "resume schedules exactly one execution")
		if i == 0 {
			// Remove the pending execution to exercise a second pause/resume cycle.
			require.NoError(t, fixture.client.Delete(t.Context(), &runs[0]))
		}
	}
}
