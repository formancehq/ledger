package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// Each fixture owns its API state and supplies reconciliation time explicitly.
// A fresh reconciler must recover exclusively from the persisted objects.
type backupSchedulingFixture struct {
	client client.WithWatch
	scheme *runtime.Scheme
	key    types.NamespacedName
}

func newBackupSchedulingFixture(t *testing.T) *backupSchedulingFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	zero := int32(0)
	backup := &ledgerv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "daily", Namespace: "test", UID: "backup-uid"},
		Spec: ledgerv1alpha1.BackupSpec{
			ClusterRef:                 "ledger",
			Schedule:                   ledgerv1alpha1.BackupSchedule{Full: "CRON_TZ=UTC 0 2 * * *"},
			SuccessfulRunsHistoryLimit: &zero,
			FailedRunsHistoryLimit:     &zero,
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&ledgerv1alpha1.Backup{}, &ledgerv1alpha1.BackupRun{}).
		WithObjects(backup, &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "ledger", Namespace: "test"}}).Build()

	return &backupSchedulingFixture{client: c, scheme: scheme, key: client.ObjectKeyFromObject(backup)}
}

func (f *backupSchedulingFixture) reconcile(t *testing.T, now time.Time) {
	t.Helper()
	r := &BackupReconciler{Client: f.client, APIReader: f.client, Scheme: f.scheme}
	_, err := r.reconcile(context.Background(), ctrl.Request{NamespacedName: f.key}, now)
	require.NoError(t, err)
}

func (f *backupSchedulingFixture) runs(t *testing.T) []ledgerv1alpha1.BackupRun {
	t.Helper()
	var runs ledgerv1alpha1.BackupRunList
	require.NoError(t, f.client.List(context.Background(), &runs, client.InNamespace(f.key.Namespace)))

	return runs.Items
}

func (f *backupSchedulingFixture) backup(t *testing.T) *ledgerv1alpha1.Backup {
	t.Helper()
	var backup ledgerv1alpha1.Backup
	require.NoError(t, f.client.Get(context.Background(), f.key, &backup))

	return &backup
}

func (f *backupSchedulingFixture) complete(t *testing.T, run ledgerv1alpha1.BackupRun, phase ledgerv1alpha1.BackupRunPhase, at time.Time) {
	t.Helper()
	completion := metav1.NewTime(at)
	run.Status.Phase = phase
	run.Status.CompletionTime = &completion
	if phase == ledgerv1alpha1.BackupRunPhaseSucceeded {
		if run.Spec.Type == ledgerv1alpha1.BackupRunTypeFull {
			run.Status.Full = &ledgerv1alpha1.FullBackupStatus{Time: &completion, FilesUploaded: 7}
		} else {
			run.Status.Incremental = &ledgerv1alpha1.IncrementalBackupStatus{Time: &completion}
		}
	}
	require.NoError(t, f.client.Status().Update(context.Background(), &run))
}

func TestBackupScheduling_ZeroRetentionRestart(t *testing.T) {
	t.Parallel()
	for _, phase := range []ledgerv1alpha1.BackupRunPhase{ledgerv1alpha1.BackupRunPhaseSucceeded, ledgerv1alpha1.BackupRunPhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			f := newBackupSchedulingFixture(t)
			now := time.Date(2026, 9, 10, 2, 0, 0, 0, time.UTC)
			for range 2 {
				f.reconcile(t, now)
				runs := f.runs(t)
				require.Len(t, runs, 1, "initial execution or due cron must create exactly one run")
				f.complete(t, runs[0], phase, now.Add(time.Minute))
				f.reconcile(t, now.Add(2*time.Minute))
				require.Empty(t, f.runs(t), "zero retention must prune the terminal run")
				backup := f.backup(t)
				next := now.Add(24 * time.Hour)
				require.NotNil(t, backup.Status.NextFullBackupTime)
				require.True(t, next.Equal(backup.Status.NextFullBackupTime.Time))
				if phase == ledgerv1alpha1.BackupRunPhaseSucceeded {
					require.NotNil(t, backup.Status.LastFullBackup)
					require.EqualValues(t, 7, backup.Status.LastFullBackup.FilesUploaded)
				} else {
					require.Nil(t, backup.Status.LastFullBackup, "failed-only runs must not fabricate a success summary")
				}
				f.reconcile(t, now.Add(3*time.Minute))
				require.Empty(t, f.runs(t), "restart after pruning must not recreate a run before the persisted deadline")
				f.reconcile(t, next.Add(-time.Second))
				require.Empty(t, f.runs(t))
				now = next
			}
		})
	}
}

func TestBackupScheduling_PersistBeforePruning(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"status", "delete"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			f := newBackupSchedulingFixture(t)
			now := time.Date(2026, 9, 10, 2, 0, 0, 0, time.UTC)
			f.reconcile(t, now)
			run := f.runs(t)[0]
			completion := now.Add(time.Minute)
			f.complete(t, run, ledgerv1alpha1.BackupRunPhaseSucceeded, completion)
			failure := errors.New("injected API failure")
			statusAttempts, deleteAttempts := 0, 0
			f.client = interceptor.NewClient(f.client, interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, c client.Client, subResource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if _, ok := obj.(*ledgerv1alpha1.Backup); ok && subResource == "status" {
						statusAttempts++
						if operation == "status" && statusAttempts == 1 {
							return failure
						}
					}

					return c.SubResource(subResource).Update(ctx, obj, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					deleteAttempts++
					// Every delete must follow a durable status write, never just an in-memory update.
					backup := f.backup(t)
					require.NotNil(t, backup.Status.LastFullRunCompletionTime)
					require.True(t, completion.Equal(backup.Status.LastFullRunCompletionTime.Time))
					require.NotNil(t, backup.Status.LastFullBackup)
					if operation == "delete" && deleteAttempts == 1 {
						return failure
					}

					return c.Delete(ctx, obj, opts...)
				},
			})
			r := &BackupReconciler{Client: f.client, APIReader: f.client, Scheme: f.scheme}
			_, err := r.reconcile(context.Background(), ctrl.Request{NamespacedName: f.key}, now.Add(2*time.Minute))
			require.ErrorIs(t, err, failure)
			require.Len(t, f.runs(t), 1, "failed status/pruning attempt preserves the terminal child")
			if operation == "status" {
				require.Zero(t, deleteAttempts)
				require.Nil(t, f.backup(t).Status.LastFullRunCompletionTime)
				require.Nil(t, f.backup(t).Status.LastFullBackup)
			} else {
				require.Equal(t, 1, deleteAttempts)
				require.NotNil(t, f.backup(t).Status.LastFullRunCompletionTime)
			}
			f.reconcile(t, now.Add(3*time.Minute))
			require.Equal(t, 2, statusAttempts, "retry must persist status successfully")
			expectedDeletes := 1
			if operation == "delete" {
				expectedDeletes = 2
			}
			require.Equal(t, expectedDeletes, deleteAttempts)
			require.Empty(t, f.runs(t))
			f.reconcile(t, now.Add(4*time.Minute))
			require.Empty(t, f.runs(t), "restart after retry/pruning must retain scheduling progress")
		})
	}
}

func TestBackupScheduling_ActiveRunBlocksAfterRestart(t *testing.T) {
	t.Parallel()
	for _, phase := range []ledgerv1alpha1.BackupRunPhase{"", ledgerv1alpha1.BackupRunPhasePending, ledgerv1alpha1.BackupRunPhaseRunning} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			f := newBackupSchedulingFixture(t)
			now := time.Date(2026, 9, 10, 2, 0, 0, 0, time.UTC)
			f.reconcile(t, now)
			run := f.runs(t)[0]
			f.complete(t, run, ledgerv1alpha1.BackupRunPhaseSucceeded, now.Add(time.Minute))
			f.reconcile(t, now.Add(2*time.Minute))
			require.Empty(t, f.runs(t))
			f.reconcile(t, now.Add(24*time.Hour))
			runs := f.runs(t)
			require.Len(t, runs, 1)
			run = runs[0]
			run.Status.Phase = phase
			require.NoError(t, f.client.Status().Update(context.Background(), &run))
			// The completion cursor is overdue, but this active run still forbids another.
			f.reconcile(t, now.Add(48*time.Hour))
			runs = f.runs(t)
			require.Len(t, runs, 1)
			require.Equal(t, run.Name, runs[0].Name)
			require.True(t, now.Add(time.Minute).Equal(f.backup(t).Status.LastFullRunCompletionTime.Time))
		})
	}
}

func TestBackupScheduling_IncrementalZeroRetentionRestart(t *testing.T) {
	t.Parallel()
	for _, phase := range []ledgerv1alpha1.BackupRunPhase{ledgerv1alpha1.BackupRunPhaseSucceeded, ledgerv1alpha1.BackupRunPhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			f := newBackupSchedulingFixture(t)
			backup := f.backup(t)
			backup.Spec.Schedule.Incremental = "CRON_TZ=UTC 0 * * * *"
			require.NoError(t, f.client.Update(context.Background(), backup))
			now := time.Date(2026, 9, 10, 2, 0, 0, 0, time.UTC)
			f.reconcile(t, now)
			runs := f.runs(t)
			require.Len(t, runs, 1, "incremental requires a successful full")
			require.Equal(t, ledgerv1alpha1.BackupRunTypeFull, runs[0].Spec.Type)
			f.complete(t, runs[0], ledgerv1alpha1.BackupRunPhaseSucceeded, now.Add(time.Minute))
			f.reconcile(t, now.Add(2*time.Minute))
			runs = f.runs(t)
			require.Len(t, runs, 1, "full is pruned while first incremental starts")
			require.Equal(t, ledgerv1alpha1.BackupRunTypeIncremental, runs[0].Spec.Type)
			f.complete(t, runs[0], phase, now.Add(3*time.Minute))
			f.reconcile(t, now.Add(4*time.Minute))
			require.Empty(t, f.runs(t))
			f.reconcile(t, now.Add(5*time.Minute))
			require.Empty(t, f.runs(t))
			backup = f.backup(t)
			require.NotNil(t, backup.Status.NextIncrementalBackupTime, "pruning full must not disable incrementals")
			require.True(t, now.Add(time.Hour).Equal(backup.Status.NextIncrementalBackupTime.Time))
			require.True(t, now.Add(time.Minute).Equal(backup.Status.LastFullRunCompletionTime.Time))
			require.True(t, now.Add(3*time.Minute).Equal(backup.Status.LastIncrementalRunCompletionTime.Time))
			if phase == ledgerv1alpha1.BackupRunPhaseFailed {
				require.Nil(t, backup.Status.LastIncrementalBackup)
			} else {
				require.NotNil(t, backup.Status.LastIncrementalBackup)
			}
			f.reconcile(t, now.Add(time.Hour))
			runs = f.runs(t)
			require.Len(t, runs, 1)
			require.Equal(t, ledgerv1alpha1.BackupRunTypeIncremental, runs[0].Spec.Type)
		})
	}
}

func TestBackupScheduling_DisabledScheduleAndOlderHistory(t *testing.T) {
	t.Parallel()
	f := newBackupSchedulingFixture(t)
	now := time.Date(2026, 9, 10, 2, 0, 0, 0, time.UTC)
	f.reconcile(t, now)
	runs := f.runs(t)
	require.Len(t, runs, 1)
	// Keep an older successful child while pruning the latest failed child.
	older := runs[0].DeepCopy()
	older.Name = "older-success"
	older.ResourceVersion = ""
	require.NoError(t, f.client.Create(context.Background(), older))
	f.complete(t, *older, ledgerv1alpha1.BackupRunPhaseSucceeded, now.Add(-24*time.Hour))
	f.complete(t, runs[0], ledgerv1alpha1.BackupRunPhaseFailed, now.Add(time.Minute))
	backup := f.backup(t)
	one := int32(1)
	backup.Spec.SuccessfulRunsHistoryLimit = &one
	backup.Spec.Schedule.Full = ""
	require.NoError(t, f.client.Update(context.Background(), backup))
	f.reconcile(t, now.Add(2*time.Minute))
	backup = f.backup(t)
	require.Nil(t, backup.Status.NextFullBackupTime)
	require.True(t, now.Add(time.Minute).Equal(backup.Status.LastFullRunCompletionTime.Time))
	require.Len(t, f.runs(t), 1)
	// Changing/enabling the schedule uses the completion cursor, not a stored next deadline.
	backup.Spec.Schedule.Full = "CRON_TZ=UTC 0 4 * * *"
	require.NoError(t, f.client.Update(context.Background(), backup))
	f.reconcile(t, now.Add(3*time.Minute))
	require.Len(t, f.runs(t), 1, "older retained success must not rewind the failure cursor")
	backup = f.backup(t)
	require.True(t, now.Add(2*time.Hour).Equal(backup.Status.NextFullBackupTime.Time))
	require.True(t, now.Add(time.Minute).Equal(backup.Status.LastFullRunCompletionTime.Time))
	f.reconcile(t, now.Add(2*time.Hour))
	require.Len(t, f.runs(t), 2, "new cron deadline must create a run alongside retained history")
}

func TestBackupScheduling_StaleParentCacheAfterPruning(t *testing.T) {
	t.Parallel()
	for _, phase := range []ledgerv1alpha1.BackupRunPhase{ledgerv1alpha1.BackupRunPhaseSucceeded, ledgerv1alpha1.BackupRunPhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			f := newBackupSchedulingFixture(t)
			now := time.Date(2026, 9, 10, 2, 0, 0, 0, time.UTC)
			f.reconcile(t, now)
			staleBackup := f.backup(t)
			f.complete(t, f.runs(t)[0], phase, now.Add(time.Minute))
			f.reconcile(t, now.Add(2*time.Minute))
			require.Empty(t, f.runs(t))
			// Child deletion has reached its informer, but the Backup informer still
			// exposes the object from before the completion cursor was persisted.
			staleClient := interceptor.NewClient(f.client, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if backup, ok := obj.(*ledgerv1alpha1.Backup); ok {
						*backup = *staleBackup.DeepCopy()

						return nil
					}

					return c.Get(ctx, key, obj, opts...)
				},
			})
			r := &BackupReconciler{Client: staleClient, APIReader: f.client, Scheme: f.scheme}
			_, err := r.reconcile(context.Background(), ctrl.Request{NamespacedName: f.key}, now.Add(3*time.Minute))
			require.Empty(t, f.runs(t), "a stale parent cache must not create a run before discovering the status conflict")
			require.NoError(t, err)
		})
	}
}

func TestBackupScheduling_APIReadFailureDoesNotSchedule(t *testing.T) {
	t.Parallel()
	f := newBackupSchedulingFixture(t)
	failure := errors.New("injected uncached read failure")
	attempts := 0
	reader := interceptor.NewClient(f.client, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			attempts++
			if attempts == 1 {
				return failure
			}

			return c.Get(ctx, key, obj, opts...)
		},
	})
	r := &BackupReconciler{Client: f.client, APIReader: reader, Scheme: f.scheme}
	now := time.Date(2026, 9, 10, 2, 0, 0, 0, time.UTC)
	_, err := r.reconcile(context.Background(), ctrl.Request{NamespacedName: f.key}, now)
	require.ErrorIs(t, err, failure)
	require.Empty(t, f.runs(t), "must not fall back to cached state on an API read failure")
	_, err = r.reconcile(context.Background(), ctrl.Request{NamespacedName: f.key}, now)
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.Len(t, f.runs(t), 1)
}
