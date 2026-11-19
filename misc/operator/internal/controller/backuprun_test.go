package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestIncrementalBackupRunDoesNotStartNewJob(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	assert.NoError(t, batchv1.AddToScheme(scheme))
	assert.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &BackupRunReconciler{Client: c, APIReader: c, Scheme: scheme}
	run := &ledgerv1alpha1.BackupRun{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "test"}, Spec: ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeIncremental}}
	_, err := r.ensureBackupJob(context.Background(), run, &ledgerv1alpha1.Backup{}, &ledgerv1alpha1.Cluster{})
	assert.ErrorContains(t, err, "create a Full BackupRun")
}

func TestIncrementalBackupRunObservesUncachedExistingJob(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	assert.NoError(t, batchv1.AddToScheme(scheme))
	assert.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	run := &ledgerv1alpha1.BackupRun{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "test", UID: "run-uid"}, Spec: ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeIncremental}}
	controller := true
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: backupJobName(run), Namespace: run.Namespace,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "ledger.formance.com/v1alpha1", Kind: "BackupRun", Name: run.Name, UID: run.UID, Controller: &controller}},
	}}
	cache := fake.NewClientBuilder().WithScheme(scheme).Build()
	live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(job).Build()
	r := &BackupRunReconciler{Client: cache, APIReader: live, Scheme: scheme}
	got, err := r.ensureBackupJob(context.Background(), run, &ledgerv1alpha1.Backup{}, &ledgerv1alpha1.Cluster{})
	assert.NoError(t, err)
	assert.Equal(t, job.Name, got.Name)
}

func TestLegacyIncrementalMigrationKeepsCheckpointCursorAndSummary(t *testing.T) {
	t.Parallel()
	older := metav1.NewTime(time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC))
	newer := metav1.NewTime(older.Add(time.Hour))
	runs := []ledgerv1alpha1.BackupRun{
		{Spec: ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull}, Status: ledgerv1alpha1.BackupRunStatus{Phase: ledgerv1alpha1.BackupRunPhaseSucceeded, CompletionTime: &older, Full: &ledgerv1alpha1.FullBackupStatus{TotalFiles: 1}}},
		{Spec: ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeIncremental}, Status: ledgerv1alpha1.BackupRunStatus{Phase: ledgerv1alpha1.BackupRunPhaseFailed, CompletionTime: &newer, Message: "provisioning backup Job: " + legacyIncrementalRunMessage}},
	}
	backup := &ledgerv1alpha1.Backup{}
	r := &BackupReconciler{}
	r.refreshCompletionCursors(backup, runs)
	assert.Nil(t, backup.Status.LastIncrementalRunCompletionTime, "rejected run must not delay the replacement checkpoint")
	assert.Equal(t, older, *backup.Status.LastFullRunCompletionTime)

	runs[1].Status = ledgerv1alpha1.BackupRunStatus{Phase: ledgerv1alpha1.BackupRunPhaseSucceeded, CompletionTime: &newer, Full: &ledgerv1alpha1.FullBackupStatus{TotalFiles: 5}}
	r.refreshStatusSummaries(backup, runs)
	assert.EqualValues(t, 5, backup.Status.LastFullBackup.TotalFiles)
}

func TestHasRunningRun_TreatsPendingAndEmptyAsRunning(t *testing.T) {
	t.Parallel()

	runs := []ledgerv1alpha1.BackupRun{
		{
			Spec:   ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.BackupRunStatus{Phase: ledgerv1alpha1.BackupRunPhasePending},
		},
	}
	assert.True(t, hasRunningRun(runs, ledgerv1alpha1.BackupRunTypeFull), "Pending must block scheduling")

	runs = []ledgerv1alpha1.BackupRun{
		{
			Spec:   ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.BackupRunStatus{Phase: ""},
		},
	}
	assert.True(t, hasRunningRun(runs, ledgerv1alpha1.BackupRunTypeFull), "uninitialized phase must block scheduling")

	runs = []ledgerv1alpha1.BackupRun{
		{
			Spec:   ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.BackupRunStatus{Phase: ledgerv1alpha1.BackupRunPhaseSucceeded},
		},
	}
	assert.False(t, hasRunningRun(runs, ledgerv1alpha1.BackupRunTypeFull))
}

func TestHasRunningRun_IgnoresOtherTypes(t *testing.T) {
	t.Parallel()

	runs := []ledgerv1alpha1.BackupRun{
		{
			Spec:   ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeIncremental},
			Status: ledgerv1alpha1.BackupRunStatus{Phase: ledgerv1alpha1.BackupRunPhaseRunning},
		},
	}
	assert.False(t, hasRunningRun(runs, ledgerv1alpha1.BackupRunTypeFull),
		"a Running incremental should not block scheduling of a full")
}

func TestLatestSucceededRun_PicksMostRecent(t *testing.T) {
	t.Parallel()

	older := metav1.NewTime(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))
	newer := metav1.NewTime(time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC))

	runs := []ledgerv1alpha1.BackupRun{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "old"},
			Spec:       ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.BackupRunStatus{
				Phase:          ledgerv1alpha1.BackupRunPhaseSucceeded,
				CompletionTime: &older,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "new"},
			Spec:       ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.BackupRunStatus{
				Phase:          ledgerv1alpha1.BackupRunPhaseSucceeded,
				CompletionTime: &newer,
			},
		},
	}

	latest := latestSucceededRun(runs, ledgerv1alpha1.BackupRunTypeFull)
	if assert.NotNil(t, latest) {
		assert.Equal(t, "new", latest.Name)
	}
}

func TestExcessRuns_KeepsLimitAndReturnsOldestFirst(t *testing.T) {
	t.Parallel()

	mkRun := func(name string, completion time.Time) ledgerv1alpha1.BackupRun {
		t := metav1.NewTime(completion)

		return ledgerv1alpha1.BackupRun{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.BackupRunStatus{
				Phase:          ledgerv1alpha1.BackupRunPhaseSucceeded,
				CompletionTime: &t,
			},
		}
	}

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	runs := []ledgerv1alpha1.BackupRun{
		mkRun("a", base),
		mkRun("b", base.Add(time.Minute)),
		mkRun("c", base.Add(2*time.Minute)),
		mkRun("d", base.Add(3*time.Minute)),
		mkRun("e", base.Add(4*time.Minute)),
	}

	excess := excessRuns(runs, ledgerv1alpha1.BackupRunTypeFull, ledgerv1alpha1.BackupRunPhaseSucceeded, 2)
	if assert.Len(t, excess, 3) {
		// Oldest first: a, b, c
		assert.Equal(t, "a", excess[0].Name)
		assert.Equal(t, "b", excess[1].Name)
		assert.Equal(t, "c", excess[2].Name)
	}
}

func TestExcessRuns_UnderLimitReturnsNone(t *testing.T) {
	t.Parallel()

	t1 := metav1.NewTime(time.Now())
	runs := []ledgerv1alpha1.BackupRun{
		{
			Spec:   ledgerv1alpha1.BackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.BackupRunStatus{Phase: ledgerv1alpha1.BackupRunPhaseSucceeded, CompletionTime: &t1},
		},
	}

	assert.Empty(t, excessRuns(runs, ledgerv1alpha1.BackupRunTypeFull, ledgerv1alpha1.BackupRunPhaseSucceeded, 3))
}

func TestIsTerminal(t *testing.T) {
	t.Parallel()

	cases := map[ledgerv1alpha1.BackupRunPhase]bool{
		"":                                     false,
		ledgerv1alpha1.BackupRunPhasePending:   false,
		ledgerv1alpha1.BackupRunPhaseRunning:   false,
		ledgerv1alpha1.BackupRunPhaseSucceeded: true,
		ledgerv1alpha1.BackupRunPhaseFailed:    true,
	}

	for phase, expected := range cases {
		run := &ledgerv1alpha1.BackupRun{
			Status: ledgerv1alpha1.BackupRunStatus{Phase: phase},
		}
		assert.Equal(t, expected, run.IsTerminal(), "phase %q", phase)
	}
}
