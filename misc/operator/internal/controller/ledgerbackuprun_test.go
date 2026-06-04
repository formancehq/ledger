package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestHasRunningRun_TreatsPendingAndEmptyAsRunning(t *testing.T) {
	t.Parallel()

	runs := []ledgerv1alpha1.LedgerBackupRun{
		{
			Spec:   ledgerv1alpha1.LedgerBackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.LedgerBackupRunStatus{Phase: ledgerv1alpha1.BackupRunPhasePending},
		},
	}
	assert.True(t, hasRunningRun(runs, ledgerv1alpha1.BackupRunTypeFull), "Pending must block scheduling")

	runs = []ledgerv1alpha1.LedgerBackupRun{
		{
			Spec:   ledgerv1alpha1.LedgerBackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.LedgerBackupRunStatus{Phase: ""},
		},
	}
	assert.True(t, hasRunningRun(runs, ledgerv1alpha1.BackupRunTypeFull), "uninitialized phase must block scheduling")

	runs = []ledgerv1alpha1.LedgerBackupRun{
		{
			Spec:   ledgerv1alpha1.LedgerBackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.LedgerBackupRunStatus{Phase: ledgerv1alpha1.BackupRunPhaseSucceeded},
		},
	}
	assert.False(t, hasRunningRun(runs, ledgerv1alpha1.BackupRunTypeFull))
}

func TestHasRunningRun_IgnoresOtherTypes(t *testing.T) {
	t.Parallel()

	runs := []ledgerv1alpha1.LedgerBackupRun{
		{
			Spec:   ledgerv1alpha1.LedgerBackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeIncremental},
			Status: ledgerv1alpha1.LedgerBackupRunStatus{Phase: ledgerv1alpha1.BackupRunPhaseRunning},
		},
	}
	assert.False(t, hasRunningRun(runs, ledgerv1alpha1.BackupRunTypeFull),
		"a Running incremental should not block scheduling of a full")
}

func TestLatestSucceededRun_PicksMostRecent(t *testing.T) {
	t.Parallel()

	older := metav1.NewTime(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))
	newer := metav1.NewTime(time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC))

	runs := []ledgerv1alpha1.LedgerBackupRun{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "old"},
			Spec:       ledgerv1alpha1.LedgerBackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.LedgerBackupRunStatus{
				Phase:          ledgerv1alpha1.BackupRunPhaseSucceeded,
				CompletionTime: &older,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "new"},
			Spec:       ledgerv1alpha1.LedgerBackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.LedgerBackupRunStatus{
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

	mkRun := func(name string, completion time.Time) ledgerv1alpha1.LedgerBackupRun {
		t := metav1.NewTime(completion)

		return ledgerv1alpha1.LedgerBackupRun{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       ledgerv1alpha1.LedgerBackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.LedgerBackupRunStatus{
				Phase:          ledgerv1alpha1.BackupRunPhaseSucceeded,
				CompletionTime: &t,
			},
		}
	}

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	runs := []ledgerv1alpha1.LedgerBackupRun{
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
	runs := []ledgerv1alpha1.LedgerBackupRun{
		{
			Spec:   ledgerv1alpha1.LedgerBackupRunSpec{Type: ledgerv1alpha1.BackupRunTypeFull},
			Status: ledgerv1alpha1.LedgerBackupRunStatus{Phase: ledgerv1alpha1.BackupRunPhaseSucceeded, CompletionTime: &t1},
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
		run := &ledgerv1alpha1.LedgerBackupRun{
			Status: ledgerv1alpha1.LedgerBackupRunStatus{Phase: phase},
		}
		assert.Equal(t, expected, run.IsTerminal(), "phase %q", phase)
	}
}
