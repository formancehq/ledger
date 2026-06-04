//go:build integration

package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestBackupReconcile_MissingLedgerService(t *testing.T) {
	ns := createTestNamespace(t)

	backup := newLedgerBackup("bk-missing", ns, "nonexistent-service")
	require.NoError(t, k8sClient.Create(ctx, backup))

	requireEventually(t, func() bool {
		var got ledgerv1alpha1.LedgerBackup
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "bk-missing", Namespace: ns}, &got); err != nil {
			return false
		}
		return got.Status.Phase == ledgerv1alpha1.BackupPhaseFailed
	}, "backup should fail when LedgerService does not exist")

	var got ledgerv1alpha1.LedgerBackup
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "bk-missing", Namespace: ns}, &got))
	assert.Contains(t, got.Status.Message, "not found")
}

func TestBackupReconcile_InvalidSchedule(t *testing.T) {
	ns := createTestNamespace(t)

	ls := newLedgerService("bk-svc", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	backup := newLedgerBackup("bk-invalid-sched", ns, "bk-svc")
	backup.Spec.Schedule.Full = "not a cron"
	backup.Spec.Schedule.Incremental = ""
	require.NoError(t, k8sClient.Create(ctx, backup))

	requireEventually(t, func() bool {
		var got ledgerv1alpha1.LedgerBackup
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "bk-invalid-sched", Namespace: ns}, &got); err != nil {
			return false
		}
		return got.Status.Phase == ledgerv1alpha1.BackupPhaseFailed
	}, "backup should fail with invalid cron schedule")

	var got ledgerv1alpha1.LedgerBackup
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "bk-invalid-sched", Namespace: ns}, &got))
	assert.Contains(t, got.Status.Message, "invalid full backup schedule")
}

// TestBackupReconcile_NoScheduleAllowed verifies that a LedgerBackup without any
// schedule is valid (reaches Ready phase). It acts as a backup configuration
// template for manual LedgerBackupRun resources.
func TestBackupReconcile_NoScheduleAllowed(t *testing.T) {
	ns := createTestNamespace(t)

	ls := newLedgerService("bk-svc2", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	backup := newLedgerBackup("bk-no-sched", ns, "bk-svc2")
	backup.Spec.Schedule.Full = ""
	backup.Spec.Schedule.Incremental = ""
	require.NoError(t, k8sClient.Create(ctx, backup))

	requireEventually(t, func() bool {
		var got ledgerv1alpha1.LedgerBackup
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "bk-no-sched", Namespace: ns}, &got); err != nil {
			return false
		}
		return got.Status.Phase == ledgerv1alpha1.BackupPhaseReady
	}, "backup without schedule should reach Ready phase")
}

// TestBackupReconcile_ScheduleCreatesFullRun verifies that when a due full schedule fires,
// the LedgerBackupReconciler creates a child LedgerBackupRun with the right owner reference
// and labels (instead of running ledgerctl inline).
func TestBackupReconcile_ScheduleCreatesFullRun(t *testing.T) {
	ns := createTestNamespace(t)

	ls := newLedgerService("bk-svc-sched", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	backup := newLedgerBackup("bk-sched", ns, "bk-svc-sched")
	// "Every minute" so the first tick is due immediately (no prior run).
	backup.Spec.Schedule.Full = "* * * * *"
	backup.Spec.Schedule.Incremental = ""
	require.NoError(t, k8sClient.Create(ctx, backup))

	requireEventually(t, func() bool {
		runs := &ledgerv1alpha1.LedgerBackupRunList{}
		if err := k8sClient.List(ctx, runs,
			client.InNamespace(ns),
			client.MatchingLabels{ledgerv1alpha1.LabelLedgerBackup: "bk-sched"},
		); err != nil {
			return false
		}
		return len(runs.Items) >= 1
	}, "scheduled run should be created")

	runs := &ledgerv1alpha1.LedgerBackupRunList{}
	require.NoError(t, k8sClient.List(ctx, runs,
		client.InNamespace(ns),
		client.MatchingLabels{ledgerv1alpha1.LabelLedgerBackup: "bk-sched"},
	))
	require.NotEmpty(t, runs.Items)

	run := &runs.Items[0]
	assert.Equal(t, "bk-sched", run.Spec.BackupRef)
	assert.Equal(t, ledgerv1alpha1.BackupRunTypeFull, run.Spec.Type)
	assert.Equal(t, "bk-sched", run.Labels[ledgerv1alpha1.LabelLedgerBackup])
	assert.Equal(t, string(ledgerv1alpha1.BackupRunTypeFull), run.Labels[ledgerv1alpha1.LabelLedgerBackupRunType])

	require.Len(t, run.OwnerReferences, 1)
	owner := run.OwnerReferences[0]
	assert.Equal(t, "LedgerBackup", owner.Kind)
	assert.Equal(t, "bk-sched", owner.Name)
	require.NotNil(t, owner.Controller)
	assert.True(t, *owner.Controller)
}

// TestBackupReconcile_RetentionPolicy verifies that succeeded runs beyond the
// SuccessfulRunsHistoryLimit are garbage-collected, oldest first.
func TestBackupReconcile_RetentionPolicy(t *testing.T) {
	ns := createTestNamespace(t)

	ls := newLedgerService("bk-svc-retention", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	limit := int32(2)
	backup := newLedgerBackup("bk-retention", ns, "bk-svc-retention")
	backup.Spec.Schedule.Full = ""
	backup.Spec.Schedule.Incremental = ""
	backup.Spec.SuccessfulRunsHistoryLimit = &limit
	require.NoError(t, k8sClient.Create(ctx, backup))

	requireEventually(t, func() bool {
		var got ledgerv1alpha1.LedgerBackup
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "bk-retention", Namespace: ns}, &got); err != nil {
			return false
		}
		return got.Status.Phase == ledgerv1alpha1.BackupPhaseReady
	}, "backup should reach Ready first")

	// Fetch the persisted LedgerBackup so owner ref UID is correct.
	var parent ledgerv1alpha1.LedgerBackup
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "bk-retention", Namespace: ns}, &parent))

	// Create 4 succeeded runs spread across distinct completion times.
	base := time.Now().Add(-1 * time.Hour)
	for i := 0; i < 4; i++ {
		completion := metav1.NewTime(base.Add(time.Duration(i) * time.Minute))
		createSucceededRun(t, ns, &parent, completion)
	}

	requireEventually(t, func() bool {
		runs := &ledgerv1alpha1.LedgerBackupRunList{}
		if err := k8sClient.List(ctx, runs,
			client.InNamespace(ns),
			client.MatchingLabels{ledgerv1alpha1.LabelLedgerBackup: "bk-retention"},
		); err != nil {
			return false
		}
		// Need a reconcile tick to trigger pruning. We poke the parent by writing an annotation.
		_ = pokeBackup(ns, "bk-retention")

		return len(runs.Items) == int(limit)
	}, "only successfulRunsHistoryLimit runs should remain after pruning")
}

// createSucceededRun creates a Succeeded LedgerBackupRun owned by the given backup
// and sets its CompletionTime via a status update.
func createSucceededRun(t *testing.T, ns string, backup *ledgerv1alpha1.LedgerBackup, completion metav1.Time) {
	t.Helper()
	tt := true
	run := &ledgerv1alpha1.LedgerBackupRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    ns,
			GenerateName: backup.Name + "-full-",
			Labels: map[string]string{
				ledgerv1alpha1.LabelLedgerBackup:        backup.Name,
				ledgerv1alpha1.LabelLedgerBackupRunType: string(ledgerv1alpha1.BackupRunTypeFull),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "ledger.formance.com/v1alpha1",
				Kind:       "LedgerBackup",
				Name:       backup.Name,
				UID:        backup.UID,
				Controller: &tt,
			}},
		},
		Spec: ledgerv1alpha1.LedgerBackupRunSpec{
			BackupRef: backup.Name,
			Type:      ledgerv1alpha1.BackupRunTypeFull,
		},
	}
	require.NoError(t, k8sClient.Create(ctx, run))

	run.Status.Phase = ledgerv1alpha1.BackupRunPhaseSucceeded
	run.Status.StartTime = &completion
	run.Status.CompletionTime = &completion
	run.Status.Full = &ledgerv1alpha1.FullBackupStatus{
		Time:        &completion,
		TotalFiles:  1,
		DurationMs:  10,
	}
	require.NoError(t, k8sClient.Status().Update(ctx, run))
}

// pokeBackup triggers a reconcile by adding a transient annotation.
func pokeBackup(ns, name string) error {
	var backup ledgerv1alpha1.LedgerBackup
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &backup); err != nil {
		return err
	}
	if backup.Annotations == nil {
		backup.Annotations = map[string]string{}
	}
	backup.Annotations["ledger.formance.com/poke"] = time.Now().Format(time.RFC3339Nano)

	return k8sClient.Update(ctx, &backup)
}

// newLedgerBackup returns a LedgerBackup CR with default S3 destination and hourly schedule.
func newLedgerBackup(name, namespace, serviceRef string) *ledgerv1alpha1.LedgerBackup {
	return &ledgerv1alpha1.LedgerBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: ledgerv1alpha1.LedgerBackupSpec{
			ServiceRef: serviceRef,
			Destination: ledgerv1alpha1.BackupDestination{
				Driver: "s3",
				S3: &ledgerv1alpha1.S3Config{
					Bucket: "test-bucket",
					Region: "us-east-1",
				},
			},
			Schedule: ledgerv1alpha1.BackupSchedule{
				Full:        "0 2 * * 0",
				Incremental: "0 * * * *",
			},
		},
	}
}
