//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

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

	// Create a LedgerService so the serviceRef check passes.
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

func TestBackupReconcile_NoScheduleSet(t *testing.T) {
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
		return got.Status.Phase == ledgerv1alpha1.BackupPhaseFailed
	}, "backup should fail when no schedule is set")

	var got ledgerv1alpha1.LedgerBackup
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "bk-no-sched", Namespace: ns}, &got))
	assert.Contains(t, got.Status.Message, "at least one of")
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
