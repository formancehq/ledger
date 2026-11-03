package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestBackupRunManualAssociationBlocksSibling(t *testing.T) {
	t.Parallel()
	for _, label := range []string{"", "another-backup"} {
		t.Run("label="+label, func(t *testing.T) {
			t.Parallel()
			c, run := backupRunRecoveryFixture(t)
			run.Labels = nil
			if label != "" {
				run.Labels = map[string]string{ledgerv1alpha1.LabelBackup: label}
			}
			require.NoError(t, c.Update(t.Context(), run))
			r := &BackupRunReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
			require.NoError(t, err)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(run), run))
			require.Equal(t, ledgerv1alpha1.BackupRunPhaseRunning, run.Status.Phase)

			sibling := &ledgerv1alpha1.BackupRun{
				ObjectMeta: metav1.ObjectMeta{
					Name: "waiting", Namespace: run.Namespace, UID: "waiting-uid",
					Labels: map[string]string{ledgerv1alpha1.LabelBackup: run.Spec.BackupRef},
				},
				Spec: run.Spec,
			}
			require.NoError(t, c.Create(t.Context(), sibling))
			// A fresh reconciler must discover the unlabeled Running reservation.
			r = &BackupRunReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
			result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(sibling)})
			require.NoError(t, err)
			require.Equal(t, concurrencyRequeue, result.RequeueAfter)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sibling), sibling))
			require.Equal(t, ledgerv1alpha1.BackupRunPhasePending, sibling.Status.Phase)
			require.Equal(t, `waiting for run "run" to complete`, sibling.Status.Message)
			require.True(t, apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKey{Namespace: run.Namespace, Name: backupJobName(sibling)}, &batchv1.Job{})))
		})
	}
}

func TestBackupRunAssociationIgnoresOtherParentsAndNamespaces(t *testing.T) {
	t.Parallel()
	for _, different := range []string{"parent", "namespace"} {
		t.Run(different, func(t *testing.T) {
			t.Parallel()
			c, run := backupRunRecoveryFixture(t)
			unrelated := &ledgerv1alpha1.BackupRun{
				ObjectMeta: metav1.ObjectMeta{
					Name: "unrelated", Namespace: run.Namespace, UID: "unrelated-uid",
					Labels: map[string]string{ledgerv1alpha1.LabelBackup: run.Spec.BackupRef},
				},
				Spec: run.Spec,
			}
			if different == "parent" {
				unrelated.Spec.BackupRef = "another-backup"
			} else {
				unrelated.Namespace = "another-namespace"
			}
			require.NoError(t, c.Create(t.Context(), unrelated))
			unrelated.Status.Phase = ledgerv1alpha1.BackupRunPhaseRunning
			require.NoError(t, c.Status().Update(t.Context(), unrelated))
			r := &BackupRunReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
			require.NoError(t, err)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(run), run))
			require.Equal(t, ledgerv1alpha1.BackupRunPhaseRunning, run.Status.Phase)
			require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: run.Namespace, Name: backupJobName(run)}, &batchv1.Job{}))
		})
	}
}
