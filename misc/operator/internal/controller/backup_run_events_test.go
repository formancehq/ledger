package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestBackupParentRunEvents(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"create", "status", "delete", "reference"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			run := &ledgerv1alpha1.BackupRun{
				ObjectMeta: metav1.ObjectMeta{
					Name: "manual", Namespace: "test",
					Labels: map[string]string{ledgerv1alpha1.LabelBackup: "misleading"},
				},
				Spec: ledgerv1alpha1.BackupRunSpec{BackupRef: "original"},
			}
			r := &BackupReconciler{}
			h := handler.EnqueueRequestsFromMapFunc(r.runToBackup)
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[ctrl.Request]())
			defer q.ShutDown()
			want := []ctrl.Request{{NamespacedName: client.ObjectKey{Namespace: "test", Name: "original"}}}
			switch kind {
			case "create":
				h.Create(t.Context(), event.CreateEvent{Object: run}, q)
			case "delete":
				h.Delete(t.Context(), event.DeleteEvent{Object: run}, q)
			case "status":
				completed := run.DeepCopy()
				completed.Status.Phase = ledgerv1alpha1.BackupRunPhaseSucceeded
				h.Update(t.Context(), event.UpdateEvent{ObjectOld: run, ObjectNew: completed}, q)
			case "reference":
				reassigned := run.DeepCopy()
				reassigned.Spec.BackupRef = "replacement"
				h.Update(t.Context(), event.UpdateEvent{ObjectOld: run, ObjectNew: reassigned}, q)
				want = append(want, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "test", Name: "replacement"}})
			}
			require.Equal(t, len(want), q.Len())
			got := make([]ctrl.Request, 0, q.Len())
			for q.Len() > 0 {
				req, shutdown := q.Get()
				require.False(t, shutdown)
				got = append(got, req)
				q.Done(req)
			}
			require.ElementsMatch(t, want, got)
		})
	}
}
