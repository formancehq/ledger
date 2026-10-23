package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestBackupRunLogResultRecovery(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"opening logs", "reading logs"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			c, run := backupRunRecoveryFixture(t)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
			r := &BackupRunReconciler{Client: c, Scheme: c.Scheme()}
			_, err := r.Reconcile(ctx, req)
			require.NoError(t, err)
			require.NoError(t, c.Get(ctx, req.NamespacedName, run))
			start := run.Status.StartTime.DeepCopy()
			job := &batchv1.Job{}
			require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: backupJobName(run)}, job))
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			require.NoError(t, c.Status().Update(ctx, job))

			// Exercise the real clientset's Stream path. No termination message
			// is present, so each observation must acquire the fallback logs.
			var logAttempts, podLists atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/api/v1/namespaces/test/pods":
					podLists.Add(1)
					w.Header().Set("Content-Type", "application/json")
					if err := json.NewEncoder(w).Encode(&corev1.PodList{
						TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"},
						Items:    []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "result", Namespace: "test"}}},
					}); err != nil {
						t.Errorf("encoding pod list: %v", err)
					}
				case "/api/v1/namespaces/test/pods/result/log":
					if request.URL.Query().Get("container") != backupJobContainerName {
						t.Errorf("unexpected log container: %s", request.URL.RawQuery)
					}
					if logAttempts.Add(1) == 1 {
						if failure == "opening logs" {
							http.Error(w, "temporary log access denied", http.StatusForbidden)

							return
						}
						// Advertise a longer body, then close after a different,
						// partial summary. io.ReadAll must report the interruption.
						w.Header().Set("Content-Length", "1000")
						if _, err := fmt.Fprint(w, `{"filesUploaded":999,"lastAppliedIndex":`); err != nil {
							t.Errorf("writing interrupted logs: %v", err)
						}

						return
					}
					if _, err := fmt.Fprint(w, "backup complete\n"+`{"filesUploaded":7,"filesDeleted":2,"orphansDeleted":3,"totalFiles":11,"durationMs":125,"lastLogSequence":41,"lastAuditSequence":43,"lastAppliedIndex":47}`+"\n"); err != nil {
						t.Errorf("writing recovered logs: %v", err)
					}
				default:
					t.Errorf("unexpected request: %s", request.URL)
					http.NotFound(w, request)
				}
			}))
			t.Cleanup(server.Close)
			clientset, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			require.NoError(t, err)
			r.Clientset = clientset
			_, err = r.Reconcile(ctx, req)
			require.ErrorContains(t, err, failure+` for pod "result"`)
			require.NoError(t, c.Get(ctx, req.NamespacedName, run))
			require.Equal(t, ledgerv1alpha1.BackupRunPhaseRunning, run.Status.Phase)
			require.Equal(t, start, run.Status.StartTime)
			require.Nil(t, run.Status.CompletionTime)
			require.Nil(t, run.Status.Full)
			require.Nil(t, meta.FindStatusCondition(run.Status.Conditions, "BackupCompleted"))
			require.Contains(t, run.Status.Message, failure+` for pod "result"`)
			require.EqualValues(t, 1, logAttempts.Load())

			// Restarting the reconciler must recover from persisted state,
			// reacquire the whole result, and keep the original owned Job.
			r = &BackupRunReconciler{Client: c, Scheme: c.Scheme(), Clientset: clientset}
			_, err = r.Reconcile(ctx, req)
			require.NoError(t, err)
			require.NoError(t, c.Get(ctx, req.NamespacedName, run))
			require.Equal(t, ledgerv1alpha1.BackupRunPhaseSucceeded, run.Status.Phase)
			require.Equal(t, start, run.Status.StartTime)
			require.NotNil(t, run.Status.CompletionTime)
			require.Empty(t, run.Status.Message)
			require.NotNil(t, run.Status.Full)
			require.NotNil(t, run.Status.Full.Time)
			require.Equal(t, &ledgerv1alpha1.FullBackupStatus{
				Time: run.Status.Full.Time, FilesUploaded: 7, FilesDeleted: 2,
				OrphansDeleted: 3, TotalFiles: 11, DurationMs: 125,
				LastLogSequence: 41, LastAuditSequence: 43, LastAppliedIndex: 47,
			}, run.Status.Full)
			require.True(t, meta.IsStatusConditionTrue(run.Status.Conditions, "BackupCompleted"))
			require.EqualValues(t, 2, logAttempts.Load())
			require.EqualValues(t, 2, podLists.Load())
			var jobs batchv1.JobList
			require.NoError(t, c.List(ctx, &jobs))
			require.Len(t, jobs.Items, 1)
			require.Equal(t, job.UID, jobs.Items[0].UID)
			require.Equal(t, job.ResourceVersion, jobs.Items[0].ResourceVersion)
			require.True(t, metav1.IsControlledBy(&jobs.Items[0], run))
		})
	}
}
