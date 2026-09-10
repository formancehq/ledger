package controller

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func backupRunRecoveryFixture(t *testing.T) (client.WithWatch, *ledgerv1alpha1.BackupRun) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	run := &ledgerv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "test", UID: "run-uid", Labels: map[string]string{ledgerv1alpha1.LabelBackup: "backup"}},
		Spec:       ledgerv1alpha1.BackupRunSpec{BackupRef: "backup", Type: ledgerv1alpha1.BackupRunTypeFull},
	}
	backup := &ledgerv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "backup", Namespace: run.Namespace},
		Spec:       ledgerv1alpha1.BackupSpec{ClusterRef: "cluster", Destination: ledgerv1alpha1.BackupDestination{Driver: "s3", S3: &ledgerv1alpha1.S3Config{Bucket: "bucket"}}},
	}
	cluster := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: run.Namespace}}

	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(run, &batchv1.Job{}).WithObjects(run, backup, cluster).Build(), run
}

func TestBackupRunCommittedCreateLostResponse(t *testing.T) {
	t.Parallel()
	for _, dependency := range []string{"none", "Backup", "Cluster"} {
		t.Run(dependency, func(t *testing.T) {
			t.Parallel()
			testBackupRunCommittedCreateLostResponse(t, dependency)
		})
	}
}

func testBackupRunCommittedCreateLostResponse(t *testing.T, dependency string) {
	t.Helper()
	ctx := t.Context()
	c, run := backupRunRecoveryFixture(t)
	creates, failedWrites := 0, 0
	dependencyFailures := 0
	wrapped := interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			_, isBackup := obj.(*ledgerv1alpha1.Backup)
			_, isCluster := obj.(*ledgerv1alpha1.Cluster)
			if creates == 1 && dependencyFailures == 0 && ((dependency == "Backup" && isBackup) || (dependency == "Cluster" && isCluster)) {
				dependencyFailures++

				return context.DeadlineExceeded
			}

			return c.Get(ctx, key, obj, opts...)
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*batchv1.Job); !ok {
				return c.Create(ctx, obj, opts...)
			}
			creates++
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			if creates == 1 {
				return context.DeadlineExceeded
			}

			return nil
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, name string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			err := c.SubResource(name).Update(ctx, obj, opts...)
			if r, ok := obj.(*ledgerv1alpha1.BackupRun); ok && r.Status.Phase == ledgerv1alpha1.BackupRunPhaseFailed && err == nil {
				failedWrites++
			}

			return err
		},
	})
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
	r := &BackupRunReconciler{Client: wrapped, Scheme: c.Scheme()}
	_, err := r.Reconcile(ctx, req)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, c.Get(ctx, req.NamespacedName, run))
	require.Zero(t, failedWrites)
	require.Equal(t, ledgerv1alpha1.BackupRunPhaseRunning, run.Status.Phase)
	require.Nil(t, run.Status.CompletionTime)
	if dependency != "none" {
		_, err = r.Reconcile(ctx, req)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 1, dependencyFailures)
		require.NoError(t, c.Get(ctx, req.NamespacedName, run))
		require.Equal(t, ledgerv1alpha1.BackupRunPhaseRunning, run.Status.Phase)
		require.Nil(t, run.Status.CompletionTime)
		require.Zero(t, failedWrites)
	}
	job := &batchv1.Job{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: backupJobName(run)}, job))
	require.True(t, metav1.IsControlledBy(job, run))
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	require.NoError(t, c.Status().Update(ctx, job))
	pods := k8sfake.NewClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "result", Namespace: run.Namespace, Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name}},
		Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: backupJobContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: `{"filesUploaded":7,"totalFiles":7,"lastAppliedIndex":42}`}}}}},
	})
	r = &BackupRunReconciler{Client: wrapped, Scheme: c.Scheme(), Clientset: pods}
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, req.NamespacedName, run))
	require.Equal(t, ledgerv1alpha1.BackupRunPhaseSucceeded, run.Status.Phase)
	require.NotNil(t, run.Status.CompletionTime)
	require.NotNil(t, run.Status.Full)
	require.EqualValues(t, 7, run.Status.Full.FilesUploaded)
	require.EqualValues(t, 42, run.Status.Full.LastAppliedIndex)
	var jobs batchv1.JobList
	require.NoError(t, c.List(ctx, &jobs))
	require.Len(t, jobs.Items, 1)
	require.Equal(t, 1, creates)
}

func TestBackupRunCreateFailureClassification(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		err      error
		terminal bool
	}{
		{"timeout", context.DeadlineExceeded, false},
		{"lost connection", io.EOF, false},
		{"server timeout", apierrors.NewServerTimeout(schema.GroupResource{Group: "batch", Resource: "jobs"}, "create", 1), false},
		{"quota or RBAC", apierrors.NewForbidden(schema.GroupResource{Group: "batch", Resource: "jobs"}, "run", errors.New("temporarily denied")), false},
		{"invalid", apierrors.NewInvalid(schema.GroupKind{Group: "batch", Kind: "Job"}, "run", field.ErrorList{field.Invalid(field.NewPath("spec"), "bad", "invalid spec")}), true},
		{"bad request", apierrors.NewBadRequest("invalid request"), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			c, run := backupRunRecoveryFixture(t)
			attempts := 0
			wrapped := interceptor.NewClient(c, interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				attempts++
				if attempts == 1 {
					return tt.err
				}

				return c.Create(ctx, obj, opts...)
			}})
			r := &BackupRunReconciler{Client: wrapped, Scheme: c.Scheme()}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
			_, err := r.Reconcile(ctx, req)
			if tt.terminal {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.err)
			}
			require.NoError(t, c.Get(ctx, req.NamespacedName, run))
			require.Equal(t, tt.terminal, run.IsTerminal())
			var jobs batchv1.JobList
			require.NoError(t, c.List(ctx, &jobs))
			require.Empty(t, jobs.Items)
			_, err = r.Reconcile(ctx, req)
			require.NoError(t, err)
			require.NoError(t, c.List(ctx, &jobs))
			if tt.terminal {
				require.Equal(t, ledgerv1alpha1.BackupRunPhaseFailed, run.Status.Phase)
				require.NotNil(t, run.Status.CompletionTime)
				require.Equal(t, 1, attempts)
				require.Empty(t, jobs.Items)
			} else {
				require.Equal(t, 2, attempts)
				require.Len(t, jobs.Items, 1)
				require.True(t, metav1.IsControlledBy(&jobs.Items[0], run))
				require.NoError(t, c.Get(ctx, req.NamespacedName, run))
				require.Equal(t, ledgerv1alpha1.BackupRunPhaseRunning, run.Status.Phase)
			}
		})
	}
}

func TestBackupRunReservationMustPersistBeforeCreate(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	c, run := backupRunRecoveryFixture(t)
	attempts := 0
	wrapped := interceptor.NewClient(c, interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, name string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
		attempts++
		if attempts == 1 {
			return context.DeadlineExceeded
		}

		return c.SubResource(name).Update(ctx, obj, opts...)
	}})
	r := &BackupRunReconciler{Client: wrapped, Scheme: c.Scheme()}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
	_, err := r.Reconcile(ctx, req)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	var jobs batchv1.JobList
	require.NoError(t, c.List(ctx, &jobs))
	require.Empty(t, jobs.Items)
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.NoError(t, c.List(ctx, &jobs))
	require.Len(t, jobs.Items, 1)
}

func TestBackupRunRejectsForeignJob(t *testing.T) {
	t.Parallel()
	for _, owner := range []string{"", "previous-run-uid"} {
		t.Run("owner="+owner, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			c, run := backupRunRecoveryFixture(t)
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: backupJobName(run), Namespace: run.Namespace}}
			if owner != "" {
				other := run.DeepCopy()
				other.UID = types.UID(owner)
				job.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(other, ledgerv1alpha1.GroupVersion.WithKind("BackupRun"))}
			}
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			require.NoError(t, c.Create(ctx, job))
			r := &BackupRunReconciler{Client: c, Scheme: c.Scheme()}
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
			require.ErrorContains(t, err, "is not controlled by BackupRun")
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(run), run))
			require.False(t, run.IsTerminal())
			require.Nil(t, run.Status.Full)
			var jobs batchv1.JobList
			require.NoError(t, c.List(ctx, &jobs))
			require.Len(t, jobs.Items, 1)
			require.Equal(t, job.OwnerReferences, jobs.Items[0].OwnerReferences)
		})
	}
}

func TestBackupRunInvalidTypeIsTerminal(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	c, run := backupRunRecoveryFixture(t)
	run.Spec.Type = "invalid"
	require.NoError(t, c.Update(ctx, run))
	r := &BackupRunReconciler{Client: c, Scheme: c.Scheme()}
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(run), run))
	require.Equal(t, ledgerv1alpha1.BackupRunPhaseFailed, run.Status.Phase)
	require.Contains(t, run.Status.Message, "unsupported backup type")
	var jobs batchv1.JobList
	require.NoError(t, c.List(ctx, &jobs))
	require.Empty(t, jobs.Items)
}

func TestBackupRunCommittedCreateCacheLag(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	c, run := backupRunRecoveryFixture(t)
	creates, commits, jobReads := 0, 0, 0
	wrapped := interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				jobReads++
				if jobReads == 2 {
					return apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, key.Name)
				}
			}

			return c.Get(ctx, key, obj, opts...)
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*batchv1.Job); !ok {
				return c.Create(ctx, obj, opts...)
			}
			creates++
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			commits++
			if creates == 1 {
				return context.DeadlineExceeded
			}

			return nil
		},
	})
	r := &BackupRunReconciler{Client: wrapped, Scheme: c.Scheme()}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
	_, err := r.Reconcile(ctx, req)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	sibling := run.DeepCopy()
	sibling.Name, sibling.UID = "sibling", "sibling-uid"
	sibling.ResourceVersion = ""
	require.NoError(t, c.Create(ctx, sibling))
	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(sibling)})
	require.NoError(t, err)
	require.Equal(t, concurrencyRequeue, result.RequeueAfter)
	require.Equal(t, 1, creates)
	_, err = r.Reconcile(ctx, req)
	require.True(t, apierrors.IsAlreadyExists(err), "stale NotFound must encounter the committed deterministic Job: %v", err)
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, req.NamespacedName, run))
	require.Equal(t, ledgerv1alpha1.BackupRunPhaseRunning, run.Status.Phase)
	require.Equal(t, 2, creates)
	require.Equal(t, 1, commits)
	var jobs batchv1.JobList
	require.NoError(t, c.List(ctx, &jobs))
	require.Len(t, jobs.Items, 1)
	require.True(t, metav1.IsControlledBy(&jobs.Items[0], run))
}
