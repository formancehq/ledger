package controller

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerbackupruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerbackupruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerbackupruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get

// LedgerBackupRunReconciler reconciles a LedgerBackupRun object.
//
// A LedgerBackupRun represents a single backup execution. It is created either:
//   - by the LedgerBackupReconciler when a cron schedule fires, or
//   - manually (e.g. by the kubectl-ledger plugin or directly with kubectl apply).
//
// The reconciler enforces a Forbid concurrency policy: if another run for the
// same parent LedgerBackup is in the Running phase, this run stays Pending and
// is re-enqueued until the in-flight run terminates.
//
// Backups themselves are delegated to a dedicated batchv1.Job per run; the
// reconciler does not block the controller while ledgerctl uploads to S3 — it
// creates the Job, watches its status, parses the JSON summary from the pod
// logs on success, and surfaces failures on the run status.
type LedgerBackupRunReconciler struct {
	client.Client

	Scheme    *runtime.Scheme
	Clientset kubernetes.Interface
}

// concurrencyRequeue is how long to wait before re-checking when another run
// is in flight for the same LedgerBackup.
const concurrencyRequeue = 10 * time.Second

func (r *LedgerBackupRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var run ledgerv1alpha1.LedgerBackupRun
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !run.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	if run.IsTerminal() {
		return ctrl.Result{}, nil
	}

	if run.Spec.Type == "" {
		run.Spec.Type = ledgerv1alpha1.BackupRunTypeFull
	}

	var backup ledgerv1alpha1.LedgerBackup
	if err := r.Get(ctx, types.NamespacedName{
		Name:      run.Spec.BackupRef,
		Namespace: run.Namespace,
	}, &backup); err != nil {
		return r.setRunFailed(ctx, &run, fmt.Sprintf("LedgerBackup %q not found: %v", run.Spec.BackupRef, err))
	}

	var cluster ledgerv1alpha1.Cluster
	if err := r.Get(ctx, types.NamespacedName{
		Name:      backup.Spec.ServiceRef,
		Namespace: backup.Namespace,
	}, &cluster); err != nil {
		return r.setRunFailed(ctx, &run, fmt.Sprintf("Cluster %q not found: %v", backup.Spec.ServiceRef, err))
	}

	// Forbid concurrency: any sibling already Running blocks this one.
	if blocking, err := r.findBlockingSibling(ctx, &run); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing sibling runs: %w", err)
	} else if blocking != "" {
		if run.Status.Phase != ledgerv1alpha1.BackupRunPhasePending {
			run.Status.Phase = ledgerv1alpha1.BackupRunPhasePending
			run.Status.Message = fmt.Sprintf("waiting for run %q to complete", blocking)
			if err := r.Status().Update(ctx, &run); err != nil {
				return ctrl.Result{}, err
			}
		}

		return ctrl.Result{RequeueAfter: concurrencyRequeue}, nil
	}

	// Ensure the backing Job exists.
	job, err := r.ensureBackupJob(ctx, &run, &backup, &cluster)
	if err != nil {
		return r.setRunFailed(ctx, &run, fmt.Sprintf("provisioning backup Job: %v", err))
	}

	// Transition to Running on the first reconcile after Job creation.
	if run.Status.Phase != ledgerv1alpha1.BackupRunPhaseRunning {
		now := metav1.Now()
		run.Status.Phase = ledgerv1alpha1.BackupRunPhaseRunning
		run.Status.Message = ""
		if run.Status.StartTime == nil {
			run.Status.StartTime = &now
		}

		if err := r.Status().Update(ctx, &run); err != nil {
			return ctrl.Result{}, err
		}
	}

	succeeded, terminal, jobMsg := jobTerminalCondition(job)
	if !terminal {
		// Job still running: wait for the next status update via the Owns watch.
		logger.V(1).Info("backup Job still in progress", "job", job.Name, "active", job.Status.Active)

		return ctrl.Result{}, nil
	}

	if !succeeded {
		message := jobMsg
		if message == "" {
			message = "backup Job failed without a status message"
		}

		return r.setRunFailed(ctx, &run, message)
	}

	// Success: parse the JSON summary from the pod logs and lift it onto status.
	if err := r.applyJobResult(ctx, &run, &cluster, job); err != nil {
		return r.setRunFailed(ctx, &run, fmt.Sprintf("reading backup result: %v", err))
	}

	completion := metav1.Now()
	run.Status.Phase = ledgerv1alpha1.BackupRunPhaseSucceeded
	run.Status.CompletionTime = &completion
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               "BackupCompleted",
		Status:             metav1.ConditionTrue,
		Reason:             "Succeeded",
		ObservedGeneration: run.Generation,
	})
	if err := r.Status().Update(ctx, &run); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// ensureBackupJob creates the Job for the run on first reconcile and returns
// the live Job afterwards. The Job is owner-referenced by the LedgerBackupRun
// so deletion cascades.
func (r *LedgerBackupRunReconciler) ensureBackupJob(
	ctx context.Context,
	run *ledgerv1alpha1.LedgerBackupRun,
	backup *ledgerv1alpha1.LedgerBackup,
	ls *ledgerv1alpha1.Cluster,
) (*batchv1.Job, error) {
	existing := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: backupJobName(run), Namespace: run.Namespace}, existing)
	if err == nil {
		return existing, nil
	}

	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("looking up Job: %w", err)
	}

	tlsMode, err := fetchTLSMode(ctx, r.Client, ls.Namespace, resourceName(ls.Name))
	if err != nil {
		return nil, fmt.Errorf("resolving TLS mode for Cluster %q: %w", ls.Name, err)
	}

	desired, err := buildBackupJob(run, backup, ls, tlsMode)
	if err != nil {
		return nil, err
	}

	if err := controllerutil.SetControllerReference(run, desired, r.Scheme); err != nil {
		return nil, fmt.Errorf("setting owner reference on Job: %w", err)
	}

	if err := r.Create(ctx, desired); err != nil {
		return nil, fmt.Errorf("creating Job: %w", err)
	}

	return desired, nil
}

// applyJobResult fetches the JSON summary produced by the Job's ledgerctl
// invocation and stamps the corresponding status sub-object on the
// LedgerBackupRun. The summary is read from
// ContainerStatus.State.Terminated.Message (populated from the container's
// terminationMessagePath when ledgerctl --json finishes cleanly) and falls
// back to the merged stdout+stderr log stream if the structured field is
// missing — e.g. the container died before writing the file.
func (r *LedgerBackupRunReconciler) applyJobResult(
	ctx context.Context,
	run *ledgerv1alpha1.LedgerBackupRun,
	_ *ledgerv1alpha1.Cluster,
	job *batchv1.Job,
) error {
	payload, err := fetchJobResultPayload(ctx, r.Clientset, job.Namespace, job.Name)
	if err != nil {
		return err
	}

	now := metav1.Now()

	switch run.Spec.Type {
	case ledgerv1alpha1.BackupRunTypeFull:
		var result fullBackupResult
		if err := parseBackupResult(payload, run.Spec.Type, &result); err != nil {
			return err
		}

		run.Status.Full = &ledgerv1alpha1.FullBackupStatus{
			Time:              &now,
			FilesUploaded:     result.FilesUploaded,
			FilesDeleted:      result.FilesDeleted,
			OrphansDeleted:    result.OrphansDeleted,
			TotalFiles:        result.TotalFiles,
			DurationMs:        result.DurationMs,
			LastLogSequence:   result.LastLogSequence,
			LastAuditSequence: result.LastAuditSequence,
			LastAppliedIndex:  result.LastAppliedIndex,
		}
	case ledgerv1alpha1.BackupRunTypeIncremental:
		var result incrementalBackupResult
		if err := parseBackupResult(payload, run.Spec.Type, &result); err != nil {
			return err
		}

		run.Status.Incremental = &ledgerv1alpha1.IncrementalBackupStatus{
			Time:                 &now,
			LogEntriesExported:   result.LogEntriesExported,
			AuditEntriesExported: result.AuditEntriesExported,
			SegmentsUploaded:     result.SegmentsUploaded,
			OrphansDeleted:       result.OrphansDeleted,
			DurationMs:           result.DurationMs,
			LastLogSequence:      result.LastLogSequence,
			LastAuditSequence:    result.LastAuditSequence,
		}
	default:
		return fmt.Errorf("unsupported backup type %q", run.Spec.Type)
	}

	return nil
}

// setRunFailed transitions the run to Failed with the given message and returns a terminal result.
func (r *LedgerBackupRunReconciler) setRunFailed(ctx context.Context, run *ledgerv1alpha1.LedgerBackupRun, message string) (ctrl.Result, error) {
	completion := metav1.Now()
	run.Status.Phase = ledgerv1alpha1.BackupRunPhaseFailed
	run.Status.Message = message
	run.Status.CompletionTime = &completion
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               "BackupCompleted",
		Status:             metav1.ConditionFalse,
		Reason:             "Failed",
		Message:            message,
		ObservedGeneration: run.Generation,
	})
	if err := r.Status().Update(ctx, run); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// findBlockingSibling returns the name of an in-flight sibling LedgerBackupRun
// (same parent backup, Running phase) that blocks the current run, or "" if
// none. Self is excluded.
func (r *LedgerBackupRunReconciler) findBlockingSibling(ctx context.Context, run *ledgerv1alpha1.LedgerBackupRun) (string, error) {
	var list ledgerv1alpha1.LedgerBackupRunList
	if err := r.List(ctx, &list,
		client.InNamespace(run.Namespace),
		client.MatchingLabels{ledgerv1alpha1.LabelLedgerBackup: run.Spec.BackupRef},
	); err != nil {
		return "", err
	}

	for i := range list.Items {
		s := &list.Items[i]
		if s.UID == run.UID {
			continue
		}

		if s.Status.Phase == ledgerv1alpha1.BackupRunPhaseRunning {
			return s.Name, nil
		}
	}

	return "", nil
}

func (r *LedgerBackupRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.LedgerBackupRun{}).
		Owns(&batchv1.Job{}).
		Named("ledgerbackuprun").
		Complete(r)
}
