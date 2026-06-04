package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerbackupruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerbackupruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerbackupruns/finalizers,verbs=update

// LedgerBackupRunReconciler reconciles a LedgerBackupRun object.
//
// A LedgerBackupRun represents a single backup execution. It is created either:
//   - by the LedgerBackupReconciler when a cron schedule fires, or
//   - manually (e.g. by the kubectl-ledger plugin or directly with kubectl apply).
//
// The reconciler enforces a Forbid concurrency policy: if another run for the
// same parent LedgerBackup is in the Running phase, this run stays Pending and
// is re-enqueued until the in-flight run terminates.
type LedgerBackupRunReconciler struct {
	client.Client

	Scheme    *runtime.Scheme
	Config    *rest.Config
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

	// Terminal phase: nothing to do.
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

	var ledgerService ledgerv1alpha1.LedgerService
	if err := r.Get(ctx, types.NamespacedName{
		Name:      backup.Spec.ServiceRef,
		Namespace: backup.Namespace,
	}, &ledgerService); err != nil {
		return r.setRunFailed(ctx, &run, fmt.Sprintf("LedgerService %q not found: %v", backup.Spec.ServiceRef, err))
	}

	// Forbid concurrency: any sibling already Running blocks this one.
	siblings, err := r.listSiblings(ctx, &run)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing sibling runs: %w", err)
	}
	for i := range siblings {
		s := &siblings[i]
		if s.UID == run.UID {
			continue
		}
		if s.Status.Phase == ledgerv1alpha1.BackupRunPhaseRunning {
			if run.Status.Phase != ledgerv1alpha1.BackupRunPhasePending {
				run.Status.Phase = ledgerv1alpha1.BackupRunPhasePending
				run.Status.Message = fmt.Sprintf("waiting for run %q to complete", s.Name)
				if err := r.Status().Update(ctx, &run); err != nil {
					return ctrl.Result{}, err
				}
			}

			return ctrl.Result{RequeueAfter: concurrencyRequeue}, nil
		}
	}

	// Transition to Running.
	if run.Status.Phase != ledgerv1alpha1.BackupRunPhaseRunning {
		now := metav1.Now()
		run.Status.Phase = ledgerv1alpha1.BackupRunPhaseRunning
		run.Status.Message = ""
		run.Status.StartTime = &now
		if err := r.Status().Update(ctx, &run); err != nil {
			return ctrl.Result{}, err
		}
	}

	grpcPort := ledgerService.Spec.GrpcPort
	if grpcPort == 0 {
		grpcPort = 8888
	}

	logger.Info("executing backup", "type", run.Spec.Type, "backup", backup.Name, "service", ledgerService.Name)

	switch run.Spec.Type {
	case ledgerv1alpha1.BackupRunTypeFull:
		result, err := execFullBackup(ctx, r.Config, r.Clientset, &backup, &ledgerService, grpcPort)
		if err != nil {
			return r.setRunFailed(ctx, &run, err.Error())
		}
		t := metav1.Now()
		run.Status.Full = &ledgerv1alpha1.FullBackupStatus{
			Time:              &t,
			FilesUploaded:     result.FilesUploaded,
			FilesDeleted:      result.FilesDeleted,
			TotalFiles:        result.TotalFiles,
			DurationMs:        result.DurationMs,
			LastLogSequence:   result.LastLogSequence,
			LastAuditSequence: result.LastAuditSequence,
			LastAppliedIndex:  result.LastAppliedIndex,
		}
	case ledgerv1alpha1.BackupRunTypeIncremental:
		result, err := execIncrementalBackup(ctx, r.Config, r.Clientset, &backup, &ledgerService, grpcPort)
		if err != nil {
			return r.setRunFailed(ctx, &run, err.Error())
		}
		t := metav1.Now()
		run.Status.Incremental = &ledgerv1alpha1.IncrementalBackupStatus{
			Time:                 &t,
			LogEntriesExported:   result.LogEntriesExported,
			AuditEntriesExported: result.AuditEntriesExported,
			SegmentsUploaded:     result.SegmentsUploaded,
			DurationMs:           result.DurationMs,
			LastLogSequence:      result.LastLogSequence,
			LastAuditSequence:    result.LastAuditSequence,
		}
	default:
		return r.setRunFailed(ctx, &run, fmt.Sprintf("unsupported backup type %q", run.Spec.Type))
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

// listSiblings returns all LedgerBackupRun resources sharing the same backupRef in the same namespace.
func (r *LedgerBackupRunReconciler) listSiblings(ctx context.Context, run *ledgerv1alpha1.LedgerBackupRun) ([]ledgerv1alpha1.LedgerBackupRun, error) {
	var list ledgerv1alpha1.LedgerBackupRunList
	if err := r.List(ctx, &list,
		client.InNamespace(run.Namespace),
		client.MatchingLabels{ledgerv1alpha1.LabelLedgerBackup: run.Spec.BackupRef},
	); err != nil {
		return nil, err
	}

	return list.Items, nil
}

func (r *LedgerBackupRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.LedgerBackupRun{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("ledgerbackuprun").
		Complete(r)
}
