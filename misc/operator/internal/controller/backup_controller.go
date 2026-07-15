package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/robfig/cron/v3"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// +kubebuilder:rbac:groups=ledger.formance.com,resources=backups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ledger.formance.com,resources=backups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=backups/finalizers,verbs=update

const (
	defaultSuccessfulRunsHistoryLimit int32 = 3
	defaultFailedRunsHistoryLimit     int32 = 1
)

// BackupReconciler reconciles a Backup object.
//
// The reconciler acts as a scheduler/template: it does NOT run backups itself.
// When a cron schedule fires, it creates a BackupRun whose own reconciler
// performs the actual ledgerctl invocation. It also maintains Backup status
// summaries from the latest Succeeded child runs and prunes runs in excess of the
// configured history limits.
type BackupReconciler struct {
	client.Client

	Scheme    *runtime.Scheme
	Config    *rest.Config
	Clientset kubernetes.Interface
}

func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var backup ledgerv1alpha1.Backup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !backup.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Validate that the referenced Cluster exists.
	var cluster ledgerv1alpha1.Cluster
	if err := r.Get(ctx, types.NamespacedName{
		Name:      backup.Spec.ClusterRef,
		Namespace: backup.Namespace,
	}, &cluster); err != nil {
		return r.setFailed(ctx, &backup, fmt.Sprintf("Cluster %q not found: %v", backup.Spec.ClusterRef, err))
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

	var fullSched cron.Schedule
	if backup.Spec.Schedule.Full != "" {
		var err error
		fullSched, err = parser.Parse(backup.Spec.Schedule.Full)
		if err != nil {
			return r.setFailed(ctx, &backup, fmt.Sprintf("invalid full backup schedule: %v", err))
		}
	}

	var incrSched cron.Schedule
	if backup.Spec.Schedule.Incremental != "" {
		var err error
		incrSched, err = parser.Parse(backup.Spec.Schedule.Incremental)
		if err != nil {
			return r.setFailed(ctx, &backup, fmt.Sprintf("invalid incremental backup schedule: %v", err))
		}
	}

	meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               "ConfigValid",
		Status:             metav1.ConditionTrue,
		Reason:             "Valid",
		ObservedGeneration: backup.Generation,
	})

	runs, err := r.listChildRuns(ctx, &backup)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing child runs: %w", err)
	}

	now := time.Now()
	var nextRequeue time.Duration

	if fullSched != nil {
		next, err := r.scheduleType(ctx, &backup, runs, ledgerv1alpha1.BackupRunTypeFull, fullSched, now)
		if err != nil {
			return ctrl.Result{}, err
		}
		backup.Status.NextFullBackupTime = &metav1.Time{Time: next}
		nextRequeue = minDuration(nextRequeue, time.Until(next))
	} else {
		backup.Status.NextFullBackupTime = nil
	}

	if incrSched != nil {
		// An incremental backup is only meaningful after at least one Succeeded Full run.
		if hasSucceededRun(runs, ledgerv1alpha1.BackupRunTypeFull) {
			next, err := r.scheduleType(ctx, &backup, runs, ledgerv1alpha1.BackupRunTypeIncremental, incrSched, now)
			if err != nil {
				return ctrl.Result{}, err
			}
			backup.Status.NextIncrementalBackupTime = &metav1.Time{Time: next}
			nextRequeue = minDuration(nextRequeue, time.Until(next))
		} else {
			logger.V(1).Info("skipping incremental schedule: no successful full backup yet")
			backup.Status.NextIncrementalBackupTime = nil
		}
	} else {
		backup.Status.NextIncrementalBackupTime = nil
	}

	// Refresh status summaries from the latest Succeeded run of each type.
	r.refreshStatusSummaries(&backup, runs)

	// Prune runs in excess of history limits.
	if err := r.pruneRuns(ctx, &backup, runs); err != nil {
		return ctrl.Result{}, fmt.Errorf("pruning runs: %w", err)
	}

	backup.Status.Phase = ledgerv1alpha1.BackupPhaseReady
	backup.Status.Message = ""
	if err := r.Status().Update(ctx, &backup); err != nil {
		return ctrl.Result{}, err
	}

	if nextRequeue > 0 {
		nextRequeue += time.Second
	}

	return ctrl.Result{RequeueAfter: nextRequeue}, nil
}

// scheduleType creates a new BackupRun of the given type if the schedule is due
// and no run of the same type is currently in flight. Returns the next scheduled time.
func (r *BackupReconciler) scheduleType(
	ctx context.Context,
	backup *ledgerv1alpha1.Backup,
	runs []ledgerv1alpha1.BackupRun,
	runType ledgerv1alpha1.BackupRunType,
	sched cron.Schedule,
	now time.Time,
) (time.Time, error) {
	logger := log.FromContext(ctx)

	if hasRunningRun(runs, runType) {
		// Concurrency Forbid: defer scheduling until the in-flight run finishes.
		return sched.Next(now), nil
	}

	lastTime := lastRunTime(runs, runType)
	nextTime := nextRunTime(sched, lastTime)

	if now.Before(nextTime) {
		return nextTime, nil
	}

	if err := r.createRun(ctx, backup, runType); err != nil {
		return time.Time{}, fmt.Errorf("creating %s BackupRun: %w", runType, err)
	}
	logger.Info("scheduled backup run", "type", runType, "backup", backup.Name)

	return sched.Next(now), nil
}

// createRun creates a new BackupRun owned by the given Backup.
func (r *BackupReconciler) createRun(
	ctx context.Context,
	backup *ledgerv1alpha1.Backup,
	runType ledgerv1alpha1.BackupRunType,
) error {
	var suffix string
	switch runType {
	case ledgerv1alpha1.BackupRunTypeFull:
		suffix = "full-"
	case ledgerv1alpha1.BackupRunTypeIncremental:
		suffix = "incr-"
	default:
		return fmt.Errorf("unsupported backup run type %q", runType)
	}

	run := &ledgerv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    backup.Namespace,
			GenerateName: backup.Name + "-" + suffix,
			Labels: map[string]string{
				ledgerv1alpha1.LabelBackup:        backup.Name,
				ledgerv1alpha1.LabelBackupRunType: string(runType),
			},
		},
		Spec: ledgerv1alpha1.BackupRunSpec{
			BackupRef: backup.Name,
			Type:      runType,
		},
	}
	if err := controllerutil.SetControllerReference(backup, run, r.Scheme); err != nil {
		return fmt.Errorf("setting owner reference: %w", err)
	}

	return r.Create(ctx, run)
}

// refreshStatusSummaries updates LastFullBackup / LastIncrementalBackup from the latest Succeeded run.
func (r *BackupReconciler) refreshStatusSummaries(
	backup *ledgerv1alpha1.Backup,
	runs []ledgerv1alpha1.BackupRun,
) {
	if latest := latestSucceededRun(runs, ledgerv1alpha1.BackupRunTypeFull); latest != nil && latest.Status.Full != nil {
		backup.Status.LastFullBackup = latest.Status.Full.DeepCopy()
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               "FullBackupHealthy",
			Status:             metav1.ConditionTrue,
			Reason:             "BackupSucceeded",
			ObservedGeneration: backup.Generation,
		})
	} else if latestFailed := latestFailedRun(runs, ledgerv1alpha1.BackupRunTypeFull); latestFailed != nil {
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               "FullBackupHealthy",
			Status:             metav1.ConditionFalse,
			Reason:             "BackupFailed",
			Message:            latestFailed.Status.Message,
			ObservedGeneration: backup.Generation,
		})
	}

	if latest := latestSucceededRun(runs, ledgerv1alpha1.BackupRunTypeIncremental); latest != nil && latest.Status.Incremental != nil {
		backup.Status.LastIncrementalBackup = latest.Status.Incremental.DeepCopy()
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               "IncrementalBackupHealthy",
			Status:             metav1.ConditionTrue,
			Reason:             "BackupSucceeded",
			ObservedGeneration: backup.Generation,
		})
	} else if latestFailed := latestFailedRun(runs, ledgerv1alpha1.BackupRunTypeIncremental); latestFailed != nil {
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               "IncrementalBackupHealthy",
			Status:             metav1.ConditionFalse,
			Reason:             "BackupFailed",
			Message:            latestFailed.Status.Message,
			ObservedGeneration: backup.Generation,
		})
	}
}

// pruneRuns deletes runs in excess of the per-type history limits, oldest first.
func (r *BackupReconciler) pruneRuns(
	ctx context.Context,
	backup *ledgerv1alpha1.Backup,
	runs []ledgerv1alpha1.BackupRun,
) error {
	logger := log.FromContext(ctx)
	successLimit := defaultSuccessfulRunsHistoryLimit
	if backup.Spec.SuccessfulRunsHistoryLimit != nil {
		successLimit = *backup.Spec.SuccessfulRunsHistoryLimit
	}
	failLimit := defaultFailedRunsHistoryLimit
	if backup.Spec.FailedRunsHistoryLimit != nil {
		failLimit = *backup.Spec.FailedRunsHistoryLimit
	}

	for _, runType := range []ledgerv1alpha1.BackupRunType{
		ledgerv1alpha1.BackupRunTypeFull,
		ledgerv1alpha1.BackupRunTypeIncremental,
	} {
		toDelete := excessRuns(runs, runType, ledgerv1alpha1.BackupRunPhaseSucceeded, successLimit)
		toDelete = append(toDelete, excessRuns(runs, runType, ledgerv1alpha1.BackupRunPhaseFailed, failLimit)...)
		for i := range toDelete {
			run := toDelete[i]
			if err := r.Delete(ctx, &run); err != nil && !kerrors.IsNotFound(err) {
				return fmt.Errorf("deleting run %s: %w", run.Name, err)
			}
			logger.V(1).Info("pruned old run", "name", run.Name, "type", runType)
		}
	}

	return nil
}

func (r *BackupReconciler) listChildRuns(
	ctx context.Context,
	backup *ledgerv1alpha1.Backup,
) ([]ledgerv1alpha1.BackupRun, error) {
	var list ledgerv1alpha1.BackupRunList
	if err := r.List(ctx, &list,
		client.InNamespace(backup.Namespace),
		client.MatchingLabels{ledgerv1alpha1.LabelBackup: backup.Name},
	); err != nil {
		return nil, err
	}

	return list.Items, nil
}

func (r *BackupReconciler) setFailed(ctx context.Context, backup *ledgerv1alpha1.Backup, message string) (ctrl.Result, error) {
	backup.Status.Phase = ledgerv1alpha1.BackupPhaseFailed
	backup.Status.Message = message
	if err := r.Status().Update(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.Backup{}).
		Owns(&ledgerv1alpha1.BackupRun{}).
		Named("backup").
		Complete(r)
}

// nextRunTime returns the next scheduled time after lastRun, or zero time if lastRun is nil (run immediately).
func nextRunTime(sched cron.Schedule, lastRun *metav1.Time) time.Time {
	if lastRun != nil {
		return sched.Next(lastRun.Time)
	}

	return time.Time{}
}

func minDuration(current, candidate time.Duration) time.Duration {
	if current == 0 || candidate < current {
		return candidate
	}

	return current
}

// hasRunningRun reports whether any run of the given type is currently Running or Pending.
// A Pending run also blocks scheduling (Forbid concurrency includes queued runs).
func hasRunningRun(runs []ledgerv1alpha1.BackupRun, runType ledgerv1alpha1.BackupRunType) bool {
	for i := range runs {
		if runs[i].Spec.Type != runType {
			continue
		}
		switch runs[i].Status.Phase {
		case ledgerv1alpha1.BackupRunPhaseRunning, ledgerv1alpha1.BackupRunPhasePending, "":
			return true
		}
	}

	return false
}

func hasSucceededRun(runs []ledgerv1alpha1.BackupRun, runType ledgerv1alpha1.BackupRunType) bool {
	for i := range runs {
		if runs[i].Spec.Type == runType && runs[i].Status.Phase == ledgerv1alpha1.BackupRunPhaseSucceeded {
			return true
		}
	}

	return false
}

// lastRunTime returns the CompletionTime of the most recent terminal run of the given type, or nil.
func lastRunTime(runs []ledgerv1alpha1.BackupRun, runType ledgerv1alpha1.BackupRunType) *metav1.Time {
	var latest *metav1.Time
	for i := range runs {
		if runs[i].Spec.Type != runType {
			continue
		}
		if !runs[i].IsTerminal() {
			continue
		}
		t := runs[i].Status.CompletionTime
		if t == nil {
			continue
		}
		if latest == nil || t.After(latest.Time) {
			latest = t
		}
	}

	return latest
}

func latestSucceededRun(runs []ledgerv1alpha1.BackupRun, runType ledgerv1alpha1.BackupRunType) *ledgerv1alpha1.BackupRun {
	return latestRunInPhase(runs, runType, ledgerv1alpha1.BackupRunPhaseSucceeded)
}

func latestFailedRun(runs []ledgerv1alpha1.BackupRun, runType ledgerv1alpha1.BackupRunType) *ledgerv1alpha1.BackupRun {
	return latestRunInPhase(runs, runType, ledgerv1alpha1.BackupRunPhaseFailed)
}

func latestRunInPhase(runs []ledgerv1alpha1.BackupRun, runType ledgerv1alpha1.BackupRunType, phase ledgerv1alpha1.BackupRunPhase) *ledgerv1alpha1.BackupRun {
	var latest *ledgerv1alpha1.BackupRun
	for i := range runs {
		if runs[i].Spec.Type != runType || runs[i].Status.Phase != phase {
			continue
		}
		if latest == nil || completionOrCreation(&runs[i]).After(completionOrCreation(latest).Time) {
			latest = &runs[i]
		}
	}

	return latest
}

func completionOrCreation(run *ledgerv1alpha1.BackupRun) metav1.Time {
	if run.Status.CompletionTime != nil {
		return *run.Status.CompletionTime
	}

	return run.CreationTimestamp
}

// excessRuns returns runs of the given type/phase beyond `limit`, oldest first.
func excessRuns(
	runs []ledgerv1alpha1.BackupRun,
	runType ledgerv1alpha1.BackupRunType,
	phase ledgerv1alpha1.BackupRunPhase,
	limit int32,
) []ledgerv1alpha1.BackupRun {
	var filtered []ledgerv1alpha1.BackupRun
	for i := range runs {
		if runs[i].Spec.Type == runType && runs[i].Status.Phase == phase {
			filtered = append(filtered, runs[i])
		}
	}
	if int32(len(filtered)) <= limit {
		return nil
	}
	sort.Slice(filtered, func(i, j int) bool {
		return completionOrCreation(&filtered[i]).Time.Before(completionOrCreation(&filtered[j]).Time)
	})

	return filtered[:int32(len(filtered))-limit]
}
