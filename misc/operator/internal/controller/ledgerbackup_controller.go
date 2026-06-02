package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerbackups/finalizers,verbs=update

// LedgerBackupReconciler reconciles a LedgerBackup object.
type LedgerBackupReconciler struct {
	client.Client

	Scheme    *runtime.Scheme
	Config    *rest.Config
	Clientset kubernetes.Interface
}

func (r *LedgerBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var backup ledgerv1alpha1.LedgerBackup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !backup.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Validate that the referenced LedgerService exists.
	var ledgerService ledgerv1alpha1.LedgerService
	if err := r.Get(ctx, types.NamespacedName{
		Name:      backup.Spec.ServiceRef,
		Namespace: backup.Namespace,
	}, &ledgerService); err != nil {
		return r.setFailed(ctx, &backup, fmt.Sprintf("LedgerService %q not found: %v", backup.Spec.ServiceRef, err))
	}

	// Validate schedules.
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

	if fullSched == nil && incrSched == nil {
		return r.setFailed(ctx, &backup, "at least one of schedule.full or schedule.incremental must be set")
	}

	meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               "ConfigValid",
		Status:             metav1.ConditionTrue,
		Reason:             "Valid",
		ObservedGeneration: backup.Generation,
	})

	now := time.Now()
	grpcPort := ledgerService.Spec.GrpcPort
	if grpcPort == 0 {
		grpcPort = 8888
	}
	var nextRequeue time.Duration

	// Check if a full backup is due.
	if fullSched != nil {
		var lastFullTime *metav1.Time
		if backup.Status.LastFullBackup != nil {
			lastFullTime = backup.Status.LastFullBackup.Time
		}
		nextFullTime := nextRunTime(fullSched, lastFullTime)
		if now.After(nextFullTime) || now.Equal(nextFullTime) {
			logger.Info("triggering full backup", "service", backup.Spec.ServiceRef)
			backup.Status.Phase = ledgerv1alpha1.BackupPhaseRunning
			if err := r.Status().Update(ctx, &backup); err != nil {
				return ctrl.Result{}, err
			}

			result, err := r.execFullBackup(ctx, &backup, &ledgerService, grpcPort)
			if err != nil {
				logger.Error(err, "full backup failed")
				meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
					Type:               "FullBackupHealthy",
					Status:             metav1.ConditionFalse,
					Reason:             "BackupFailed",
					Message:            err.Error(),
					ObservedGeneration: backup.Generation,
				})
			} else {
				logger.Info("full backup completed", "filesUploaded", result.FilesUploaded, "totalFiles", result.TotalFiles)
				t := metav1.Now()
				backup.Status.LastFullBackup = &ledgerv1alpha1.FullBackupStatus{
					Time:              &t,
					FilesUploaded:     result.FilesUploaded,
					FilesDeleted:      result.FilesDeleted,
					TotalFiles:        result.TotalFiles,
					DurationMs:        result.DurationMs,
					LastLogSequence:   result.LastLogSequence,
					LastAuditSequence: result.LastAuditSequence,
					LastAppliedIndex:  result.LastAppliedIndex,
				}
				meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
					Type:               "FullBackupHealthy",
					Status:             metav1.ConditionTrue,
					Reason:             "BackupSucceeded",
					ObservedGeneration: backup.Generation,
				})
			}
			// Recompute next full time from now.
			nextFullTime = fullSched.Next(now)
		}
		backup.Status.NextFullBackupTime = &metav1.Time{Time: nextFullTime}
		untilFull := time.Until(nextFullTime)
		if nextRequeue == 0 || untilFull < nextRequeue {
			nextRequeue = untilFull
		}
	}

	// Check if an incremental backup is due.
	if incrSched != nil {
		var lastIncrTime *metav1.Time
		if backup.Status.LastIncrementalBackup != nil {
			lastIncrTime = backup.Status.LastIncrementalBackup.Time
		}
		nextIncrTime := nextRunTime(incrSched, lastIncrTime)
		if now.After(nextIncrTime) || now.Equal(nextIncrTime) {
			// Incremental requires a prior full backup.
			if backup.Status.LastFullBackup != nil && backup.Status.LastFullBackup.Time != nil {
				logger.Info("triggering incremental backup", "service", backup.Spec.ServiceRef)
				backup.Status.Phase = ledgerv1alpha1.BackupPhaseRunning
				if err := r.Status().Update(ctx, &backup); err != nil {
					return ctrl.Result{}, err
				}

				result, err := r.execIncrementalBackup(ctx, &backup, &ledgerService, grpcPort)
				if err != nil {
					logger.Error(err, "incremental backup failed")
					meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
						Type:               "IncrementalBackupHealthy",
						Status:             metav1.ConditionFalse,
						Reason:             "BackupFailed",
						Message:            err.Error(),
						ObservedGeneration: backup.Generation,
					})
				} else {
					logger.Info("incremental backup completed",
						"logEntries", result.LogEntriesExported,
						"auditEntries", result.AuditEntriesExported,
						"segments", result.SegmentsUploaded)
					t := metav1.Now()
					backup.Status.LastIncrementalBackup = &ledgerv1alpha1.IncrementalBackupStatus{
						Time:                 &t,
						LogEntriesExported:   result.LogEntriesExported,
						AuditEntriesExported: result.AuditEntriesExported,
						SegmentsUploaded:     result.SegmentsUploaded,
						DurationMs:           result.DurationMs,
						LastLogSequence:      result.LastLogSequence,
						LastAuditSequence:    result.LastAuditSequence,
					}
					meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
						Type:               "IncrementalBackupHealthy",
						Status:             metav1.ConditionTrue,
						Reason:             "BackupSucceeded",
						ObservedGeneration: backup.Generation,
					})
				}
			} else {
				logger.Info("skipping incremental backup: no prior full backup exists")
			}
			nextIncrTime = incrSched.Next(now)
		}
		backup.Status.NextIncrementalBackupTime = &metav1.Time{Time: nextIncrTime}
		untilIncr := time.Until(nextIncrTime)
		if nextRequeue == 0 || untilIncr < nextRequeue {
			nextRequeue = untilIncr
		}
	}

	backup.Status.Phase = ledgerv1alpha1.BackupPhaseReady
	if err := r.Status().Update(ctx, &backup); err != nil {
		return ctrl.Result{}, err
	}

	// Add a small buffer to avoid racing with the exact second.
	if nextRequeue > 0 {
		nextRequeue += time.Second
	}

	return ctrl.Result{RequeueAfter: nextRequeue}, nil
}

// nextRunTime returns the next scheduled time after lastRun.
// If lastRun is nil (no previous run), returns time.Time{} (run immediately).
func nextRunTime(sched cron.Schedule, lastRun *metav1.Time) time.Time {
	if lastRun != nil {
		return sched.Next(lastRun.Time)
	}

	return time.Time{}
}

// backupFlags builds the common ledgerctl flags for backup commands.
func backupFlags(dest *ledgerv1alpha1.BackupDestination) []string {
	args := []string{"--driver", dest.Driver}
	if dest.BucketID != "" {
		args = append(args, "--bucket-id", dest.BucketID)
	}
	if dest.S3 != nil {
		if dest.S3.Bucket != "" {
			args = append(args, "--s3-bucket", dest.S3.Bucket)
		}
		if dest.S3.Region != "" {
			args = append(args, "--s3-region", dest.S3.Region)
		}
		if dest.S3.Endpoint != "" {
			args = append(args, "--s3-endpoint", dest.S3.Endpoint)
		}
	}
	if dest.S3AccessKeyID != "" {
		args = append(args, "--s3-access-key-id", dest.S3AccessKeyID)
	}
	if dest.S3SecretAccessKey != "" {
		args = append(args, "--s3-secret-access-key", dest.S3SecretAccessKey)
	}

	return args
}

// fullBackupResult holds the parsed JSON output from ledgerctl store backup.
type fullBackupResult struct {
	FilesUploaded     uint32 `json:"filesUploaded"`
	FilesDeleted      uint32 `json:"filesDeleted"`
	TotalFiles        uint32 `json:"totalFiles"`
	DurationMs        int64  `json:"durationMs"`
	LastLogSequence   uint64 `json:"lastLogSequence"`
	LastAuditSequence uint64 `json:"lastAuditSequence"`
	LastAppliedIndex  uint64 `json:"lastAppliedIndex"`
}

// incrementalBackupResult holds the parsed JSON output from ledgerctl store incremental-backup.
type incrementalBackupResult struct {
	LogEntriesExported   uint64 `json:"logEntriesExported"`
	AuditEntriesExported uint64 `json:"auditEntriesExported"`
	SegmentsUploaded     uint32 `json:"segmentsUploaded"`
	DurationMs           int64  `json:"durationMs"`
	LastLogSequence      uint64 `json:"lastLogSequence"`
	LastAuditSequence    uint64 `json:"lastAuditSequence"`
}

func (r *LedgerBackupReconciler) execFullBackup(
	ctx context.Context,
	backup *ledgerv1alpha1.LedgerBackup,
	ledgerService *ledgerv1alpha1.LedgerService,
	grpcPort int32,
) (*fullBackupResult, error) {
	// Full backup is forwarded to the leader, so pod-0 is fine.
	pod := ledgerService.Name + "-0"
	container := "ledger"

	args := []string{"store", "backup"}
	args = append(args, backupFlags(&backup.Spec.Destination)...)
	args = append(args, "--json")

	result, err := podExec(ctx, r.Config, r.Clientset, ledgerService.Namespace, pod, container,
		ledgerctlCommand(grpcPort, args...))
	if err != nil {
		return nil, fmt.Errorf("ledgerctl store backup: %w", err)
	}

	var resp fullBackupResult
	if err := json.Unmarshal([]byte(result.Stdout), &resp); err != nil {
		return nil, fmt.Errorf("parsing backup output: %w (stdout: %s)", err, result.Stdout)
	}

	return &resp, nil
}

func (r *LedgerBackupReconciler) execIncrementalBackup(
	ctx context.Context,
	backup *ledgerv1alpha1.LedgerBackup,
	ledgerService *ledgerv1alpha1.LedgerService,
	grpcPort int32,
) (*incrementalBackupResult, error) {
	// Incremental backup can run on any node. Use pod-0 for simplicity.
	pod := ledgerService.Name + "-0"
	container := "ledger"

	args := []string{"store", "incremental-backup"}
	args = append(args, backupFlags(&backup.Spec.Destination)...)
	args = append(args, "--json")

	result, err := podExec(ctx, r.Config, r.Clientset, ledgerService.Namespace, pod, container,
		ledgerctlCommand(grpcPort, args...))
	if err != nil {
		return nil, fmt.Errorf("ledgerctl store incremental-backup: %w", err)
	}

	var resp incrementalBackupResult
	if err := json.Unmarshal([]byte(result.Stdout), &resp); err != nil {
		return nil, fmt.Errorf("parsing incremental backup output: %w (stdout: %s)", err, result.Stdout)
	}

	return &resp, nil
}

func (r *LedgerBackupReconciler) setFailed(ctx context.Context, backup *ledgerv1alpha1.LedgerBackup, message string) (ctrl.Result, error) {
	backup.Status.Phase = ledgerv1alpha1.BackupPhaseFailed
	backup.Status.Message = message
	if err := r.Status().Update(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *LedgerBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.LedgerBackup{}).
		Named("ledgerbackup").
		Complete(r)
}
