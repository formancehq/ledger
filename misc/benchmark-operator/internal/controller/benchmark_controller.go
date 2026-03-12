package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	benchmarkv1alpha1 "github.com/formancehq/ledger-v3-poc/misc/benchmark-operator/api/v1alpha1"
)

const requeueDelay = 5 * time.Second

// +kubebuilder:rbac:groups=benchmark.formance.com,resources=benchmarks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=benchmark.formance.com,resources=benchmarks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=benchmark.formance.com,resources=benchmarks/finalizers,verbs=update
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerservices,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=k6.io,resources=testruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

// BenchmarkReconciler reconciles a Benchmark object.
type BenchmarkReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Dynamic dynamic.Interface
	Grafana *GrafanaClient
	Ledger  *LedgerClient
}

func (r *BenchmarkReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	var bm benchmarkv1alpha1.Benchmark
	if err := r.Get(ctx, req.NamespacedName, &bm); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion.
	if !bm.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, log, &bm)
	}

	// Ensure finalizer. Return after adding to avoid stale ResourceVersion on status updates.
	if !controllerutil.ContainsFinalizer(&bm, benchmarkFinalizer) {
		controllerutil.AddFinalizer(&bm, benchmarkFinalizer)
		if err := r.Update(ctx, &bm); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	switch bm.Status.Phase {
	case "", benchmarkv1alpha1.BenchmarkPhasePending:
		return r.transitionCreatingCluster(ctx, log, &bm)
	case benchmarkv1alpha1.BenchmarkPhaseCreatingCluster:
		return r.transitionWaitingCluster(ctx, log, &bm)
	case benchmarkv1alpha1.BenchmarkPhaseWaitingCluster:
		return r.checkClusterReady(ctx, log, &bm)
	case benchmarkv1alpha1.BenchmarkPhaseCreatingLedger:
		return r.createLedger(ctx, log, &bm)
	case benchmarkv1alpha1.BenchmarkPhaseRunning:
		return r.checkTestRunDone(ctx, log, &bm)
	case benchmarkv1alpha1.BenchmarkPhaseSnapshotting:
		return r.snapshot(ctx, log, &bm)
	case benchmarkv1alpha1.BenchmarkPhaseCompleted, benchmarkv1alpha1.BenchmarkPhaseFailed:
		return ctrl.Result{}, nil
	default:
		log.Info("unknown phase", "phase", bm.Status.Phase)
		return ctrl.Result{}, nil
	}
}

func (r *BenchmarkReconciler) transitionCreatingCluster(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) (ctrl.Result, error) {
	lsName := ledgerServiceName(bm.Name)
	log.Info("creating LedgerService", "name", lsName)

	ls := buildLedgerService(bm)
	_, err := r.Dynamic.Resource(ledgerServiceGVR).Namespace(bm.Namespace).Create(ctx, ls, metav1.CreateOptions{})
	if err != nil && !kerrors.IsAlreadyExists(err) {
		return r.setBenchmarkFailed(ctx, bm, fmt.Sprintf("failed to create LedgerService: %v", err))
	}

	bm.Status.Phase = benchmarkv1alpha1.BenchmarkPhaseCreatingCluster
	bm.Status.LedgerServiceName = lsName
	if err := r.Status().Update(ctx, bm); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueDelay}, nil
}

func (r *BenchmarkReconciler) transitionWaitingCluster(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) (ctrl.Result, error) {
	lsName := ledgerServiceName(bm.Name)

	_, err := r.Dynamic.Resource(ledgerServiceGVR).Namespace(bm.Namespace).Get(ctx, lsName, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			log.Info("LedgerService not found yet, requeuing", "name", lsName)
			return ctrl.Result{RequeueAfter: requeueDelay}, nil
		}
		return ctrl.Result{}, err
	}

	bm.Status.Phase = benchmarkv1alpha1.BenchmarkPhaseWaitingCluster
	if err := r.Status().Update(ctx, bm); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueDelay}, nil
}

func (r *BenchmarkReconciler) checkClusterReady(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) (ctrl.Result, error) {
	lsName := ledgerServiceName(bm.Name)

	ls, err := r.Dynamic.Resource(ledgerServiceGVR).Namespace(bm.Namespace).Get(ctx, lsName, metav1.GetOptions{})
	if err != nil {
		return ctrl.Result{}, err
	}

	lsPhase := getString(ls.Object, "status", "phase")
	if lsPhase != "Running" {
		log.Info("LedgerService not ready", "name", lsName, "phase", lsPhase)
		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}

	log.Info("LedgerService ready")

	// If a ledger name is configured, transition to CreatingLedger first.
	if bm.Spec.LedgerName != "" {
		bm.Status.Phase = benchmarkv1alpha1.BenchmarkPhaseCreatingLedger
		if err := r.Status().Update(ctx, bm); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{Requeue: true}, nil
	}

	return r.startTestRun(ctx, log, bm)
}

func (r *BenchmarkReconciler) createLedger(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) (ctrl.Result, error) {
	lsName := ledgerServiceName(bm.Name)
	grpcEndpoint := ledgerServiceGRPCEndpoint(lsName, bm.Namespace)

	log.Info("creating ledger", "name", bm.Spec.LedgerName, "endpoint", grpcEndpoint)

	if err := r.Ledger.CreateLedger(ctx, grpcEndpoint, bm.Spec.LedgerName); err != nil {
		return r.setBenchmarkFailed(ctx, bm, fmt.Sprintf("failed to create ledger: %v", err))
	}

	return r.startTestRun(ctx, log, bm)
}

func (r *BenchmarkReconciler) startTestRun(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) (ctrl.Result, error) {
	lsName := ledgerServiceName(bm.Name)
	grpcEndpoint := ledgerServiceGRPCEndpoint(lsName, bm.Namespace)
	trName := testRunName(bm.Name)

	log.Info("creating TestRun", "name", trName)

	tr := buildTestRun(bm, grpcEndpoint)

	_, err := r.Dynamic.Resource(testRunGVR).Namespace(bm.Namespace).Create(ctx, tr, metav1.CreateOptions{})
	if err != nil && !kerrors.IsAlreadyExists(err) {
		return r.setBenchmarkFailed(ctx, bm, fmt.Sprintf("failed to create TestRun: %v", err))
	}

	now := metav1.Now()
	bm.Status.Phase = benchmarkv1alpha1.BenchmarkPhaseRunning
	bm.Status.TestRunName = trName
	bm.Status.StartTime = &now
	if err := r.Status().Update(ctx, bm); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueDelay}, nil
}

func (r *BenchmarkReconciler) checkTestRunDone(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) (ctrl.Result, error) {
	trName := testRunName(bm.Name)

	tr, err := r.Dynamic.Resource(testRunGVR).Namespace(bm.Namespace).Get(ctx, trName, metav1.GetOptions{})
	if err != nil {
		return ctrl.Result{}, err
	}

	stage := getString(tr.Object, "status", "stage")
	if stage != stageFinished && stage != stageError {
		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}

	log.Info("TestRun finished", "stage", stage)

	bm.Status.Phase = benchmarkv1alpha1.BenchmarkPhaseSnapshotting
	if err := r.Status().Update(ctx, bm); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{Requeue: true}, nil
}

func (r *BenchmarkReconciler) snapshot(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) (ctrl.Result, error) {
	trName := testRunName(bm.Name)

	tr, err := r.Dynamic.Resource(testRunGVR).Namespace(bm.Namespace).Get(ctx, trName, metav1.GetOptions{})
	if err != nil {
		return ctrl.Result{}, err
	}

	log.Info("creating Grafana snapshots")

	result, err := r.Grafana.ProcessTestRun(ctx, tr)
	if err != nil {
		return r.setBenchmarkFailed(ctx, bm, fmt.Sprintf("snapshot failed: %v", err))
	}

	// Store report in ConfigMap.
	if result.Report != "" {
		reportName := reportConfigMapName(trName)
		if cmErr := r.ensureReportConfigMap(ctx, bm.Namespace, reportName, result.Report); cmErr != nil {
			log.Error(cmErr, "failed to write report configmap")
		}
	}

	// Mark TestRun as processed so standalone reconciler doesn't re-process it.
	if markErr := r.markTestRunProcessed(ctx, tr); markErr != nil {
		log.Error(markErr, "failed to mark testrun processed")
	}

	// Delete ledger if one was created.
	lsName := ledgerServiceName(bm.Name)
	if bm.Spec.LedgerName != "" {
		grpcEndpoint := ledgerServiceGRPCEndpoint(lsName, bm.Namespace)
		log.Info("deleting ledger", "name", bm.Spec.LedgerName)
		if delErr := r.Ledger.DeleteLedger(ctx, grpcEndpoint, bm.Spec.LedgerName); delErr != nil {
			log.Error(delErr, "failed to delete ledger")
		}
	}

	// Delete LedgerService now that we have snapshots.
	log.Info("deleting LedgerService", "name", lsName)
	if delErr := r.Dynamic.Resource(ledgerServiceGVR).Namespace(bm.Namespace).Delete(ctx, lsName, metav1.DeleteOptions{}); delErr != nil && !kerrors.IsNotFound(delErr) {
		log.Error(delErr, "failed to delete LedgerService")
	}

	now := metav1.Now()
	bm.Status.Phase = benchmarkv1alpha1.BenchmarkPhaseCompleted
	bm.Status.CompletionTime = &now
	if result.Report != "" {
		bm.Status.Report = result.Report
	}

	log.Info("benchmark completed")

	if err := r.Status().Update(ctx, bm); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *BenchmarkReconciler) handleDelete(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(bm, benchmarkFinalizer) {
		return ctrl.Result{}, nil
	}

	log.Info("cleaning up resources")

	// Delete TestRun if it exists.
	trName := testRunName(bm.Name)
	if err := r.Dynamic.Resource(testRunGVR).Namespace(bm.Namespace).Delete(ctx, trName, metav1.DeleteOptions{}); err != nil && !kerrors.IsNotFound(err) {
		log.Error(err, "failed to delete TestRun")
	}

	// Best-effort delete ledger with a short timeout — the LedgerService may already be down.
	lsName := ledgerServiceName(bm.Name)
	if bm.Spec.LedgerName != "" {
		grpcEndpoint := ledgerServiceGRPCEndpoint(lsName, bm.Namespace)
		log.Info("deleting ledger (best-effort)", "name", bm.Spec.LedgerName)
		deleteCtx, deleteCancel := context.WithTimeout(ctx, 10*time.Second)
		if err := r.Ledger.DeleteLedger(deleteCtx, grpcEndpoint, bm.Spec.LedgerName); err != nil {
			log.Error(err, "failed to delete ledger (best-effort, continuing cleanup)")
		}
		deleteCancel()
	}

	// Delete LedgerService if it exists.
	if err := r.Dynamic.Resource(ledgerServiceGVR).Namespace(bm.Namespace).Delete(ctx, lsName, metav1.DeleteOptions{}); err != nil && !kerrors.IsNotFound(err) {
		log.Error(err, "failed to delete LedgerService")
	}

	// Cleanup Grafana snapshots from report ConfigMap.
	reportName := reportConfigMapName(trName)
	if err := r.cleanupReport(ctx, log, bm.Namespace, reportName); err != nil {
		log.Error(err, "cleanup report failed")
	}

	// Remove finalizer.
	controllerutil.RemoveFinalizer(bm, benchmarkFinalizer)
	if err := r.Update(ctx, bm); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("cleanup completed")

	return ctrl.Result{}, nil
}

func (r *BenchmarkReconciler) setBenchmarkFailed(ctx context.Context, bm *benchmarkv1alpha1.Benchmark, message string) (ctrl.Result, error) {
	now := metav1.Now()
	bm.Status.Phase = benchmarkv1alpha1.BenchmarkPhaseFailed
	bm.Status.Message = message
	bm.Status.CompletionTime = &now

	if err := r.Status().Update(ctx, bm); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *BenchmarkReconciler) markTestRunProcessed(ctx context.Context, obj *unstructured.Unstructured) error {
	patch := fmt.Appendf(nil, `{"metadata":{"annotations":{"%s":"%s"}}}`, processedAnnotation, time.Now().UTC().Format(time.RFC3339))
	_, err := r.Dynamic.Resource(testRunGVR).Namespace(obj.GetNamespace()).Patch(ctx, obj.GetName(), types.MergePatchType, patch, metav1.PatchOptions{})

	return err
}

func (r *BenchmarkReconciler) ensureReportConfigMap(ctx context.Context, namespace, name, report string) error {
	cm := &corev1.ConfigMap{}
	cm.Namespace = namespace
	cm.Name = name

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Data = map[string]string{
			"report.json": report,
		}
		return nil
	})

	return err
}

func (r *BenchmarkReconciler) cleanupReport(ctx context.Context, log logr.Logger, namespace, name string) error {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cm); err != nil {
		if kerrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	reportData := cm.Data["report.json"]
	if reportData != "" {
		if err := r.deleteSnapshots(ctx, reportData); err != nil {
			log.Error(err, "snapshot cleanup failed")
		}
	}

	return r.Delete(ctx, cm)
}

func (r *BenchmarkReconciler) deleteSnapshots(ctx context.Context, report string) error {
	var parsed struct {
		Entries []struct {
			DeleteURL   string `json:"deleteUrl"`
			DeleteKey   string `json:"deleteKey"`
			SnapshotKey string `json:"snapshotKey"`
		} `json:"entries"`
	}

	if err := json.Unmarshal([]byte(report), &parsed); err != nil {
		return err
	}

	for _, entry := range parsed.Entries {
		_ = r.Grafana.DeleteSnapshot(ctx, entry.DeleteURL, entry.DeleteKey, entry.SnapshotKey) //nolint:errcheck // best-effort
	}

	return nil
}

func (r *BenchmarkReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&benchmarkv1alpha1.Benchmark{}).
		Complete(r)
}
