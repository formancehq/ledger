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
// +kubebuilder:rbac:groups=k6.io,resources=testruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

// BenchmarkReconciler reconciles a Benchmark object.
type BenchmarkReconciler struct {
	client.Client

	Scheme  *runtime.Scheme
	Dynamic dynamic.Interface
	Grafana *GrafanaClient
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
		return r.transitionCreatingResources(ctx, log, &bm)
	case benchmarkv1alpha1.BenchmarkPhaseCreatingResources:
		return r.createResources(ctx, log, &bm)
	case benchmarkv1alpha1.BenchmarkPhaseWaitingForResources:
		return r.checkResourcesReady(ctx, log, &bm)
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

func (r *BenchmarkReconciler) transitionCreatingResources(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) (ctrl.Result, error) {
	// If no resources to create, go straight to starting the test run.
	if len(bm.Spec.Resources) == 0 {
		return r.startTestRun(ctx, log, bm)
	}

	log.Info("transitioning to CreatingResources")

	bm.Status.Phase = benchmarkv1alpha1.BenchmarkPhaseCreatingResources
	bm.Status.ResourceNames = nil
	if err := r.Status().Update(ctx, bm); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{Requeue: true}, nil
}

func (r *BenchmarkReconciler) createResources(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) (ctrl.Result, error) {
	created := len(bm.Status.ResourceNames)
	total := len(bm.Spec.Resources)

	// If all resources have been created, transition to waiting.
	if created >= total {
		bm.Status.Phase = benchmarkv1alpha1.BenchmarkPhaseWaitingForResources
		if err := r.Status().Update(ctx, bm); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{Requeue: true}, nil
	}

	// If the last created resource is not yet ready, wait.
	if created > 0 {
		lastEntry := bm.Spec.Resources[created-1]
		lastName := bm.Status.ResourceNames[created-1]

		gvr, err := gvrForManifest(lastEntry.Manifest.Raw)
		if err != nil {
			return r.setBenchmarkFailed(ctx, bm, fmt.Sprintf("invalid resource manifest at index %d: %v", created-1, err))
		}

		obj, err := r.Dynamic.Resource(gvr).Namespace(bm.Namespace).Get(ctx, lastName, metav1.GetOptions{})
		if err != nil {
			return ctrl.Result{}, err
		}

		if !checkReadyCondition(obj, lastEntry.ReadyCondition) {
			log.Info("waiting for resource to be ready", "name", lastName, "index", created-1)

			return ctrl.Result{RequeueAfter: requeueDelay}, nil
		}
	}

	// Create the next resource.
	entry := bm.Spec.Resources[created]
	name := resourceName(bm.Name, created)

	gvr, err := gvrForManifest(entry.Manifest.Raw)
	if err != nil {
		return r.setBenchmarkFailed(ctx, bm, fmt.Sprintf("invalid resource manifest at index %d: %v", created, err))
	}

	log.Info("creating resource", "name", name, "index", created, "gvr", gvr.String())

	res := buildResource(bm, entry, name)
	_, err = r.Dynamic.Resource(gvr).Namespace(bm.Namespace).Create(ctx, res, metav1.CreateOptions{})
	if err != nil && !kerrors.IsAlreadyExists(err) {
		return r.setBenchmarkFailed(ctx, bm, fmt.Sprintf("failed to create resource %d: %v", created, err))
	}

	bm.Status.ResourceNames = append(bm.Status.ResourceNames, name)
	if err := r.Status().Update(ctx, bm); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueDelay}, nil
}

func (r *BenchmarkReconciler) checkResourcesReady(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) (ctrl.Result, error) {
	// Check all resources are ready.
	for i, entry := range bm.Spec.Resources {
		if i >= len(bm.Status.ResourceNames) {
			break
		}
		name := bm.Status.ResourceNames[i]

		gvr, err := gvrForManifest(entry.Manifest.Raw)
		if err != nil {
			return r.setBenchmarkFailed(ctx, bm, fmt.Sprintf("invalid resource manifest at index %d: %v", i, err))
		}

		obj, err := r.Dynamic.Resource(gvr).Namespace(bm.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return ctrl.Result{}, err
		}

		if !checkReadyCondition(obj, entry.ReadyCondition) {
			log.Info("resource not ready", "name", name, "index", i)

			return ctrl.Result{RequeueAfter: requeueDelay}, nil
		}
	}

	log.Info("all resources ready")

	return r.startTestRun(ctx, log, bm)
}

func (r *BenchmarkReconciler) startTestRun(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) (ctrl.Result, error) {
	trName := testRunName(bm.Name)

	log.Info("creating TestRun", "name", trName)

	tr := buildTestRun(bm)

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

	// Delete resources in reverse order.
	r.deleteResourcesReverse(ctx, log, bm)

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

func (r *BenchmarkReconciler) deleteResourcesReverse(ctx context.Context, log logr.Logger, bm *benchmarkv1alpha1.Benchmark) {
	for i := len(bm.Spec.Resources) - 1; i >= 0; i-- {
		if i >= len(bm.Status.ResourceNames) {
			continue
		}
		name := bm.Status.ResourceNames[i]
		entry := bm.Spec.Resources[i]

		gvr, err := gvrForManifest(entry.Manifest.Raw)
		if err != nil {
			log.Error(err, "invalid resource manifest during cleanup", "index", i)

			continue
		}

		log.Info("deleting resource", "name", name, "index", i)
		if delErr := r.Dynamic.Resource(gvr).Namespace(bm.Namespace).Delete(ctx, name, metav1.DeleteOptions{}); delErr != nil && !kerrors.IsNotFound(delErr) {
			log.Error(delErr, "failed to delete resource", "name", name)
		}
	}
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

	// Delete resources in reverse order.
	r.deleteResourcesReverse(ctx, log, bm)

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
		_ = r.Grafana.DeleteSnapshot(ctx, entry.DeleteURL, entry.DeleteKey, entry.SnapshotKey)
	}

	return nil
}

func (r *BenchmarkReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&benchmarkv1alpha1.Benchmark{}).
		Complete(r)
}
